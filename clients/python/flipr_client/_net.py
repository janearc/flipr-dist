#!/usr/bin/env python3
# net -- the only sanctioned way to make a call that can fail transiently.
#
# WHY THIS EXISTS. Every caller should use backoff and jitter, because bad
# things happen when they don't, and a rule written in a document only holds
# some of the time, usually not at the moment it matters. So it lives in a
# library everyone owns, and there is no question of whether or how to do it.
#
# dodo got src/net.ts the same day. This is its sibling, and it exists because
# the rule has to hold in any language that joins the mesh -- a policy that is
# only enforced in TypeScript is a policy the Python service is exempt from,
# and the Python service is the one doing the heavy fetching.
#
# KINGFISHER IS THE SERVICE THAT MOST NEEDS IT. dodo's calls are almost all
# in-cluster. Ours go over the public internet to USGS, GIBS, OpenSky and the
# weather services -- people who can and will block us, so those calls are
# wrapped too: if we make them angry, we get banned. A retry storm against a government endpoint is not a latency
# problem, it is an access problem, and access is not something we can restore
# by fixing the code afterwards.
#
# FULL JITTER, deliberately. sleep = random(0, min(cap, base * 2^attempt)).
# Not fixed backoff, which synchronises every caller onto the same schedule and
# rebuilds the herd one beat later. Not equal jitter, which still leaves a floor
# everyone shares. Full jitter spreads retries across the whole window and is
# the only one of the three that decorrelates independent callers -- which
# matters here because gibsd, openskyd and weatherd all wake on their own
# timers and would otherwise line up the moment one provider has a bad minute.
#
# WHAT THIS IS NOT. It is transport only. Decompression stays in providers.py
# (gzip and brotli, both learned the hard way from CDNs that compress whatever
# you asked for), and the flag gates stay with their callers. This layer knows
# about attempts, deadlines and status codes, and nothing else.

import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

# Structured JSON to stdout, one line per event, matching the estate's shape --
# but ONLY when the host application has not provided its own logger. A client
# library that writes to somebody else's stdout uninvited is a client library
# that gets vendored once and then quietly edited, so set_logger() is the seam:
# kingfisher passes its log(), everybody else gets this.
#
# The default is not silence. A retry that nobody can see is how a dependency
# degrades for a week without anyone noticing -- the failure mode this whole
# module exists to make visible.
import json as _json
import sys as _sys


def _default_log(level, event, **fields):
    rec = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
           "level": level, "event": event, "svc": "flipr-client"}
    rec.update(fields)
    _sys.stdout.write(_json.dumps(rec, default=str) + "\n")
    _sys.stdout.flush()


_log = _default_log


# Hand the library the host application's logger. Call it once at startup;
# kingfisher passes its own log() so retries land in the same stream as
# everything else it emits.
def set_logger(fn):
    global _log
    _log = fn if fn is not None else _default_log


def log(level, event, **fields):
    _log(level, event, **fields)

# the one outbound door, injectable so the suite can stub the network without
# monkeypatching urllib for every OTHER caller in the process -- urllib.request
# is a shared module object, and patching it globally hijacks the test
# harness's own HTTP client. Same seam providers.py and routing.py already use,
# and the reason it exists there is the reason it exists here.
_urlopen = urllib.request.urlopen


# The knobs, with the defaults every caller gets unless it argues otherwise.
class RetryPolicy:
    # attempts INCLUDES the first, so 1 means "try once, never retry"
    def __init__(self, attempts=4, base_s=0.5, max_s=30.0,
                 timeout_s=30.0, total_budget_s=120.0):
        self.attempts = attempts
        self.base_s = base_s
        self.max_s = max_s
        self.timeout_s = timeout_s
        self.total_budget_s = total_budget_s

    # so a caller can take the default and change one thing, without
    # reconstructing the others from memory and getting one subtly wrong
    def replace(self, **kw):
        p = RetryPolicy(self.attempts, self.base_s, self.max_s,
                        self.timeout_s, self.total_budget_s)
        for k, v in kw.items():
            setattr(p, k, v)
        return p


# Retry-After is honoured up to this: a locked flipr asks for thirty seconds
# at most (clients/CONTRACT.md section 9), and a server asking for more is
# asking us to stall.
RETRY_AFTER_CEILING_S = 30.0

# Four attempts over 30s deadlines with a two-minute ceiling.
#
# Deliberately slower and more patient than dodo's default, because the work is
# different: dodo is rendering a page somebody is waiting on, kingfisher is a
# daemon fetching a 400MB lidar tile in the background. Nobody is watching a
# spool pass, so the right trade is to ride out a provider's bad minute rather
# than to fail fast and leave the item for the next scan.
DEFAULT = RetryPolicy()

# For anything a person is waiting on -- the RPC surface, a flag check. Same
# argument dodo's flipr client makes: a hot path must refuse quickly rather
# than queue behind a dependency already known to be down.
INTERACTIVE = RetryPolicy(attempts=2, base_s=0.05, max_s=0.25,
                          timeout_s=2.0, total_budget_s=4.5)

# The flipr client's policy, from clients/CONTRACT.md section 6: three
# attempts, 50 ms base, one second cap, full jitter; a budget that lets a
# lock's Retry-After through once.
CONTRACT = RetryPolicy(attempts=3, base_s=0.05, max_s=1.0,
                       timeout_s=2.0, total_budget_s=35.0)


# Every attempt failed. Carries the last cause so a caller can still branch on
# WHY, and the attempt count so the log says how hard we tried.
class RetriesExhausted(Exception):
    def __init__(self, label, attempts, cause):
        super().__init__(f"{label}: gave up after {attempts} attempt(s): {cause}")
        self.label = label
        self.attempts = attempts
        self.cause = cause


# A response whose status we refuse to accept, so status failures travel the
# same path as network failures instead of being a second shape every caller
# has to remember to check.
class HttpStatusError(Exception):
    def __init__(self, status, url, retry_after_s=None, body=b""):
        # the body is what the server was TRYING to tell us, and once the
        # connection is closed it is gone. Truncated so an error path cannot
        # buffer a large response.
        snippet = body[:BODY_SNIPPET].decode("utf-8", "replace") if body else ""
        super().__init__(f"{url} answered {status}" + (f": {snippet}" if snippet else ""))
        self.status = status
        self.url = url
        self.retry_after_s = retry_after_s
        self.body = snippet


# enough for a sentence of explanation, not enough to be a memory decision
BODY_SNIPPET = 200

# Methods that promise repeating the request is the same as making it once.
# PATCH is absent deliberately: it is idempotent only if the patch document
# says so, which this layer cannot know.
IDEMPOTENT = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE"})


# whether this request may be attempted more than once
def may_retry(method, opt_in=False):
    if opt_in:
        return True
    return (method or "GET").upper() in IDEMPOTENT


# the backoff sleep before a given attempt, with full jitter. `attempt` is
# 0-based and names the attempt about to be made, so the sleep before the first
# retry (attempt 1) has ceiling base_s*2.
def backoff_s(attempt, policy=DEFAULT, rand=random.random):
    ceiling = min(policy.max_s, policy.base_s * (2 ** attempt))
    return rand() * ceiling


# which failures deserve another try.
#
# 5xx, 429 and 408 are the server saying "not now"; anything else in 4xx is the
# server saying "not ever, and not differently next time". Retrying a 404 is
# how a poison queue starts -- see errors.py and the 119 files that were
# re-refused 4,463 times each. Network-level failures are retryable because
# they are indistinguishable from a service that is mid-restart.
def http_retryable(err):
    if isinstance(err, HttpStatusError):
        return err.status >= 500 or err.status in (429, 408)
    return True


# Retry-After, which servers send as either seconds or an HTTP date. Honouring
# it is the difference between backing off and arguing -- and with the
# government endpoints, arguing is how access gets withdrawn.
def retry_after_s(header, now=None):
    if not header:
        return None
    header = header.strip()
    try:
        return max(0.0, float(header))
    except ValueError:
        pass
    try:
        from email.utils import parsedate_to_datetime
        when = parsedate_to_datetime(header).timestamp()
    except Exception:
        return None
    return max(0.0, when - (now if now is not None else time.time()))


# a host label for metrics. Never the full URL: paths carry dataset ids and
# would explode the label cardinality prometheus has to hold in memory.
def host_of(url):
    try:
        return urllib.parse.urlparse(url).netloc or "unparseable"
    except Exception:
        return "unparseable"


# Counters, in kingfisher's own shape: module-level dicts under a lock, read by
# serve.py's /metrics handler. Deliberately NOT a new metrics framework -- the
# service already has a pattern and a second one would mean two places to look.
_LOCK = threading.Lock()
# host -> outcome -> count
CALLS = {}
# host -> reason -> count
RETRIES = {}
# host -> {"sum": seconds, "count": n} -- wall time INCLUDING backoff sleeps,
# because what the caller waited is the number worth charting
DURATION = {}


def _count(host, outcome):
    with _LOCK:
        CALLS.setdefault(host, {})
        CALLS[host][outcome] = CALLS[host].get(outcome, 0) + 1


def _count_retry(host, reason):
    with _LOCK:
        RETRIES.setdefault(host, {})
        RETRIES[host][reason] = RETRIES[host].get(reason, 0) + 1


def _observe(host, seconds):
    with _LOCK:
        d = DURATION.setdefault(host, {"sum": 0.0, "count": 0})
        d["sum"] += seconds
        d["count"] += 1


# bucket a failure for the metric label. Kept coarse on purpose: three values a
# dashboard can chart, not the open set of error strings.
def _reason(err):
    if isinstance(err, HttpStatusError):
        return "status"
    if isinstance(err, TimeoutError) or "timed out" in str(err).lower():
        return "timeout"
    return "network"


# Run `fn` until it succeeds, the failure is judged permanent, attempts run out,
# or the total budget is spent -- backing off with full jitter between tries.
# This is the primitive; the HTTP wrapper below is a thin shell on it.
def retrying(label, fn, is_retryable=http_retryable, policy=DEFAULT,
             rand=None, sleep=None, now=None):
    # RESOLVED HERE, not in the signature. `sleep=time.sleep` as a default
    # argument binds the function object once, at def time, so a test that
    # patches net.time.sleep afterwards changes nothing and the suite sleeps
    # for real -- measured 2026-09-01 as 90 seconds of genuine waiting in a
    # test that looked instant. Resolving at call time makes the injection
    # points work the way they appear to.
    rand = rand if rand is not None else random.random
    sleep = sleep if sleep is not None else time.sleep
    now = now if now is not None else time.monotonic
    started = now()
    last = None
    for attempt in range(policy.attempts):
        try:
            return fn(attempt)
        except Exception as err:
            last = err
            permanent = not is_retryable(err)
            exhausted = attempt == policy.attempts - 1
            if permanent or exhausted:
                # say WHICH of the two it was. "gave up" and "refused to try"
                # are different operational stories, and a log that conflates
                # them sends the on-call reader looking for capacity problems
                # that do not exist.
                log("warn",
                    "call failed permanently" if permanent else "call failed, retries exhausted",
                    label=label, attempt=attempt + 1, of=policy.attempts, err=str(err))
                break
            wait = backoff_s(attempt, policy, rand)
            # a server that says how long to wait outranks our own jitter --
            # ignoring Retry-After is how a 429 becomes a ban
            if isinstance(err, HttpStatusError) and err.retry_after_s is not None:
                wait = min(max(wait, err.retry_after_s), RETRY_AFTER_CEILING_S)
            # check the budget against the sleep we are ABOUT to take, not
            # after taking it -- otherwise the last sleep always overruns by
            # design
            if now() - started + wait > policy.total_budget_s:
                log("warn", "call failed, total budget spent",
                    label=label, attempt=attempt + 1,
                    budget_s=policy.total_budget_s, err=str(err))
                break
            _count_retry(label, _reason(err))
            log("debug", "retrying after backoff",
                label=label, attempt=attempt + 1, wait_s=round(wait, 3), err=str(err))
            sleep(wait)
    raise RetriesExhausted(label, policy.attempts, last)


# One HTTP call, with backoff, jitter, a per-attempt deadline and a total
# budget. THIS IS THE ONLY WAY TO REACH THE NETWORK IN THIS CODEBASE.
#
# Returns (status, headers, body_bytes). Raises for any status not accepted, so
# callers get one failure shape rather than two -- which is why nobody here
# forgets to check the status.
#
# accept_status widens what counts as success, for the callers that read the
# status themselves rather than treating it as an exception. It is a predicate
# rather than a flag so a caller says WHICH status it means; widening it to
# "anything is fine" would delete the retry behaviour along with the error
# handling.
def request(url, data=None, headers=None, method=None, policy=DEFAULT,
            retry_non_idempotent=False, accept_status=None,
            rand=None, sleep=None, now=None, is_retryable=None):
    # is_retryable: a caller's own judgement of which failures deserve another
    # try (flipr's client adds 423 Locked and the edge's 404); the default is
    # http_retryable
    is_retryable = is_retryable if is_retryable is not None else http_retryable
    now = now if now is not None else time.monotonic
    host = host_of(url)
    began = now()

    # A non-idempotent request gets exactly one attempt, whatever policy it was
    # handed. Collapsing it HERE rather than at the call site means a caller
    # cannot get this wrong by copying a policy from somewhere else.
    #
    # This is not theoretical for kingfisher. RefreshIndex and Fetch are how
    # this service is told to go and pull from somebody else's servers; a
    # retried Fetch is a second download of the same dataset from a host that
    # is already unhappy. A timeout is the worst case, because the request very
    # often arrived and only the answer was lost.
    if not may_retry(method or ("POST" if data is not None else "GET"), retry_non_idempotent):
        policy = policy.replace(attempts=1)

    def attempt(_n):
        req = urllib.request.Request(url, data=data, headers=headers or {},
                                     method=method)
        try:
            # a per-attempt deadline, not a shared one: a hung connection must
            # not consume the budget the retry after it needs
            with _urlopen(req, timeout=policy.timeout_s) as r:
                # r.headers, NOT dict(r.headers). urllib hands back an
                # email.message.Message whose lookups are CASE-INSENSITIVE, and
                # flattening it to a dict silently makes them case-sensitive.
                # Servers disagree about the spelling -- providers.py reads
                # Content-Encoding, and worldview's CDN is exactly the sort of
                # thing that sends content-encoding -- so the flattened version
                # misses the header, skips decompression, and hands the caller
                # gzip bytes to json.loads. The Message survives the with-block
                # because it is parsed up front, not read from the socket.
                return (r.status, r.headers, r.read())
        except urllib.error.HTTPError as e:
            # urllib raises on non-2xx and the error IS the response, so the
            # body is readable exactly once and only here
            body = b""
            try:
                body = e.read()
            except Exception:
                pass
            if accept_status is not None and accept_status(e.code):
                return (e.code, e.headers, body)
            raise HttpStatusError(e.code, url,
                                  retry_after_s(e.headers.get("Retry-After")
                                                if e.headers else None),
                                  body) from None

    try:
        out = retrying(host, attempt, is_retryable, policy, rand, sleep, now)
        _count(host, "ok")
        return out
    except RetriesExhausted as err:
        permanent = not is_retryable(err.cause)
        _count(host, "refused" if permanent else "failed")
        raise
    finally:
        _observe(host, now() - began)


# fetch and parse JSON in one call, because most callers do exactly this and
# hand-rolling it is where the missing status checks live
def get_json(url, **kw):
    import json as _json
    _status, _headers, body = request(url, **kw)
    return _json.loads(body)


# The download policy, and it is the most conservative one here on purpose.
#
# A retried download is not a retried read: a 400MB lidar tile restarted from
# zero costs the provider the whole transfer again. Government sites are
# flaky, so a retry matters, but there is a limit.
#
# So: two attempts, not four, and a long deadline rather than many tries. One
# restart is worth it for a connection that dropped at 90%; a second is us
# being the problem.
#
# THE HONEST GAP: this restarts rather than resumes. Most of these servers
# advertise Accept-Ranges, so a resumed download would cost the remaining bytes
# instead of all of them, and that is the right fix -- it is not done here
# because it needs the partial file's length threaded through and a conditional
# If-Range, and doing it badly re-downloads anyway while looking like it did
# not. Until then, two attempts is the ceiling that keeps the waste bounded.
DOWNLOAD = RetryPolicy(attempts=2, base_s=2.0, max_s=30.0,
                       timeout_s=300.0, total_budget_s=700.0)


# Stream a URL to a file, with the same backoff as everything else.
#
# Separate from request() because request() reads the whole body into memory,
# which is correct for an index page and catastrophic for a point cloud. Returns
# the byte count.
#
# on_progress is called with the running total as it goes, so a caller can
# publish it; it must be cheap, because it is called per chunk.
def download(url, dest, headers=None, policy=DOWNLOAD, chunk=1 << 20,
             on_progress=None, rand=None, sleep=None, now=None):
    now = now if now is not None else time.monotonic
    host = host_of(url)
    began = now()

    def attempt(_n):
        req = urllib.request.Request(url, headers=headers or {})
        n = 0
        try:
            # The partial file is truncated on every attempt. That is the point
            # of opening it inside the attempt rather than outside: a retry that
            # appends to what the failed attempt left behind produces a
            # corrupt file that looks like a successful download, which is a
            # worse outcome than any number of retries.
            with _urlopen(req, timeout=policy.timeout_s) as r, open(dest, "wb") as f:
                while True:
                    buf = r.read(chunk)
                    if not buf:
                        break
                    f.write(buf)
                    n += len(buf)
                    if on_progress is not None:
                        on_progress(n)
            return n
        except urllib.error.HTTPError as e:
            body = b""
            try:
                body = e.read()
            except Exception:
                pass
            raise HttpStatusError(e.code, url,
                                  retry_after_s(e.headers.get("Retry-After")
                                                if e.headers else None),
                                  body) from None

    try:
        n = retrying(host, attempt, http_retryable, policy, rand, sleep, now)
        _count(host, "ok")
        return n
    except RetriesExhausted as err:
        _count(host, "refused" if not http_retryable(err.cause) else "failed")
        raise
    finally:
        _observe(host, now() - began)

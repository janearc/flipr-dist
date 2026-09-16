# flipr_client -- the python client for flipr, one of three published clients
# tested against clients/CONTRACT.md. Where this file and the contract
# disagree, this file is wrong.
#
# stdlib only, and packaged (flipr-client) so a service depends on it at a tag
# rather than copying it: the copies are what the libfliprclient sprint of
# 2026-09-06 squashes, and a repository that carries its own flipr client is
# defective by the dev rules of that date.
#
# THE SEMANTICS ARE RULED AND THIS FILE IMPLEMENTS THEM EXACTLY.
# clients cache the last read, and always check that flipr is there.
#
#   - the cache is a PERFORMANCE optimisation, never a fallback
#   - every check() pings before trusting the cache
#   - what happens when flipr is away is the caller's POLICY, declared at
#     construction and never defaulted: REFUSE raises (a stale yes costs
#     money), HOLD returns the last known value and says it is holding
#
# usage, for the common gate-an-expensive-thing shape:
#
#     flags = FliprClient("http://flipr.test", "kingfisher", "v1",
#                         declared=DECLARED, policy=Policy.REFUSE)
#     if flags.check("fetch.enabled"):
#         do_the_expensive_fetch()
#
# under REFUSE check() raises FliprDown when flipr cannot be reached and
# FlagMissing when the flag is not declared; do not catch FliprDown and carry
# on -- failing is the designed behaviour, and the network being down when
# flipr is down is the point of flipr.

import hashlib
import json
import re
import time

from . import _net


# what a caller does when nothing authoritative answered (CONTRACT.md section 4)
class Policy:
    # unknown never authorises: check() raises. for gates over spend.
    REFUSE = "refuse"
    # unknown keeps the last known value, or the declared default if nothing
    # has been read yet, and the client says it is holding (last_known False,
    # last_why). for instruments, where going quiet IS the failure.
    HOLD = "hold"


class FliprDown(Exception):
    # flipr is unreachable. the caller STOPS. this is not a transient to
    # retry around: if flipr is down the network is down, by design.
    pass


class FliprLocked(FliprDown):
    # an operator locked flipr (423, fliprctl lock). a FliprDown so existing
    # handlers stop, with the reason and the fact that it is a lock.
    def __init__(self, reason):
        super().__init__(f"flipr locked: {reason}")
        self.reason = reason


class FliprRefused(FliprDown):
    # flipr answered a 4xx to a write: off-contract body, missing reason,
    # over budget. a FliprDown so existing handlers stop, with the status
    # and flipr's text so a log line says the true thing.
    def __init__(self, method, status, body):
        super().__init__(f"flipr refused {method}: {status} {body}")
        self.status = status
        self.body = body


class FlagMissing(Exception):
    # the flag was never declared. distinct from False on purpose: an absent
    # flag is a wiring mistake, and reading it as "off" would hide that
    # mistake until someone wondered why a feature never ran.
    pass


class ConfigError(ValueError):
    # a misconfiguration is refused at construction; a flipr that is down is
    # never one (CONTRACT.md section 10)
    pass


# this client's identity on the wire, X-Flipr-Client: py/<tag>/<hash>
# (CONTRACT.md sections 1 and 8). set at package build from the release tag
# and a sha256 over this file; a build that did not set them announces itself
# as dev/unknown, which flipr counts as an unknown client.
from ._stamp import CLIENT_TAG, CLIENT_HASH


def client_header():
    return f"py/{CLIENT_TAG}/{CLIENT_HASH}"


# a version that is a commit hash is not a flag contract version
_COMMIT = re.compile(r"^[0-9a-f]{7,40}$")
_IPV4 = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")


# an IP host other than loopback: flipr is reached by name (a port on a name
# is fine; the throwaway edge is flipr.test:7800)
def _is_address(base):
    host = re.sub(r"^[a-z]+://", "", base, flags=re.I)
    host = re.split(r"[/?#]", host)[0]
    if "]" not in host:
        host = re.sub(r":\d+$", "", host)
    if host in ("127.0.0.1", "localhost", "::1", "[::1]"):
        return False
    return bool(_IPV4.match(host))


# which failures deserve another try, flipr's version of _net.http_retryable
# (CONTRACT.md section 6): 5xx, 429, 423, transport errors; and a 404 whose
# body is not flipr's own JSON, which is the edge answering for a flipr that
# is not there. any other 4xx is the contract speaking.
def _retryable(err):
    if isinstance(err, _net.HttpStatusError):
        if err.status == 404:
            return not err.body.lstrip().startswith("{")
        # a 423 is not retried inside the call: the lock is the answer, and
        # the client remembers it (CONTRACT.md section 9)
        return err.status >= 500 or err.status == 429
    return True


# a lock is remembered for its Retry-After, capped at the ceiling; the
# default when flipr sent none
LOCK_CEILING_S = 30.0
LOCK_DEFAULT_S = 5.0


# the instruments a client emits (CONTRACT.md section 7); the caller's sink
# has these four methods, or is None
class _NoMetrics:
    def request(self, method, outcome): pass
    def retry(self, method): pass
    def duration(self, method, seconds): pass
    def generation_change(self): pass


class FliprClient:
    # one client = one service at one version, matching flipr's namespace
    # scheme. the version is the flag contract's (v1), pinned, never a commit.

    def __init__(self, base, service, version, declared=None, policy=None,
                 cache_ttl=5.0, timeout=2.0, metrics=None, log=None):
        # base like "http://flipr.test" -- a NAME, never an address. which
        # flipr answers that name is the network's decision, and that is the
        # whole environment story.
        if not service:
            raise ConfigError("service is required")
        if not version:
            raise ConfigError("version is required; pin it, do not pass a build commit")
        if _COMMIT.match(version):
            raise ConfigError(f"version {version!r} looks like a commit hash; the version is the flag contract's, pinned")
        if not declared:
            raise ConfigError("declared is empty; a service with no flags has nothing to read")
        if policy not in (Policy.REFUSE, Policy.HOLD):
            raise ConfigError("policy must be Policy.REFUSE (spend gates) or Policy.HOLD (instruments); there is no safe default")
        if not base:
            raise ConfigError("base is required; flipr is reached by name")
        if _is_address(base):
            raise ConfigError(f"base {base!r} is an address; flipr is reached by name")
        self._base = base.rstrip("/")
        self._service = service
        self._version = version
        self._declared = list(declared)
        self._policy = policy
        self._ttl = cache_ttl
        self._timeout = timeout
        self._metrics = metrics if metrics is not None else _NoMetrics()
        # the logger is required by the contract; the package's own structured
        # JSON logger (set_logger to route it) is the one a caller gets by
        # leaving it, which is a logger and not silence
        self._log = log if log is not None else _net.log
        self._cache = {}
        self._fetched = 0.0
        # the store generation last seen: a change means flipr lost its
        # store, and the cure is re-publishing what this service declared
        # (ruled 2026-08-28: "if we blow flipr away, our services that use it
        # ought to be able to write defaults to it")
        self._generation = None
        # the declarations reached flipr at least once: a boot that found
        # flipr down publishes on the first answer instead (CONTRACT.md 10)
        self._published = False
        # what the last read was told, for the caller's own health line
        self.last_known = True
        self.last_why = ""
        self.locked_why = None
        # while it stands, every call answers from the remembered lock with
        # no request, so a hot path cannot storm a locked flipr
        self._lock_until = 0.0

    @property
    def namespace(self):
        return f"{self._service}@{self._version}"

    def _post(self, method, body):
        # one rpc, protojson over http, through _net with the contract's
        # policy. errors collapse to FliprDown except a 404 from flipr itself
        # (FlagMissing), a 423 (FliprLocked) and any other 4xx (refused, which
        # is FliprDown carrying the status: asking again gets the same answer).
        started = time.monotonic()
        if self.locked_why is not None and time.monotonic() < self._lock_until:
            self._metrics.request(method, "locked")
            self._metrics.duration(method, 0.0)
            raise FliprLocked(self.locked_why)
        try:
            _status, _headers, raw = _net.request(
                f"{self._base}/flipr.v1.FliprService/{method}",
                data=json.dumps(body).encode(),
                headers={
                    "Content-Type": "application/json",
                    # self-declared attribution for flipr's record and its
                    # counters; this client reads and publishes its own
                    # declarations and never calls SetFlag
                    "X-Flipr-Caller": self.namespace,
                    # and which client this is, so a copy announces itself
                    # as a copy (CONTRACT.md section 8)
                    "X-Flipr-Client": client_header(),
                },
                method="POST",
                retry_non_idempotent=True,
                policy=_net.CONTRACT.replace(timeout_s=self._timeout),
                is_retryable=_retryable,
            )
            if self.locked_why is not None:
                self._log("info", "flipr unlocked", was=self.locked_why)
                self.locked_why = None
                self._lock_until = 0.0
            self._metrics.request(method, "value")
            return json.loads(raw)
        except _net.RetriesExhausted as e:
            cause = e.cause
            if e.attempts > 1:
                for _ in range(e.attempts - 1):
                    self._metrics.retry(method)
            if not isinstance(cause, _net.HttpStatusError):
                self._metrics.request(method, "unreachable")
                raise FliprDown(f"flipr unreachable at {self._base}: {cause}") from None
            if cause.status == 423:
                reason = _reason_of(cause.body)
                hold = min(cause.retry_after_s or LOCK_DEFAULT_S, LOCK_CEILING_S)
                if self.locked_why is None:
                    self._log("warn", "flipr locked; holding and backing off", reason=reason, hold_s=hold)
                self.locked_why = reason
                self._lock_until = time.monotonic() + hold
                self._metrics.request(method, "locked")
                raise FliprLocked(reason) from None
            if cause.status == 404:
                # two DIFFERENT 404s arrive here and confusing them is a real
                # incident shape: flipr's own 404 is a JSON error and means
                # "that flag/namespace is not declared"; the edge's 404 is
                # plain text and means THERE IS NO FLIPR BEHIND THIS ROUTE.
                # absent-but-routed is DOWN. discriminated on the first
                # character because _net truncates the body.
                if cause.body.lstrip().startswith("{"):
                    self._metrics.request(method, "missing")
                    raise FlagMissing(f"{method}: {cause.body}") from None
                self._metrics.request(method, "unreachable")
                raise FliprDown(
                    f"the route answered 404 with a non-flipr body -- "
                    f"flipr is absent behind {self._base}"
                ) from None
            if cause.status >= 500 or cause.status == 429:
                self._metrics.request(method, "unreachable")
                raise FliprDown(f"flipr answered {cause.status} to {method}") from None
            self._metrics.request(method, "refused")
            self._log("error", "flipr refused", method=method, status=cause.status, body=cause.body)
            raise FliprRefused(method, cause.status, cause.body) from None
        finally:
            self._metrics.duration(method, time.monotonic() - started)

    def ping(self):
        # the liveness check every read makes, and the wipe detector. cheap by
        # contract: flipr's Ping never touches its store. raises FliprDown if
        # flipr is not there; heals if flipr is there but wearing a new
        # generation.
        resp = self._post("Ping", {})
        self._observe_generation(resp.get("storeGeneration"))
        return resp.get("version", "")

    def _observe_generation(self, gen):
        # the self-heal. a changed generation means flipr's store was wiped
        # and recreated: everything this service declared is gone. the cure
        # is to re-publish the declarations (idempotent, never clobbers an
        # operator's value) and drop the cache so the next read refetches.
        # operator flips are NOT ours to restore; the oplog replay is flipr's
        # own half of the heal.
        if gen is None:
            return
        changed = self._generation is not None and gen != self._generation
        first = not self._published
        self._generation = gen
        if changed:
            self._cache = {}
            self._fetched = 0.0
            self._metrics.generation_change()
            self._log("warn", "flipr store generation changed; republishing declared defaults", generation=gen)
        if changed or first:
            # the declarations reach flipr on the first answer after a boot
            # that found it down, and again after a wipe; never clobbering a
            # value an operator set
            self._publish_declared()
            self._published = True

    def _publish_declared(self):
        # guarded against the ping inside: _post does not ping, so no loop
        self._post(
            "PublishNamespace",
            {"namespace": {
                "service": self._service,
                "version": self._version,
                "flags": self._declared,
            }},
        )

    def refresh(self):
        # ping, then fetch the whole namespace in one round trip and replace
        # the cache. ping first: if flipr was wiped, the heal must run BEFORE
        # the fetch, or the fetch 404s on a namespace the heal is restoring.
        # returns the namespace as a dict of key -> value.
        self.ping()
        resp = self._post(
            "GetNamespace",
            {"service": self._service, "version": self._version},
        )
        flags = {}
        for f in resp.get("namespace", {}).get("flags", []):
            v = f.get("value", {})
            # the oneof arrives as exactly one of these keys
            if "boolValue" in v:
                flags[f["key"]] = v["boolValue"]
            elif "stringValue" in v:
                flags[f["key"]] = v["stringValue"]
            elif "intValue" in v:
                flags[f["key"]] = int(v["intValue"])
            else:
                flags[f["key"]] = None
        self._cache = flags
        self._fetched = time.monotonic()
        return flags

    def snapshot(self):
        # the namespace as of the last refresh, whole, for a health page
        return dict(self._cache)

    def namespace_flags(self):
        # the contract's namespace() verb: the whole namespace as data, from
        # flipr now (one trip), for a health page or a roster. `namespace` the
        # property is the name; this is the contents.
        return self.refresh()

    def check(self, key):
        # the call sites use this. in order:
        #   1. flipr must be THERE -- a fresh cache does not skip the ping
        #   2. a stale cache is refreshed; a fresh one is served
        #   3. an undeclared flag is missing, never "off"
        # under REFUSE, 1 raises FliprDown (or FliprLocked) and 3 raises
        # FlagMissing; under HOLD, 1 returns the last known value and says so
        # in last_known and last_why, and 3 still raises, because an
        # undeclared flag is a wiring mistake in either policy.
        try:
            age = time.monotonic() - self._fetched
            if age >= self._ttl or not self._cache:
                self.refresh()
            else:
                self.ping()
        except FliprDown as down:
            if self._policy == Policy.REFUSE:
                self.last_known, self.last_why = False, f"{down}; gates fail STOP"
                raise
            held = self._cache.get(key, self._declared_default(key))
            self.last_known, self.last_why = False, f"{down}; holding last known"
            return held
        if key not in self._cache:
            raise FlagMissing(
                f"flag {key!r} is not declared in {self.namespace}; "
                "publish it before gating on it"
            )
        self.last_known, self.last_why = True, ""
        return self._cache[key]

    def _declared_default(self, key):
        for f in self._declared:
            if f.get("key") == key:
                v = f.get("value", {})
                for k in ("boolValue", "stringValue", "intValue"):
                    if k in v:
                        return int(v[k]) if k == "intValue" else v[k]
        # a key nobody declared is not this service's to authorise
        return None

    def publish(self, flags=None):
        # declare this service's flags (the ones given at construction unless
        # a list is passed) and remember them so a wiped flipr can be
        # re-seeded on the spot. idempotent; never overwrites an operator's
        # value, which flipr guarantees server-side. call this at service
        # STARTUP: a service that publishes on boot heals a flipr wiped while
        # it was down.
        if flags is not None:
            self._declared = list(flags)
        n = self._post(
            "PublishNamespace",
            {"namespace": {
                "service": self._service,
                "version": self._version,
                "flags": self._declared,
            }},
        ).get("flagsPublished", 0)
        self._published = True
        # observe the generation ping-style so the baseline is set even if
        # the caller never pings explicitly
        self.ping()
        return n

    def list_namespaces(self):
        # every namespace flipr holds: a tool's verb (puffin's audit), never
        # a service's
        resp = self._post("ListNamespaces", {})
        return resp.get("namespaces", [])

    def locked(self):
        # whether the last answer from flipr was a lock, and why, for the
        # caller's own health line (CONTRACT.md section 9)
        return (self.locked_why is not None, self.locked_why or "")


# flipr's own error text out of a JSON body, for a lock's reason. _net keeps
# a snippet of the body, so a long one no longer parses; the error field is
# read by pattern in that case.
def _reason_of(body):
    try:
        m = json.loads(body)
        return m.get("error") or m.get("reason") or body
    except Exception:
        found = re.search(r'"(?:error|reason)"\s*:\s*"((?:[^"\\]|\\.)*)', body)
        return found.group(1) if found else body


# the sha256 over this file and _net.py together, the same bytes
# clients/bin/stamp.sh hashes, for a build to stamp as CLIENT_HASH
def source_hash():
    import os
    here = os.path.dirname(__file__)
    h = hashlib.sha256()
    for name in ("_client.py", "_net.py"):
        with open(os.path.join(here, name), "rb") as f:
            h.update(f.read())
    return h.hexdigest()[:12]

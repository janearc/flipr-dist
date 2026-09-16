# the flipr client contract

what every published client does, in every language, so a service written
against one behaves like a service written against another.

three clients are published, `go/`, `typescript/` and `python/`, and no
others. a service reaches flipr through the client for its language, at a
tag; code in a repository that posts to `flipr.v1.FliprService` itself is
a defect. if the client lacks something, the client changes.

this file is normative: where a client and this file disagree, the client
is wrong. `fixtures/` is this file made executable, and every client runs
it against a live flipr before it is tagged.

## 1. identity

a client is constructed with the service name and the namespace version,
and nothing it does is anonymous.

- **namespace.** `service@version`, where the version is the flag
  contract's version (kingfisher uses `v1`), never the commit, so
  operator values survive deploys. pinned in configuration, never derived.
- **caller header.** `X-Flipr-Caller: <service>@<version>` on every
  request, so a namespace that is a burden has a number.
- **client header.** `X-Flipr-Client: <lang>/<tag>/<hash>`, where lang is
  `go`, `ts` or `py`, tag is the client's release tag, and hash is the
  first twelve hex digits of the sha256 of the client's source at that
  tag. it is embedded at build time, never read from a file at run time.
- **address.** the base url is a name, `http://flipr.test` here, never an
  address; a port on a name is fine. the client appends
  `/flipr.v1.FliprService/<Method>`.

## 2. the verbs

five, the same five in every language. names follow the language's
convention, behaviour does not vary.

| verb | rpc | what it does |
|---|---|---|
| `ping` | Ping | reachability and the store generation |
| `refresh` | GetNamespace | the whole namespace into the cache in one trip |
| `check(key)` | cache, else GetFlag | one flag, with its outcome |
| `publish(flags)` | PublishNamespace | declare key, default, description, expensive; idempotent; never overwrites an operator's value |
| `namespace()` | GetNamespace | the namespace as data, for a health page |

two more are for tools, not services: `list()` (every namespace, for an
audit) and `set(namespace, key, value, reason)` (the operator path; a
service never calls it).

`check` pings, then reads the cache while it is fresh and refreshes
otherwise. `refresh` is what a service calls on its loop.

## 3. outcomes

every read has exactly one of four outcomes, and the caller can tell them
apart without parsing a message.

| outcome | meaning | how it surfaces |
|---|---|---|
| value | flipr answered with the flag | the value, `known: true` |
| missing | flipr answered and the flag or namespace is not declared | go and ts: `known: false` with why; py: `FlagMissing` |
| unreachable | nothing authoritative answered: refused connection, timeout, 5xx after retries, or a 404 that is not flipr's own json | go and ts: `known: false` with why; py: `FliprDown` |
| refused | flipr answered 4xx to a write: off-contract body, missing reason, over budget | the status and flipr's text; never retried |

absent but routed is unreachable. flipr's own 404 is a json error body
and means missing; a 404 with any other body is the edge answering for a
flipr that is not there. a guard that tested only whether something
answered once marched a build into a dead flipr.

a locked flipr is its own answer, `423 Locked` with a reason, and is
neither missing nor unreachable.

## 4. when nothing authoritative answered

the client does not decide. the caller declares a policy at construction
and the client applies it.

- **refuse.** unknown never authorises, and a boolean read returns false.
  for gates over spend, where a stale yes costs money.
- **hold.** unknown keeps the last known value, or the declared default
  if nothing has been read. for components whose job is to keep
  reporting. `known: false` is how the caller publishes that it is
  holding.

there is no default: a client constructed without a policy refuses to
construct.

## 5. cache, liveness and generation

clients cache the last read and always check that flipr is there. the
cache is a performance optimisation on the namespace fetch, never a
fallback for flipr's absence.

- **every check pings.** `Ping` never touches the store, and an
  unanswered ping is the unreachable outcome whatever the cache holds.
- **the namespace is cached** from `refresh` for a ttl the caller sets,
  five seconds by default. a check inside the ttl is a map read, and the
  first check after it refreshes in one trip. where the store revision is
  carried, a refresh happens only when it changed.
- **generation.** every answer carries the store generation, and the
  client remembers the first it sees. a change means the store was wiped:
  the client warns, drops its cache, republishes once, and continues.
- a republish restores defaults and never values, so an operator's lost
  values are visibly lost.

## 6. retry

every retryable call is retried with bounded exponential backoff and full
jitter. no caller adds its own retry around the client, and none turns
the client's off except to one attempt for a test.

- attempts include the first, three by default for reads and publishes
- the wait before attempt n is uniform in `[0, min(cap, base * 2^n))`,
  base 50 ms and cap one second. full jitter, because an outage
  synchronises the fleet and a shared floor rebuilds the herd
- retryable: connection errors, timeouts, 5xx, 429. not retryable: any
  other 4xx, and flipr's own 404
- `Retry-After` on 429 replaces the computed wait, capped at the lock
  ceiling; on 423 it sets the lock's deadline
- the total is bounded, so a dead flipr is reported promptly: the
  caller's policy handles away, not a longer retry

a service may lower attempts to one for an interactive path. it may not
raise the cap above the lock ceiling.

## 7. instruments

through an interface the caller supplies, where a no-op is fine:

    flipr_client_requests_total{method, outcome}
    flipr_client_retries_total{method}
    flipr_client_duration_seconds{method}
    flipr_client_generation_changes_total

outcome is `value`, `missing`, `unreachable`, `refused` or `locked`.
logging, through the caller's logger, is debug for a retry, warn for a
generation change or a lock, error for a refused publish. a value read is
never logged.

## 8. authenticity

flipr labels every request by `X-Flipr-Client`, checked against the
`known.*` flags of the namespace `flipr@clients`, one
`<lang>/<tag>/<hash>` each, written by `fliprctl clients add` and read
like any flag, so a new tag needs no restart.

a known triple is counted under its `<lang>/<tag>`. a hash that does not
match its tag, a tag the set lacks, or a client that did not stamp is
`other`; no header at all is `none`.

an operator sees both on the dashboard and may turn on refusal with
`refuse.other`, which answers 403 with `unknown_client` on every rpc,
Ping included, so the service behind it goes loud at once.

this is not attestation against a forger. it is a copy announcing itself
as a copy, so that preferring one's own call site is visible before it is
enforced.

## 9. lock

flipr may be locked, whole or per namespace, by an operator through
`fliprctl lock` with a reason. no service, session or flipr ever locks on
its own judgement.

a locked namespace answers every read and write with `423 Locked`, a
reason, a time and `Retry-After`. the client:

- treats it as the `locked` outcome and does not retry inside the call
- remembers it with a deadline, `Retry-After` capped at the lock ceiling
  of thirty seconds, five when absent, and while the deadline stands
  answers from the policy at once and makes no request, so a hot path
  cannot storm a locked flipr
- applies the caller's policy meanwhile, and reports `holding on a flipr
  lock: <reason>` in its own health

## 10. configuration, and what is refused

required at construction: service, version, base url, declared flags,
policy, logger. a client refuses to construct on a missing one, a version
that looks like a commit hash, a base url whose host is an address rather
than a name (loopback excepted, for tests), or an empty declared list.

a flipr that is down is never a construction error: a service that will
not start because the flag store is down is a second outage.

nor is it env mode. env mode is chosen by an empty base url, for a laptop
with no enclave, and is never fallen into.

a flipr that was named and did not answer leaves the client in flipr mode
with the policy applied to every read, because an outage must not make a
service more willing to spend. every check pings, so the next answer is
the promotion and the declarations land with it.

## 11. fixtures

`fixtures/` holds one scenario file per section in a language-neutral
shape, and a runner per language that plays them against a live flipr
named by `FLIPR_TEST_BASE`. a client is tagged only when its runner is
green.

the scenarios: each verb; each outcome, including absent but routed; each
policy; the generation change and republish; retry counts and bounds
under an injected failing edge; the instrument names; the header shape;
the lock.

## 12. versioning

tags are `client-ts/v<major>.<minor>.<patch>`, `client-py/v...`, and
`clients/go/v...` for the go module, which is the go command's rule for a
module in a subdirectory.

a consumer pins a tag: go by module version, typescript and python as git
dependencies at the tag. a patch is a tag and a pin bump in each
consumer, never a copy. a change to this file bumps every client's minor,
and the changelog names the section.

## 13. building a client at a tag

`clients/bin/stamp.sh <go|typescript|python>` prints the tag, the newest
`client-<lang>/v*` reachable from HEAD or else `dev`, and the hash, over
the client's source files.

all three clients commit a generated stamp module, written by `stamp.sh
<lang> --write` at release and committed as `dev`/`unknown` between tags,
so a checkout announces itself as a checkout.

the release is: bump the version field, run `stamp.sh <lang> --write
--tag vX.Y.Z` with the tag named ahead, commit, tag that commit
`client-<short>/vX.Y.Z`, push the tag. the hash covers the source and not
the stamp module, so it is the same before and after the stamp commit.

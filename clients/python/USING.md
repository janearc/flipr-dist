# Using the flipr Python client

    uv add "flipr-client @ git+https://github.com/janearc/flipr-dist@TAG\
    #subdirectory=clients/python"

with TAG the release you pin, and the two lines joined into one string.

Stdlib only, no dependencies, and it stays that way. The Go client next door
explains why it is its own module and the reasoning is identical: a consumer
importing this must not inherit flipr's server dependencies. A metrics daemon
that wants to read one flag has no business building against a kafka client.

Pin a tag. `uv` and `pip` both install from a git subdirectory, and a version
bump becomes a reviewable line in a diff instead of a copy-paste nobody sees.

## Why this is a package now

it was three hand-copied files, drifted to 175, 143 and 214 lines.

the copy carrying the instruction "do not diverge here; fixes land
upstream first" was the only one following it, and it pointed at a
repository that was not the source.

"Do not diverge" is unenforceable advice. "There is only one" is a fact.

## Usage

    import flipr_client
    from flipr_client import Policy

    DECLARED = [
        {"key": "fetch.enabled", "value": {"boolValue": True},
         "description": "on: tiles are fetched, one request per tile. "
                        "off: nothing is fetched.",
         "expensive": True},
    ]
    flags = flipr_client.FliprClient("http://flipr.test", "kingfisher", "v1",
                                     declared=DECLARED, policy=Policy.REFUSE)
    flags.publish()
    if flags.check("fetch.enabled"):
        do_the_expensive_fetch()

This client follows `../CONTRACT.md`; where the two disagree, the client is
wrong. The version is the flag contract's (`v1`), pinned, never a commit;
the base is a name, never an address (a port on a name is fine).

**every `check()` pings.** a check inside `cache_ttl` of the last refresh
reads the cached namespace; the first check after it refreshes the whole
namespace in one trip.

a changed store generation drops the cache and republishes the
declarations, so a service heals a wiped flipr without a restart.

**the policy is yours and has no default.**

`Policy.REFUSE`: `check()` raises `FliprDown` when flipr cannot be reached,
for gates over spend. do not catch it and carry on.

`Policy.HOLD`: `check()` returns the last known value, or the declared
default, and says so in `last_known` and `last_why`, for instruments where
going quiet is the failure. publish `last_known` in your own health.

under either policy `FlagMissing` means the flag is not declared in your
namespace. publish it before gating on it: a silent default would let a
typo ship as an off switch.

## The four outcomes, and the two 404s

| outcome | means | `REFUSE` | `HOLD` |
|---|---|---|---|
| value | flipr answered | the value | the value |
| missing | flipr answered; the flag or namespace is not declared | `FlagMissing` | `FlagMissing` |
| unreachable | nothing authoritative answered | `FliprDown` | last known, `last_known=False` |
| locked | an operator locked flipr (`fliprctl lock`) | `FliprLocked`, a `FliprDown` | last known; `locked()` says why |

a 404 is one of two different facts and the client tells them apart:
flipr's own has a json body and is missing, while a plain-text one is the
edge answering for a flipr that is not there, and is unreachable.

absent but routed is down. a guard that tested only whether curl succeeded
once marched a script into a dead service.

## Retries

every call goes through backoff with full jitter: three attempts, 50 ms
base, one second cap, `Retry-After` honoured up to thirty seconds.

retried: transport errors, 5xx, 429, 423, and a plain-text 404. any other
4xx is the contract speaking.

that is tighter than a background job would use, because every flag check
reads through: a long retry would turn a flipr outage from a fast refusal
into a stall on every gated call.

## Identity

every request carries `X-Flipr-Caller: <service>@<version>` and
`X-Flipr-Client: py/<tag>/<hash>`.

the tag and hash come from `FLIPR_CLIENT_TAG` and `FLIPR_CLIENT_HASH` at
package build, and a build that set neither announces itself as
`dev/unknown`, which flipr counts as an unknown client.

## Instruments

Pass `metrics=` an object with `request(method, outcome)`, `retry(method)`,
`duration(method, seconds)` and `generation_change()`, and the client emits
the contract's series through it. The default emits nothing.

## Logging

Retries are logged as structured JSON on stdout, because a retry nobody can see
is how a dependency degrades for a week unnoticed. If your service has its own
logger, hand it over once at startup and everything lands in your stream:

    flipr_client.set_logger(my_log)   # my_log(level, event, **fields)

## Tests

    FLIPR_TEST_BASE=http://flipr.test:9800 python3 flipr_client_test.py

No mocks, by design: the suite drives a live flipr end to end. A client tested
against a fake of the server it exists for proves the fake, not the client.

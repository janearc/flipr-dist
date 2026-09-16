# Using the flipr Go client

    import flipr "github.com/janearc/flipr-dist/clients/go"

Its own Go module, with one dependency. Importing the flipr repo itself would
drag in franz-go, bbolt and big-little-mesh, and a metrics daemon that reads
one flag has no business building against a kafka client.

## The one thing you must choose

the cache is a performance optimisation and never a fallback. every check
pings, so flipr's absence is seen every time.

a check inside `CacheTTL` of the last refresh, five seconds by default,
reads the cached namespace; the first check after it refreshes the whole
namespace in one trip. a changed generation drops the cache and
republishes the declarations.

an unanswered ping is reported as unknown, and the cache is never served
past its TTL when flipr is away: what happens then is the policy below.

this client follows `../CONTRACT.md`, and where they disagree the client
is wrong.

What to DO about unknown is the one thing this package refuses to decide,
because the two right answers point in opposite directions.

| policy | for | unknown means |
|---|---|---|
| `flipr.Refuse` | gates over spend | stop. A stale yes authorizes money nobody approved. |
| `flipr.Hold` | instruments | carry on with the last known answer, and publish the uncertainty. Going quiet is itself the failure. |

gaggle is `Refuse`: its flags guard GPU hours and LLM calls. metricsd is
`Hold`: its flag decides whether a collector reports, and silencing telemetry
because the flag store wobbled removes the instrument in exactly the incident
it exists for.

Picking wrong is quiet in both directions, so there is no default. A `Config`
without `OnUnknown` is rejected at construction.

## Using it

```go
fl, err := flipr.New(flipr.Config{
    Service:   "metricsd",
    Version:   "v1",            // PIN it; see below
    URL:       os.Getenv("METRICSD_FLIPR_URL"),
    EnvPrefix: "METRICSD",
    OnUnknown: flipr.Hold,
    Log:       log,
    Declared: []flipr.Flag{{
        Key: "smc.enabled", On: true,
        Desc: "on: temperature, fan and power published every 15s. off: those series go absent within one interval.",
    }},
})
if err != nil {
    return err // misconfiguration only; a flipr that is DOWN is not an error
}

on, known, why := fl.Check("smc.enabled")
```

`Check` keeps the value and what is known about it apart. `on` is what to
act on, already resolved through your policy; `known` says whether
anything authoritative answered.

publish `known` in your metrics too: "the flag is off" and "we could not
ask" have to be distinguishable from outside the process. `Enabled` is the
same call for callers that only want a yes or no.

## Retry

every call is tried three times: 50 milliseconds after the first failure,
doubling to a one second ceiling, with full jitter so a fleet that lost
flipr together does not ask again together.

that covers a dropped packet, a reset connection, or an edge that has not
learned a new pod. it does not span a flipr restart and is not meant to:
what a caller does then is `OnUnknown`.

retried: transport errors, 5xx, 429, 423, and a 404 whose body is not
flipr's own json. any other 4xx is the contract speaking. `Retry-After` is
honoured, capped at thirty seconds.

A locked flipr (`423 Locked`, set by an operator through `fliprctl lock`) is
its own outcome: the policy applies, `Locked()` says why for your health
line, and the next good answer clears it.

Every request carries `X-Flipr-Caller: <service>@<version>` and
`X-Flipr-Client: go/<tag>/<hash>`; the tag and hash are set at build time
with `-ldflags` and a build that did not set them is counted by flipr as an
unknown client.

a transport failure or a 5xx is retried; a 4xx is returned at once.

every flipr rpc is a POST and every one this client makes is safe to
repeat, so POSTs are retried: a helper that skipped them would retry
nothing here. the error after the last attempt says how many were made and
how long they took.

    flipr.Config{..., Retry: flipr.Retry{Attempts: 3}}   // more tries
    flipr.Config{..., Retry: flipr.Retry{Attempts: 1}}   // one try, no retry

The per-attempt timeout is the `HTTP` client's, five seconds by default.

## Two modes

Chosen at startup, never mixed.

**flipr mode** -- first contact succeeded. Values are refreshed once per
`CacheTTL`, so an operator flip lands within it; `CacheTTL: NoCache` reads
fresh on every check.

**env mode** -- no url. chosen, never fallen into: a configured flipr that
does not answer at boot keeps the client in flipr mode, with the policy
applied to every read, until it answers.

gates read `<PREFIX>_<KEY>_ENABLED`, so `smc.enabled` becomes
`METRICSD_SMC_ENABLED`. this is pre-onboarding behaviour and not a fallback
from a flipr that was ever reachable.

env answers are always `known`: nothing went unanswered, there was simply
nothing to ask.

A flipr that is down does **not** fail construction. A service that will not
start because a flag store is down is useless in the incident it exists for.
Misconfiguration does fail, because an operator can flip their way out of one
and not the other.

## Pin the version

`Version` is the namespace's second half, and it should be a pinned string, not
a build commit. Per-commit namespaces are born carrying the declared defaults,
so every deploy silently resurrects flags an operator had killed.

## Declarations

`Declared` is published at boot, idempotently -- it never overwrites a value an
operator set. If flipr's store generation changes (a wipe or rebuild), the
client republishes automatically, so flags nobody has touched do not vanish.

Write `Desc` for whoever reads it at 03:20: what ON does, what OFF does, and
what happens to work already in flight. Mark `Expensive` only for real spend;
puffin renders it as `$` and a cheap flag wearing that marker dilutes it.

## Regenerating

`bin/gen.sh` at the repo root generates both this module's types and the
server's, from the one proto, with the plugin version pinned and installed
repo-locally. Run it and the tree must be clean.

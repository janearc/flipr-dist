# Using the flipr TypeScript client

```ts
import { Client, Policy, boolValue } from "@janearc/flipr";
```

Its own npm package, with one runtime dependency. Importing the flipr repo
itself would drag in the server's world; a consumer of a flag store should need
nothing but the wire.

## The one thing you must choose

the cache is a performance optimisation and never a fallback. every check
pings, so flipr's absence is seen every time.

a check inside `cacheTtlMs` of the last refresh, five seconds by default,
reads the cached namespace; the first check after it refreshes the whole
namespace in one trip. a changed generation drops the cache and
republishes the declarations.

an unanswered ping is reported as unknown, and the cache is never served
past its TTL when flipr is away: what happens then is the policy below.

this client follows `../CONTRACT.md`, and where they disagree the client is
wrong.

What to DO about unknown is the one thing this package refuses to decide,
because the two right answers point in opposite directions.

| policy | for | unknown means |
|---|---|---|
| `Policy.Refuse` | gates over spend | stop. A stale yes authorizes money nobody approved. |
| `Policy.Hold` | instruments | carry on with the last known answer, and publish the uncertainty. Going quiet is itself the failure. |

Picking wrong is quiet in both directions, so there is no default. A `Config`
without `onUnknown` is rejected at construction.

## Using it

```ts
const flags = await Client.connect({
  service: "metricsd",
  version: "v1",             // PIN it; see below
  url: process.env.METRICSD_FLIPR_URL ?? "",
  envPrefix: "METRICSD",
  onUnknown: Policy.Hold,
  log,
  declared: [{
    key: "smc.enabled",
    value: boolValue(true),
    desc: "on: temperature, fan and power published every 15s. off: those series go absent within one interval.",
    expensive: false,
  }],
});

const { on, why } = await flags.enabled("smc.enabled");
```

`connect` rejects only on a **misconfigured** Config. A flipr that is DOWN is
not an error: a service that will not start because a flag store is down is
useless in the incident it exists for. An operator can flip their way out of one
and not the other.

`check` separates the value from the epistemics:

```ts
const { value, known, why } = await flags.check("smc.enabled");
```

`value` is what to act on, already resolved through your policy. `known` says
whether anything authoritative answered. **If your service publishes metrics,
publish `known` too** — "the flag is off" and "we could not ask" must be
distinguishable from outside the process.

## Flags are not only booleans

The proto says a value is bool **or** string **or** int64, and says why: a
setting like `model.backend` set to `ollama` or `claude` would otherwise be two
booleans that can both be true. This client reads all three.

```ts
const { value } = await flags.checkString("model.backend");  // "ollama"
const { value: n } = await flags.checkInt("batch.size");     // 64n, a bigint
```

`int` is a `bigint` because the wire is int64 and a JavaScript number silently
loses precision above 2^53.

`enabled()` is the boolean shorthand. A non-boolean flag read through it is
`false` with a stated reason rather than a coerced truthy value — silently
treating the string `"false"` as a yes is exactly the bug the oneof prevents.

## Two modes

Chosen at startup, never mixed.

**flipr mode** — first contact succeeded. Values are refreshed once per
`cacheTtlMs`, so an operator flip lands within it; `cacheTtlMs: NO_CACHE`
reads fresh on every check.

**env mode** -- no url. chosen, never fallen into: a configured flipr that
does not answer at boot keeps the client in flipr mode, with the policy
applied to every read, until it answers.

gates read `<PREFIX>_<KEY>_ENABLED`, so `smc.enabled` becomes
`METRICSD_SMC_ENABLED`; a key that does not end in `.enabled` takes the
plain form, and `model.backend` reads `METRICSD_MODEL_BACKEND`.

this is pre-onboarding behaviour and not a fallback from a flipr that was
ever reachable. env answers are always `known`: nothing went unanswered,
there was simply nothing to ask.

## Pin the version

`version` is the namespace's second half, and it should be a pinned string, not
a build commit. Per-commit namespaces are born carrying the declared defaults,
so every deploy silently resurrects flags an operator had killed.

## Declarations

`declared` is published at boot, idempotently — it never overwrites a value an
operator set. If flipr's store generation changes (a wipe or rebuild), the
client republishes automatically, so flags nobody has touched do not vanish.

Write `desc` for whoever reads it at 03:20: what ON does, what OFF does, and
what happens to work already in flight. Mark `expensive` only for real spend;
puffin renders it as `$`, and a cheap flag wearing that marker dilutes the one
signal an operator has during an incident.

## Metrics

Pass a `metrics` sink and the client publishes its own series. It is an
interface rather than a registry because a library must not own one — these
belong in the surface your service already has. Omit it and you pay a no-op
call.

| series | type | what it tells you |
|---|---|---|
| `flipr_client_up` | gauge | 1 in flipr mode, 0 in env mode. |
| `flipr_client_flag_state{key}` | gauge | 1/0 per boolean flag. **This is how a flip becomes visible on a graph.** |
| `flipr_client_reads_total{key,outcome}` | counter | `ok` / `unknown` / `env`. |
| `flipr_client_rpc_total{method,outcome}` | counter | `ok` / `error`. One per call, not one per attempt. |
| `flipr_client_rpc_retries_total{method}` | counter | attempts past the first. **A flapping flipr shows up here first.** |
| `flipr_client_rpc_duration_seconds{method}` | histogram | seconds. |
| `flipr_client_generation_changes_total` | counter | store wipes observed. |
| `flipr_client_publish_total{outcome}` | counter | `ok` / `refused`. |

Series for every declared flag are initialised at zero on connect, because a
`rate()` over a series that has never incremented is not zero — it is absent,
and absent does not alert.

## Retries

Every call is retried with bounded exponential backoff and **full jitter** —
three attempts, 50ms base, 1s cap, all configurable through `retry`.

It is **bounded** because the doctrine still holds: an unanswered read is
unknown, the caller is blocked on it, and a client that retried until it got an
answer would turn "flipr is down" into an unbounded stall.

the jitter is the point, and it is not about this client's latency. every
service pings flipr on every check, so an outage synchronises the fleet:
they fail together and, without jitter, come back together.

that wave arrives at the process that has just started, when it can least
absorb one. full jitter, a random point in `[0, window)` rather than the
window itself, spreads it most evenly, because equal and decorrelated
jitter both leave a floor under the delay.

retried: a transport failure, a 429, a 423, a 5xx, and a 404 whose body is
not flipr's own json. those are the shapes a restart, a rate limit, a lock
and an absent flipr take.

any other 4xx is not: asking the same wrong question three times only
delays the error. `Retry-After` is honoured, capped at thirty seconds.

A locked flipr (`423 Locked`, set by an operator through `fliprctl lock`) is
its own outcome: the policy applies, `locked()` says why for your health line,
and the next good answer clears it.

every request carries `X-Flipr-Caller: <service>@<version>` and
`X-Flipr-Client: ts/<tag>/<hash>`.

the tag and hash come from `FLIPR_CLIENT_TAG` and `FLIPR_CLIENT_HASH` at
package build, and a build that set neither announces itself as
`dev/unknown`, which flipr counts as an unknown client.

Set `retry: { attempts: 1, ... }` to disable it.

## What it will not do

There is no `setFlag()` and no `deleteNamespace()`. Those are **operator**
operations: the proto requires a `reason` on both, precisely because an
unexplained flip is how an outage becomes unexplainable, and a reason invented
by the service that benefits from the flip is not a reason.

It also keeps the `X-Flipr-Caller` attribution honest. Every call declares
`service@version`, so whoever mines the oplog for who touched a namespace can
rule this client out at a glance — and that is only true while it stays true.

**This client has no flags of its own.** It never asks flipr whether to be a
client. Every knob is constructor configuration, resolved once, because a flag
store client whose behaviour depends on the flag store cannot be reasoned about
when the flag store is the thing that is broken.

## Differences from the Go client

Go is canonical and this mirrors it: same config fields, same
check/enabled/namespace/mode/declared surface, same two modes, same generation
self-heal, same env-var derivation, same refusal to call SetFlag. It differs in
four places, each deliberate:

1. **It reads the whole `Value` oneof.** The Go client calls `GetBoolValue()`
   and can express only the boolean third of the contract. String flags are live
   in the store today, so this is a real gap rather than a hypothetical one.
2. **Construction is async.** A TypeScript constructor cannot await, and a
   client that returned before it knew its own mode would report `env` for a
   moment and then change its mind. `await Client.connect(cfg)`.
3. **It is instrumented.** The Go client publishes no metrics, which leaves the
   package deciding whether the expensive departments run as the one place with
   no numbers. Worth porting back.
4. **It implements `GetNamespace`** as `snapshot()`. The proto describes it as a
   client call; the Go client just does not have it yet.

## Tests

```
npm test        # 36 cases; the list mirrors clients/go/flipr_test.go
npm run coverage
npm run smoke   # against a LIVE flipr; publishes a throwaway namespace and retires it
```

## Regenerating

`bin/gen.sh` at the repo root generates the server's types, the Go client's and
this one, from the one proto, with every plugin version pinned and installed
repo-locally. Run it and the tree must be clean.

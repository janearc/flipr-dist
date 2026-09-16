# flipr TypeScript client

```ts
import { Client, Policy, boolValue } from "@janearc/flipr";
```

Its own package, with one runtime dependency: a consumer of a flag store
needs the wire and nothing else.

```ts
const flags = await Client.connect({
  service: "metricsd", version: "v1", envPrefix: "METRICSD",
  url: process.env.METRICSD_FLIPR_URL ?? "", onUnknown: Policy.Hold,
  declared: [{ key: "smc.enabled", value: boolValue(true),
    desc: "on: series published. off: they go absent.", expensive: false }],
});
const { on, why } = await flags.enabled("smc.enabled");
```

Every check pings flipr first; the cache is never a fallback. Unknown means
what the policy says: Refuse for spend, Hold for instruments. Construction
is async so the client knows its mode before it answers.

USING.md covers modes, declarations, metrics, retries and tests.
../CONTRACT.md is what every flipr client keeps.

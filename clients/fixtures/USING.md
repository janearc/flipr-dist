# Contract fixtures, in full

One scenario file per section of `../CONTRACT.md`, in a language-neutral
shape, and a runner per language. A client is tagged only when its runner is
green against every scenario at that tag, against a live flipr named by
`FLIPR_TEST_BASE` (a running deployment, or a throwaway).

Scenario files are JSON, one object per scenario:

    {"id": "outcome.missing",
     "section": 3,
     "given":  {"namespace": "fixture@v1", "declared": ["a.enabled"]},
     "when":   {"verb": "check", "key": "b.enabled"},
     "then":   {"outcome": "missing", "known": false}}

The runner constructs a client for `fixture@v1` with the scenario's declared
flags and policy, performs `when`, and asserts `then`.

Scenarios that need a failing edge (`retry.*`, `outcome.unreachable`,
`lock.*`) run against a stand-in the runner starts on a loopback port,
which answers as the scenario says: refuse connections, 503 N times then
200, 423 with Retry-After.

The contract's bounds are asserted on wall time and on the client's own
instruments.

Scenario files:

- `identity.json`: headers present and shaped; version that looks like a
  commit refused at construction.
- `verbs.json`: each of the five, plus list and set for the tool clients.
- `outcomes.json`: value, missing, unreachable (including absent-but-routed),
  refused.
- `policy.json`: Refuse and Hold under each outcome; no default.
- `generation.json`: a generation change republishes once.
- `retry.json`: attempts, full jitter within bounds, retryable and not,
  Retry-After honoured, total bounded.
- `instruments.json`: the four series and their labels exist after one of
  each outcome.
- `lock.json`: 423 is its own outcome, policy applied, ceiling thirty
  seconds, never gives up, health line present.

Runners: `go/contract_test.go`, `typescript/contract.test.ts`,
`python/flipr_client_contract_test.py`. Each reads every scenario file here.

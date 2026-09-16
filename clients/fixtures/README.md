# Contract fixtures

One scenario file per section of `../CONTRACT.md`, in a shape any language
can read, and a runner per language. A client is tagged only when its
runner is green against every scenario, against a live flipr named by
`FLIPR_TEST_BASE`.

    {"id": "outcome.missing",
     "section": 3,
     "given":  {"namespace": "fixture@v1", "declared": ["a.enabled"]},
     "when":   {"verb": "check", "key": "b.enabled"},
     "then":   {"outcome": "missing", "known": false}}

The runner builds a client for `fixture@v1` with the scenario's flags and
policy, does `when`, and asserts `then`. Scenarios that need a failing edge
run against a stand-in on a loopback port that answers as the scenario says.

USING.md lists every scenario file and what each one holds.

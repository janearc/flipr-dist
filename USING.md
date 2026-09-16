# using flipr

the flag store. what it answers and why it is shaped that way is
DESIGN.md; this is how to run it and talk to it.

## running it

    flipr -addr 127.0.0.1:15100 -db /var/lib/flipr/flipr.db

SIGTERM drains in-flight requests and closes the store. `kill -9` is
also fine: every write is durable before its response is sent.

| setting | flag | environment | default |
|---|---|---|---|
| listen address | `-addr` | `FLIPR_ADDR` | `127.0.0.1:15100` |
| database path | `-db` | `FLIPR_DB` | `flipr.db` |
| the oplog file | `-oplog` | `FLIPR_OPLOG_FILE` | `<db>.oplog`; empty means stdout |
| kafka brokers for the mirror | `-kafka` | `FLIPR_KAFKA_BROKERS` | empty: no mirror |
| oplog topic | `-topic` | `FLIPR_OPLOG_TOPIC` | `flipr.oplog` |
| schema registry, with kafka | -- | `FLIPR_SCHEMA_REGISTRY` | `http://schema-registry:8081` |
| debug lines | -- | `FLIPR_DEBUG` | unset; `log.level` overrides it at runtime |

that is the whole configuration surface, and there is no config file. a
deployed flipr always carries brokers, because kafka is below it in the
boot order and a flipr that cannot log refuses flips.

## asking how it is

    curl -s http://127.0.0.1:15100/health

| path | what it gives you |
|---|---|
| `/health` | reads the store, the sink and the last write: healthy, degraded (reads are true, flips are failing; `reasons` says why) or down. never an uncritical 200 |
| `/metrics` | prometheus text: rpc counts and latencies, store timings, queue depth, rollbacks, runtime. every series exists from the first scrape |
| `/api` | flipr's own contract, as a FileDescriptorSet |

in a cluster the pod answers at `http://flipr.test`, `bin/flipr` drives
it from a terminal, and OPERATION.md says what to do about degraded.

## talking to it

protojson over plain http, one route per proto method:

    POST /flipr.v1.FliprService/Ping
    POST /flipr.v1.FliprService/GetFlag
    POST /flipr.v1.FliprService/GetNamespace
    POST /flipr.v1.FliprService/SetFlag
    POST /flipr.v1.FliprService/PublishNamespace
    POST /flipr.v1.FliprService/ListNamespaces
    POST /flipr.v1.FliprService/DeleteNamespace

onboard a service:

    curl -X POST \
      http://127.0.0.1:15100/flipr.v1.FliprService/PublishNamespace -d '{
      "namespace": {"service": "albatross", "version": "a1b2c3d", "flags": [
        {"key": "nightly.smoothing",
         "value": {"stringValue": "surreal"},
         "description": "surreal or claude",
         "expensive": true}
      ]}}'

flip something. the reason is required, recorded and shipped:

    curl -X POST http://127.0.0.1:15100/flipr.v1.FliprService/SetFlag -d '{
      "service": "albatross", "version": "a1b2c3d",
      "key": "nightly.smoothing",
      "value": {"stringValue": "surreal"},
      "reason": "claude smoothing is banned; data owns the replacement"}'

retire a namespace when a deploy is gone for good, reason required:

    curl -X POST \
      http://127.0.0.1:15100/flipr.v1.FliprService/DeleteNamespace -d '{
      "service": "albatross", "version": "a1b2c3d",
      "reason": "stale deploy namespace"}'

a flip carries a typed value, may not change a flag's type, may not be
the 25th flag in a namespace, and names things the way the store does:
dotted lowercase keys, no `@` or whitespace in a service or version.
each refusal is a 400 with the reason, and it is counted.

unknown fields are refused rather than ignored, so an off-contract
request never arrives.

## namespaces and flags

a namespace is one service at one version, so `albatross@a1b2c3d` and
`albatross@deadbee` are separate namespaces under `albatross`.

publishing is part of a service's build and never overwrites a value: a
deploy says which flags exist, an operator says what they are set to.

keys are dotted and the dots are the hierarchy. values are a bool, a
string or an integer. `expensive` says whether acting on the flag costs
network, disk, cpu or a model call, and the emergency page is the set
where it is true, computed rather than kept by hand.

## the global scope

`_global@_` answers for every service that does not set its own flag.
resolution is server-side, the service's own flag always wins, and
`GetNamespace` returns the merged view, so no client carries precedence
code.

its first convention is `log.level`. set it globally and every
application that honours the convention changes level; flipr honours it
itself, on the next operation, with no restart:

    curl -X POST http://flipr.test/flipr.v1.FliprService/SetFlag -d '{
      "service": "_global", "version": "_",
      "key": "log.level", "value": {"stringValue": "debug"},
      "reason": "chasing a bug across the whole mesh"}'

an absent `log.level` leaves each service's environment default standing.

## clients

go, python and typescript under `clients/`, each its own module at its
own tag; `clients/CONTRACT.md` is what all three keep. an unanswered
read is reported as unknown rather than guessed, and the caller decides
what unknown means: a spend gate refuses, an instrument keeps reporting.

every client announces itself as `X-Flipr-Client: <lang>/<tag>/<hash>`,
and flipr counts requests by client against the set in `flipr@clients`
(`fliprctl clients`). a copy or a curl shows as `other` or `none`;
refusing those is a flag, off by default.

## storage

bbolt, never on the read path. a read is a pointer load and a map lookup
against an immutable snapshot.

a write goes to bbolt, publishes a fresh snapshot, then writes the oplog
record, and if the record cannot be written the write rolls back.

the oplog is a file beside the store, one record per line, flips fsynced
before their caller is answered.

it is the replay source: a store with no generation replays the file to
its end before serving, and refuses to serve if a record could not be
applied. kafka mirrors the file and never fails a flip.

reads are not recorded; they are counted in the metrics by method and
caller.

every write bumps the revision inside its own transaction. every answer
carries it, `/health` shows it, and every oplog record is stamped with it.

a client that sends `since` gets `{"revision": N, "unchanged": true}` when
nothing has changed. reads never move it, and it is the number a restore
names.

## operating it

`fliprctl` works on the files on the host: `status`, `log`, `assess`,
`lock`, `unlock`, `restore --to-revision N`. RUNBOOK-restore.md is its
page. `bin/deploy.sh <environment>` puts a commit in a cluster.

`proto/flipr/v1/flipr.proto` is the source of truth for the wire, and
everything under `gen/` is generated from it.

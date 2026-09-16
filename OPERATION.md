# operating flipr

if flipr is down the network is refusing on purpose, not broken at
random. that is the first thing to remember at 03:20.

one pod, namespace `flipr`, in a k3d cluster, reached by name at
`http://flipr.test`. the control script is `bin/flipr`; link it onto your
path.

    flipr status        pod, image, restarts, and what /health says
    flipr health        the raw /health document
    flipr flags         every namespace and flag
    flipr get SVC VER KEY
    flipr set SVC VER KEY '{"boolValue":false}' "why"
    flipr restart       same image, new pod
    flipr stop --yes    ring 0 goes away until `flipr start`

## is it up, and is it well

    curl -s http://flipr.test/health

| status | http | meaning | what to do |
|---|---|---|---|
| `healthy` | 200 | the store reads, the sink writes, the last write succeeded | nothing |
| `degraded` | 200 | reads are served and true; flips are failing or unrecorded, and `reasons` says which | the conditions below |
| `down` | 503 | the store cannot be read | the probes restart the pod; see the store is gone |
| no answer | | the pod is not running, or replay is in progress | `flipr status`: `Init` waits for kafka, `Running` and not `Ready` is replaying |

degraded is a 200 on purpose, because the probes read `/health` and a
restart fixes neither a refused sink nor a full volume. never read a 200
without reading `status`.

`version` is the short commit the binary was built from, and `dev` means
it was not built by the deploy script.

## condition: flips are failing

verify: `flipr health` says degraded and names the sink in `reasons`, and
`flipr_store_rollbacks_total` is moving.

what you can do: nothing in flipr needs restarting. fix the sink. a flip
is committed to bbolt, published to readers, then written to the oplog,
and if the oplog write fails the flip is rolled back and the caller gets a
500 saying so.

the implications: readers never keep a value the audit does not hold. if
the rollback itself failed, the 500 says that too and the store holds a
value the log does not: that is the one state to escalate rather than wait
out.

kafka down, while it is still the mirror, is not this condition: reads and
flips keep working, the record is in the file, and
`flipr_oplog_mirror_errors_total` climbs. kafka slow looks like nothing at
all, because the mirror is off the request path.

## condition: the volume is full

verify: publishes and flips fail with `file resize error` from bbolt,
reads continue, and `/health` is degraded with the last write error.

what you can do: free space on the node. the flag file is hundreds of
bytes per flag, so this is the node's disk and not flipr's appetite.

the implications: any successful write clears the reason.

## condition: the store is gone

verify: `/health` answers 503 `down` with `store unreadable`.

what you can do: let the probes restart the pod. if the volume is intact
the pod comes back with the same generation and nothing is lost.

the implications: if the volume is gone the pod mints a new generation,
replays the oplog, and every client notices the change on its next ping
and republishes its declarations. the new generation is logged loudly at
open.

## condition: replay refuses to start

verify: the pod logs `oplog replay failed; refusing to serve a
half-restored store`, with `replay:` lines above it naming the offset.

what you can do: read the record. decide with the operator whether it is
garbage to skip by hand or a flip to apply by hand. there is no flag to
skip it.

the implications: a store missing an operator's flip that calls itself
restored is worse than a pod that will not start. published defaults come
back on each service's next deploy regardless; replay is what restores the
operator's flips on top of them.

## condition: flipr refuses a request

verify: a 400 with a reason, counted under
`flipr_rpc_rejected_total{reason=...}`. the reasons are:

- a flip with no reason, or no value
- a flip that would change a flag's type
- a 25th flag in a namespace
- a key that is not dotted lowercase, or a service or version with `@`
  or whitespace in it
- an expensive flag that is not a boolean, is negated, or whose
  description does not state both polarities
- any field the contract does not declare

there is also one 403, `unknown_client`, only when
`flipr@clients/refuse.other` is on: a client the known set does not hold,
or none at all, on every rpc including Ping.

what you can do: add the client, or turn refusal off.

    fliprctl clients add go/v1.1.0/<hash> --reason "..."
    fliprctl clients refuse-other off --reason "..."

the triple is what `clients/bin/stamp.sh go` prints at the tag, and the
next request sees it. with the pod down and the store free, the same
commands write the files and the record instead.

the implications: `fliprctl` is the one caller that cannot add itself. it
is known when it is the server's own build, so a host tool built past the
deployed commit is `other`, which is only felt with refusal on. run it
from the pod, or build it from the deployed commit.

    kubectl -n flipr exec deploy/flipr -- \
      flipr ctl -url http://127.0.0.1:15100 clients

## condition: a namespace is the burden

verify: `flipr_namespace_requests_total{namespace,method}` and
`flipr_namespace_response_bytes_total{namespace}`, and the dashboard's two
burden panels.

what you can do: read the shape. one read per second per pod is a client
with no cache interval, or one polling without the `since` checkpoint. a
publish rate above zero is a service republishing on every loop.

the implications: the label caps at 64 namespaces and says `other` past
that, so a service versioning by commit cannot grow the series without
end.

## how to build

    game check          # gofmt, vet, the tests
    game build          # flipr and fliprctl into bin/, stamped

`bin/install.sh` links `fliprctl` into `~/.local/bin`. it is `flipr ctl`
from the same binary the pod runs.

## how to deploy and bounce

    bin/deploy.sh <environment>    # the one way code reaches a cluster
    flipr restart                  # same image, new pod
    flipr stop --yes; flipr start

the environment is a file under `kube/environments/` naming the cluster,
the context, the edge port and the data root; `local.env` is the example
to copy.

the script states its target and refuses if the context disagrees with the
cluster, if the data root has no flipr directory, or if the tree is dirty.

then it builds the image with the commit hash as tag and version, runs vet
and the suite under the race detector, imports the image and substitutes
`${COMMIT}`, `${DATA_ROOT}` and `${ENV}` into the manifests.

it applies them, waits for the rollout, pushes the dashboard, and reads
`/health` back by name, failing unless it reports the hash.

the manifests carry placeholders and never a literal tag or path, because
a literal there means the next deploy applies the wrong thing.

every restart and deploy is an outage: a few seconds at the pod, about
fifteen as consumers see it, because bbolt holds an exclusive lock and the
strategy is Recreate. the pod waits for kafka first, up to 300 seconds, so
`Init:0/1` after a cluster boot is waiting rather than broken.

## the store, the oplog, and what survives what

the flags are one bbolt file at `$DATA_ROOT/flipr`, through a static
PersistentVolume and the claim `flipr-state`, mounted at `/state`. the k3d
node bind-mounts `$DATA_ROOT`, so the file outlives the node and a cluster
delete does not touch it.

every flip is written to `/state/flipr.oplog` beside the store, one record
per line, fsynced before its caller is answered; reads are counted in the
metrics and never recorded. the file is the audit and the replay source,
and the kafka topic mirrors it while the bus exists.

the hourly CronJob `flipr-export` writes every namespace and flag to
`$DATA_ROOT/backups/flipr/flags-<stamp>Z.json`, checks the counts against
`/metrics`, and deletes the file and fails if they disagree, so a dump
taken during an outage never looks like a backup.

files older than 30 days are pruned, and nothing restores from one: a dump
is for a person to read.

    kubectl -n flipr get cronjob flipr-export
    kubectl -n flipr create job flipr-export-now --from=cronjob/flipr-export

## the revision

`flipr health` shows `revision`, the count of writes committed to this
store. every flip, publish and delete moves it by one and reads never do,
and every oplog record carries the number it created.

the count belongs to the record and not to one generation, so a flipr that
heals from the oplog continues from the highest number in the file, and a
restore takes the next number above everything it undid.

two readings at 03:20: a revision moving while nobody is flipping is a
service republishing on every loop, and the highest revision in the oplog
is the store's, or the store is missing records, which `fliprctl assess`
says outright.

## fliprctl

the operator's tool over the files on the host: `status`, `log -tail N`,
`assess`, `lock`, `unlock`, `restore --to-revision N`, and `clients`.

the offline verbs refuse while the pod holds the store. `lock` is
meat-only and refuses on an unhappy flipr unless told the person knows;
the lock is a file beside the store that flipr honours within a second,
never an rpc. RUNBOOK-restore.md is the page for a restore.

## what to watch

| series | means |
|---|---|
| `flipr_oplog_sink_errors_total` moving | the sink is refusing writes; flips are failing |
| `flipr_store_rollbacks_total` moving | flips were rolled back because they could not be logged |
| `flipr_oplog_mirror_errors_total` moving | the kafka mirror is refusing; the file holds the record |
| `flipr_oplog_file_bytes` | the oplog's size; it only grows, by design |
| `flipr_rpc_rejected_total` by reason | a client is sending what flipr refuses |
| `flipr_rpc_requests_total{outcome="server_error"}` | 500s, always worth the log |
| `flipr_rpc_duration_seconds` p99 on `GetFlag` over 500µs | flipr has left the range it lives in |
| `flipr_startup_duration_microseconds` | how long the last start took |

every series exists from the first scrape, at zero, so an alert on a rate
has something to fire on. the dashboard `flipr-<environment>` is pushed by
the deploy script.

## logs

structured json on stdout, collected by the cluster's pipeline. `flipr
logs` follows the pod.

two streams, and they are not the same: the oplog is what flipr did, bound
for kafka; this log is how flipr is, with refusals, reloads, sink trouble
and store births. set `log.level` to `debug` in flipr's namespace or the
global scope to open the debug gate with no restart.

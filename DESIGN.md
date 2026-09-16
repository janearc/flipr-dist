# flipr

the flag store. every service asks flipr before it does anything
expensive, and the answer is usually a boolean. if flipr is down, the
network is down: a choice, and everything below follows from it.

    ask       GetNamespace(service@version, since) -> flags, revision
    publish   a deploy declares which flags exist; it never sets values
    flip      an operator sets one, with a reason, and it is recorded
    page      the emergency page is the flags marked expensive

a flag is a bool, a string or an integer, and a namespace is one service
at one version, pinned to the deployment.

reads come from an immutable snapshot and never touch the disk; writes
are durable to bbolt and then publish a fresh snapshot. every write
bumps a revision, and a revision names a restore point.

clients cache the last value and check flipr anyway: the cache is a
performance optimisation, never a fallback, because a service running on
a flag somebody turned off is worse than an outage.

    go build -o flipr . && ./flipr -addr :15100 -db flipr.db

that is the service. the rest is why; the readme is how to run it.

## the invariant

if flipr is down, the network is down. that is a choice, and every other
decision here follows from it.

## the shape

    main.go        flags, the mux, the listener
    server.go      one route per proto method, named for it
    store.go       bbolt, plus the snapshot reads are served from
    oplog.go       the write log on disk, fsynced, replayable
    kafka.go       a best-effort mirror of the oplog, being retired
    validate.go    the boundary: unknown fields are refused here
    metrics.go     every rpc counted and timed at the router
    ctl.go         the offline verbs: status, log, assess, lock, restore
    proto/         the source of truth; gen/ is generated from it
    clients/       go, python and typescript, each at its own tag

reads never touch bbolt. they read an immutable snapshot held in an
`atomic.Value`: a pointer load and a map lookup. writes go the slow way,
durable to bbolt and then a fresh snapshot published.

## the decisions

**bbolt, never on the read path.** a pointer load beats a disk every
time, and writes are rare because somebody has to decide to flip
something. it leaves us three orders of magnitude below the rate the
requirement asks for.

**not rocksdb.** cgo, a c++ toolchain and a shared library in the image,
for a store that is not on the read path.

**the snapshot is rebuilt after each write, not patched.** it is
o(flags), flags are few, and a rebuilt snapshot cannot disagree with the
disk.

**clients cache, and check anyway.** the cache is a performance
optimisation, never a fallback: a service running for hours on a flag
somebody turned off is worse than an outage, because nothing reports it.

**publishing never clobbers a value.** a deploy declares which flags
exist; an operator decides what they are set to. otherwise a deploy
silently undoes the emergency flip it is happening alongside.

**a flip requires a stated reason,** recorded and shipped. this estate
lost an expensive incident to having no logs; a required string is the
cheapest insurance.

**one namespace per deployment, its version pinned.** ruled after the
store filled with dead per-deploy rows and, worse, every ship
resurrected flags an operator had killed.

a new namespace is born with declared defaults and has no operator value
to protect, and a kill that does not survive a deploy is an unauthorised
enable.

**one flipr per environment.** the same commit in both environments
lands in the same namespace, so a shared instance would let dev flip
prod. putting the environment in the key puts it in every client, which
makes dev-ness a code path.

**protojson over plain http, unknown fields refused.** the generated
types are lenient, so strictness lives at the boundary. no grpc: http/2
and client stubs for an answer that fits in a cache line.

**`/api` serves a descriptor set,** built by buf from the same source
the go types came from, so the service says what its api is and cannot
drift from the code. not openapi, which describes a rest projection.

**more instrumentation than the size warrants.** everything asks flipr
before it acts, so if flipr is slow everything is slow. buckets are
dense between ten microseconds and a millisecond, because the question
is whether it has left the microsecond range.

## the schema

a namespace is one service at one version: `albatross@a1b2c3d` and
`albatross@deadbee` are separate namespaces under `albatross`. that is
what lets a deploy publish its own flags without touching the ones the
running version serves.

a flag is a key, a value, a description, and whether it is expensive.
keys are dotted and the dots are the hierarchy: `nightly.smoothing`.

values are a bool, a string or an integer. the boolean is the point; the
string exists because smoothing is `surreal` or `claude`, and two
booleans would let both be true. anything wanting richer structure is
asking the wrong question.

`expensive` has one job: the emergency page is the set of flags where it
is true, as a projection rather than a second list, so the two cannot
drift.

## the revision

every write bumps a counter inside its own transaction, kept in the
store's meta bucket, carried in every answer and in `/health`, and
stamped into every oplog record.

`GetNamespace` takes `since`: when it equals the current revision the
answer is `unchanged` and one integer, compared before the store is
read, so a consumer polling on its interval costs nothing.

a read never moves it and a rolled-back write keeps its number, so a
number names one attempt forever. that is what makes `fliprctl restore
--to-revision N` possible (RUNBOOK-restore.md).

the oplog records writes only, so the world can be rebuilt when the
store is gone. reads are counted in the metrics instead: recording them
grew the file six megabytes in one quiet night of polling.

## failure domains

| if this fails | then | because |
|---|---|---|
| flipr | the network stops | by design |
| the bbolt file | flipr refuses to start | no flags reads as "off" for everything |
| the oplog sink | writes fail and roll back; reads are unaffected | a flip nobody recorded is the event this service exists to prevent |
| kafka | log shipping lags | kafka is below flipr in the boot order, so this is startup, not runtime |

boot order, per environment, not negotiable: cold, kafka, flipr, then
the services. the store and the oplog live on a static
PersistentVolume under `$DATA_ROOT/flipr`, which outlives the node.

## what we are not in the business of

- experiments, rollouts, targeting or segmentation
- authentication here: the flag page reuses dodo's admin auth
- a config file: six flags and six environment variables, in USING.md
- client-side fallback when flipr is unreachable, per the invariant
- retention: retiring a row is an operator action inside a deploy, and
  nothing sweeps in the background

## still open

- authentication on the write api: anyone who can reach flipr can flip
- replay applies each flip as its own commit, so a log of tens of
  thousands would replay in minutes rather than seconds

## testing

unit tests cover the store, the oplog and the metrics registry.
integration tests drive the real mux against a real bbolt file with a
real oplog, no stubs in the path, and fault-injection tests cover the
branches that only run when something is broken.

coverage is above 85% of statements, measured by `go test -cover`. the
rest is `main` and `fatal`, which call `os.Exit`, and error branches
that need bbolt's internals faked. the number is a floor, not a target.

the check that matters is in the task manifest: start from cold with
nothing else running, and survive `kill -9` with state intact. both are
verified.

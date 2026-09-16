# runbook: flipr

three conditions, then how to build it and how to bounce it. every
command below is `fliprctl` on the host, which is `flipr ctl` from the
same binary the pod runs, with `FLIPR_DB` pointing at
`$DATA_ROOT/flipr/flipr.db`.

## condition: a write you need to undo

verify the state:

    fliprctl status        # store revision and generation, oplog's last
    fliprctl log -tail 50  # the last writes: revision, time, caller, reason

pick the revision before the first write you want gone, the last one to
keep, and write it down. the hourly export in `$DATA_ROOT/backups/flipr/`
is a readable copy of every value if you end up needing one.

what you can do, in this order:

    fliprctl lock --all --reason "restore to 4127: <what happened>"
    flipr stop --yes                  # the store must have no writer
    fliprctl assess                   # both files parse, and agree
    fliprctl restore --to-revision 4127 --reason "<what happened>"
    fliprctl assess                   # one restore standing
    flipr start
    flipr get <service> <version> <key>   # is it what 4127 said?
    fliprctl unlock --all --reason "restore to 4127 verified"

the implications:

- while it is locked, flipr answers degraded with your reason, clients
  hold their last values at a thirty-second cadence, and nothing in the
  estate goes down. you are the only thing moving.
- the live store is moved aside as `flipr.db.before-restore-<stamp>-rN`
  and kept for forensics. the way back is another restore, to the number
  the first one left, which records that it undid the first.
- nothing leaves the oplog. the undone records stay, and every later
  replay honours the restore record and skips them, so a flipr healed
  from this file comes back as you left it.
- the fresh store wears a new generation on purpose: every client sees
  the change, republishes its declarations once, and so fills the
  descriptions back in. a republish never overwrites a restored value.
- expect one warn line per service, "store generation changed;
  republishing declared defaults", within thirty seconds of the unlock.

## condition: the store or the oplog does not parse

verify:

    fliprctl assess    # refuses while the pod holds the file

what you can do: if the pod is still up, stop it and assess again. if the
oplog is what will not parse, stop here: the tool names the line, and a
restore over a log you cannot read is a guess. read the hourly export
from before the bad write and flip the values by hand, with reasons.

the implications: a store the tool cannot open is a wipe and a replay,
which costs the operator values that the oplog holds and nothing else.

## condition: a restore that did not land

verify:

    fliprctl assess
    flipr get <service> <version> <key>

what you can do: unlock is not the next step. stop the pod and restore to
the revision the first restore undid up to, which is the number in the
kept store's name. that puts you where you began with both restores in
the file.

if the tool failed before it appended its record, the oplog is untouched
and the moved-aside store is where it put it: `mv
flipr.db.before-restore-... flipr.db`, assess, start, unlock.

the implications: moving a kept store back by hand while a restore record
stands in the file leaves the two disagreeing, and the next replay will
apply the record. only do it when the file itself is the problem.

## how to build

    game check          # gofmt, vet, the tests
    game build          # flipr and fliprctl into bin/, stamped
    ln -sf $PWD/bin/fliprctl ~/.local/bin/fliprctl

`go build -o flipr .` is the same binary without the stamp. the clients
are their own modules under `clients/`, built and tagged separately
(clients/CONTRACT.md, section 13).

## how to deploy and bounce

    bin/deploy.sh <environment>    # a commit into a cluster
    flipr stop --yes               # ring 0: it asks because you mean it
    flipr start
    flipr health                   # healthy, degraded with reasons, or down

every deploy is an outage window by design, so a bounce is announced the
way an outage is. the boot order is not negotiable: cold, kafka, flipr,
then the services.

## what this page assumes

`fliprctl`'s offline verbs refuse while the pod holds the store, and
`lock` refuses on an unhappy flipr without `--i-know-flipr-is-unhappy`.

every client in the estate is a published client that understands a lock.
a hand-written one sees a lock as an outage, so check the audit first.

rehearse this page on a throwaway cluster with a copy of the live files
before relying on it.

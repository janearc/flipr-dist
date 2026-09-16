package main

// flipr ctl: the operator's tool over the files, for the person at the
// keyboard (RUNBOOK-restore.md). One binary with the server so the store
// code is not split into a package; installed on the host as `fliprctl`
// (bin/install.sh) and present in the image.
//
//   flipr ctl [-db PATH] [-oplog PATH] [-url URL] <verb> [args]
//
//   status                       generation, revision, namespaces, the oplog's
//                                last revision, the lock; from the server when
//                                it answers, else from the files
//   log [-tail N]                the last N writes with revision, time,
//                                caller and reason (default 50)
//   assess                       the store opens and every namespace parses;
//                                the oplog parses, and its highest revision
//                                is the store's; refuses while the server
//                                holds the file
//   lock --all | --namespace SVC@VER --reason "..."
//                                writes the lock file; refused while flipr's
//                                health is not healthy unless
//                                --i-know-flipr-is-unhappy is spelled out
//   unlock --all | --namespace SVC@VER --reason "..."
//   get|set|flags|export         the operator's reads and flips through the
//                                running server, announcing themselves as
//                                fliprctl; see the block at the end
//   clients [add|remove|refuse-other] the known client set, flipr@clients,
//                                through the running server (no bounce) or
//                                the files when it is down; see below
//   restore --to-revision N --reason "..."
//                                moves the store aside (named by its
//                                revision), opens a fresh one, replays the
//                                oplog to N, appends a Restore record
//                                numbered above everything in the file and
//                                carrying the range it undid, which every
//                                later replay honours (the way back is a
//                                second restore, to the number the first
//                                left); the server must be down; the old
//                                store is kept for forensics.
//                                The fresh store wears a NEW generation on
//                                purpose: the record holds operator flips
//                                and not declarations, so every client
//                                republishes its declarations once on its
//                                next check (the self-heal; publish never
//                                overwrites a restored value) and the store
//                                is whole again within one client interval
//
// Every offline verb takes bbolt's exclusive lock and so refuses while the
// server runs, which is the guard the runbook relies on. Nothing here is a
// client verb: a service never runs this, and flipr never runs it on its own
// judgement (the meat-only rule).

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pb "github.com/janearc/flipr-dist/gen/fliprpb"
	oplogpb "github.com/janearc/flipr-dist/gen/oplogpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ctlMain is the entry point when os.Args[1] == "ctl". It exits the process.
func ctlMain(args []string) {
	fs := flag.NewFlagSet("flipr ctl", flag.ExitOnError)
	dbPath := fs.String(
		"db",
		envOr("FLIPR_DB", "flipr.db"),
		"path to the bbolt file",
	)
	oplogPath := fs.String(
		"oplog",
		envOr("FLIPR_OPLOG_FILE", "-"),
		"the oplog file (\"-\": <db>.oplog beside the store)",
	)
	url := fs.String(
		"url",
		envOr("FLIPR_URL", "http://flipr.test"),
		"the running flipr, for status and the lock's health check",
	)
	fs.Usage = func() {
		fmt.Fprintln(
			os.Stderr,
			"usage: flipr ctl [-db PATH] [-oplog PATH] [-url "+
				"URL] "+
				"status|log|assess|lock|unlock|restore|"+
				"clients|get|set|flags|export "+
				"...",
		)
		fs.PrintDefaults()
	}
	_ = fs.Parse(args)
	if *oplogPath == "-" {
		*oplogPath = *dbPath + ".oplog"
	}
	rest := fs.Args()
	if len(rest) == 0 {
		fs.Usage()
		os.Exit(2)
	}
	c := &ctl{
		db:    *dbPath,
		oplog: *oplogPath,
		lock:  lockPath(*dbPath),
		url:   strings.TrimRight(*url, "/"),
		out:   os.Stdout,
	}
	var err error
	switch rest[0] {
	case "status":
		err = c.status()
	case "log":
		err = c.tail(rest[1:])
	case "assess":
		err = c.assess()
	case "lock":
		err = c.setLock(rest[1:], true)
	case "unlock":
		err = c.setLock(rest[1:], false)
	case "restore":
		err = c.restore(rest[1:])
	case "clients":
		err = c.clients(rest[1:])
	case "get":
		err = c.get(rest[1:])
	case "set":
		err = c.set(rest[1:])
	case "flags":
		err = c.flags(rest[1:])
	case "export":
		err = c.export()
	default:
		fs.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "flipr ctl "+rest[0]+": "+err.Error())
		os.Exit(1)
	}
}

type ctl struct {
	db, oplog, lock, url string
	out                  io.Writer
}

// openOffline opens the store with bbolt's exclusive lock: refused, with a
// sentence, while the server holds the file.
func (c *ctl) openOffline() (*Store, error) {
	if _, err := os.Stat(c.db); err != nil {
		return nil, fmt.Errorf("no store at %s", c.db)
	}
	s, err := OpenStore(c.db, NewRegistry(), NewLogger())
	if err != nil {
		if strings.Contains(err.Error(), "timeout") {
			return nil, fmt.Errorf(
				"the store is held by a running flipr (%s); "+
					"stop the pod first (`flipr stop "+
					"--yes`)",
				c.db,
			)
		}
		return nil, err
	}
	return s, nil
}

// status: the server's account when it answers, else the files'.
func (c *ctl) status() error {
	l, lerr := readLock(c.lock)
	if h, err := c.health(); err == nil {
		fmt.Fprintf(
			c.out,
			"flipr %s at %s: %s\n",
			h["version"],
			c.url,
			h["status"],
		)
		rev := "none (this flipr predates the revision, 2026-09-06)"
		if r, ok := h["revision"]; ok && r != nil {
			rev = fmt.Sprint(r)
		}
		fmt.Fprintf(
			c.out,
			"  generation %v  revision %s  uptime %vs\n",
			h["generation"],
			rev,
			h["uptime_seconds"],
		)
		if rs, _ := h["reasons"].([]any); len(rs) > 0 {
			for _, r := range rs {
				fmt.Fprintf(c.out, "  reason: %v\n", r)
			}
		}
	} else {
		fmt.Fprintf(c.out, "flipr at %s: not answering (%v); reading "+
			"the files\n", c.url, err)
		s, err := c.openOffline()
		if err != nil {
			return err
		}
		defer s.Close()
		fmt.Fprintf(
			c.out,
			"  store %s: generation %s  revision %d  "+
				"namespaces %d\n",
			c.db,
			s.Generation(),
			s.Revision(),
			len(s.ListNamespaces()),
		)
	}
	last, n, err := c.oplogTail(1)
	if err != nil {
		fmt.Fprintf(c.out, "  oplog %s: %v\n", c.oplog, err)
	} else if n == 0 {
		fmt.Fprintf(c.out, "  oplog %s: empty\n", c.oplog)
	} else {
		r := last[0]
		fmt.Fprintf(
			c.out,
			"  oplog %s: %d records, last revision %d "+
				"(%s %s %s@%s at %s)\n",
			c.oplog,
			n,
			r.Revision,
			r.Op,
			r.Key,
			r.Service,
			r.Version,
			r.Ts,
		)
	}
	switch {
	case lerr != nil:
		fmt.Fprintf(
			c.out,
			"  LOCK FILE UNREADABLE at %s: %v (the store is "+
				"locked until a person fixes it)\n",
			c.lock,
			lerr,
		)
	case l != nil:
		fmt.Fprintf(
			c.out,
			"  LOCKED (%s) by %s at %s: %s\n",
			strings.Join(l.Scopes, ","),
			l.By,
			l.At.Format(time.RFC3339),
			l.Reason,
		)
	default:
		fmt.Fprintf(c.out, "  no lock\n")
	}
	return nil
}

// health GETs the running flipr's health, quickly.
func (c *ctl) health() (map[string]any, error) {
	hc := &http.Client{Timeout: 3 * time.Second}
	resp, err := hc.Get(c.url + "/health")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, fmt.Errorf(
			"%s answered %d without flipr's health body",
			c.url,
			resp.StatusCode,
		)
	}
	return m, nil
}

// tail prints the last N writes.
func (c *ctl) tail(args []string) error {
	fs := flag.NewFlagSet("flipr ctl log", flag.ExitOnError)
	n := fs.Int("tail", 50, "how many records")
	_ = fs.Parse(args)
	recs, total, err := c.oplogTail(*n)
	if err != nil {
		return err
	}
	fmt.Fprintf(
		c.out,
		"%s: %d records; the last %d:\n",
		c.oplog,
		total,
		len(recs),
	)
	for i := len(recs) - 1; i >= 0; i-- {
		r := recs[i]
		ns := r.Service
		if r.Version != "" {
			ns += "@" + r.Version
		}
		fmt.Fprintf(
			c.out,
			"  r%-6d %s  %-16s %s %s %s  by %s  %s\n",
			r.Revision,
			r.Ts,
			r.Op,
			ns,
			r.Key,
			r.Value,
			r.Caller,
			r.Reason,
		)
	}
	return nil
}

// oplogTail reads the whole file (a line parse, no lock) and returns the
// last n records, newest first, and the total count of parseable records.
func (c *ctl) oplogTail(n int) ([]*oplogpb.Operation, int, error) {
	f, err := os.Open(c.oplog)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	var ring []*oplogpb.Operation
	total := 0
	r := bufio.NewReaderSize(f, 1<<20)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) == 0 && err == io.EOF {
			break
		}
		if err != nil && err != io.EOF {
			return nil, total, err
		}
		if err == io.EOF {
			break // a partial last line is not a record
		}
		var op oplogpb.Operation
		// game: allow width
		if perr := protojson.Unmarshal(line[:len(line)-1], &op); perr != nil {
			continue
		}
		total++
		ring = append(ring, &op)
		if len(ring) > n {
			ring = ring[1:]
		}
	}
	out := make([]*oplogpb.Operation, 0, len(ring))
	for i := len(ring) - 1; i >= 0; i-- {
		out = append(out, ring[i])
	}
	return out, total, nil
}

// assess: the store and the oplog parse, and they agree about the revision:
// the store's number is the highest stamp in the file. Not the last record's
// stamp, because a lock record carries the revision it saw and can land
// after a write that moved it.
func (c *ctl) assess() error {
	s, err := c.openOffline()
	if err != nil {
		return err
	}
	defer s.Close()
	nss := s.ListNamespaces()
	flags := 0
	for _, ns := range nss {
		flags += len(ns.GetFlags())
	}
	fmt.Fprintf(
		c.out,
		"store %s: opens; generation %s; revision %d; %d namespaces, "+
			"%d flags parse\n",
		c.db,
		s.Generation(),
		s.Revision(),
		len(nss),
		flags,
	)
	scan, err := scanOplog(context.Background(), c.oplog)
	if err != nil {
		return fmt.Errorf("oplog %s: %w", c.oplog, err)
	}
	if scan.Bad > 0 {
		return fmt.Errorf(
			"oplog %s: %d of %d lines do not parse; a replay "+
				"would refuse this file",
			c.oplog,
			scan.Bad,
			scan.Bad+scan.Records,
		)
	}
	if scan.Records == 0 {
		// no file, or an empty one: a fresh store has nothing recorded
		// yet, and a store that has written must have a record of it
		fmt.Fprintf(c.out, "oplog %s: none or empty\n", c.oplog)
		if s.Revision() != 0 {
			return fmt.Errorf(
				"the store is at revision %d and the oplog "+
					"is absent or empty: records are "+
					"missing",
				s.Revision(),
			)
		}
		fmt.Fprintln(c.out, "ASSESSED: consistent")
		return nil
	}
	fmt.Fprintf(
		c.out,
		"oplog %s: %d records parse; highest revision %d; %d "+
			"restores, %d standing\n",
		c.oplog,
		scan.Records,
		scan.Highest,
		len(scan.Restores),
		len(standing(scan.Restores, 0)),
	)
	if scan.Highest != s.Revision() {
		return fmt.Errorf(
			"the store is at revision %d and the oplog's highest "+
				"record is %d: records are missing, or the "+
				"store is not the one this file records",
			s.Revision(),
			scan.Highest,
		)
	}
	fmt.Fprintln(c.out, "ASSESSED: consistent")
	return nil
}

// setLock writes or removes the lock file. Meat-only: this is the only
// writer, and it refuses a lock on an unhappy flipr unless told the person
// knows (the anti-pattern the runbook describes).
func (c *ctl) setLock(args []string, on bool) error {
	name := "lock"
	if !on {
		name = "unlock"
	}
	fs := flag.NewFlagSet("flipr ctl "+name, flag.ExitOnError)
	all := fs.Bool("all", false, "the whole store")
	ns := fs.String("namespace", "", "one namespace, service@version")
	reason := fs.String("reason", "", "why, recorded (required)")
	unhappy := fs.Bool(
		"i-know-flipr-is-unhappy",
		false,
		"lock anyway while flipr's health is not healthy (read "+
			"RUNBOOK-restore.md first)",
	)
	_ = fs.Parse(args)
	if *reason == "" {
		return fmt.Errorf("--reason is required, and it is recorded")
	}
	if *all == (*ns != "") {
		return fmt.Errorf(
			"say --all or --namespace SVC@VER, one of them",
		)
	}
	if *ns != "" && !strings.Contains(*ns, "@") {
		return fmt.Errorf("a namespace is service@version")
	}
	if !on {
		if err := os.Remove(c.lock); err != nil && !os.IsNotExist(err) {
			return err
		}
		fmt.Fprintf(c.out, "unlocked (%s): %s\n", c.lock, *reason)
		return nil
	}
	if h, err := c.health(); err == nil && h["status"] != "healthy" &&
		!*unhappy {
		return fmt.Errorf(
			"flipr is %v, not healthy, and a lock is not a way "+
				"to bring up an unhappy flipr so the enclave "+
				"can come up around it. Fix what is wrong in "+
				"flipr, then bring up the cluster. If you "+
				"have read RUNBOOK-restore.md and this is "+
				"the case it describes, say "+
				"--i-know-flipr-is-unhappy",
			h["status"],
		)
	}
	scopes := []string{"*"}
	if *ns != "" {
		scopes = []string{*ns}
	}
	l, err := writeLock(c.lock, scopes, *reason)
	if err != nil {
		return err
	}
	fmt.Fprintf(
		c.out,
		"locked (%s) by %s at %s: %s\n",
		strings.Join(l.Scopes, ","),
		l.By,
		l.At.Format(time.RFC3339),
		l.Reason,
	)
	fmt.Fprintln(
		c.out,
		"flipr honours it within a second; clients hold their last "+
			"values and back off; /health says degraded",
	)
	return nil
}

// restore rebuilds the store as it stood at a revision, from the record,
// onto a fresh store; the old one is kept beside it, named by its revision.
//
// The restore is itself a record: one Restore, numbered above everything in the
// file, carrying the range it undid, which replay honours from then on
// (filelog.go).
//
// So the file is never edited, a wipe heal after a restore comes back restored,
// and the way back is another restore, to the number the first one left. The
// fresh store's generation is new, so clients republish their declarations
// (values are never clobbered).
func (c *ctl) restore(args []string) error {
	fs := flag.NewFlagSet("flipr ctl restore", flag.ExitOnError)
	to := fs.Uint64(
		"to-revision",
		0,
		"the revision to restore to (the last one to KEEP)",
	)
	reason := fs.String("reason", "", "why, recorded (required)")
	_ = fs.Parse(args)
	if *to == 0 || *reason == "" {
		return fmt.Errorf("--to-revision N and --reason are required")
	}
	old, err := c.openOffline()
	if err != nil {
		return err
	}
	was := old.Revision()
	old.Close()
	if *to >= was {
		return fmt.Errorf(
			"the store is at revision %d; a restore names a "+
				"revision below it",
			was,
		)
	}
	scan, err := scanOplog(context.Background(), c.oplog)
	if err != nil {
		return fmt.Errorf("the oplog at %s: %w", c.oplog, err)
	}
	if scan.Records == 0 {
		return fmt.Errorf(
			"the oplog at %s is absent or empty; nothing to "+
				"restore from",
			c.oplog,
		)
	}
	if scan.Bad > 0 {
		return fmt.Errorf(
			"the oplog at %s has %d lines that do not parse; a "+
				"restore would refuse it (fix the file "+
				"first, with the pod stopped)",
			c.oplog,
			scan.Bad,
		)
	}
	if scan.Highest < *to {
		return fmt.Errorf(
			"the oplog's highest record is revision %d; %d is "+
				"not in it",
			scan.Highest,
			*to,
		)
	}
	if scan.Highest > was {
		return fmt.Errorf(
			"the oplog reaches revision %d and the store is at "+
				"%d: the store is not the one this file "+
				"records; assess first",
			scan.Highest,
			was,
		)
	}
	if scan.Highest < was {
		// the store ran ahead of the file: a write that committed and
		// could not be recorded is rolled back and keeps its number
		// (store.go, transact), so a gap of one per rollback is the
		// store being honest, and a larger gap is a file missing lines.
		//
		// Either way the restore is well-defined from what the file
		// holds; the person sees the gap and decides
		fmt.Fprintf(
			c.out,
			"note: the store is at revision %d and the file's "+
				"highest record is %d; the %d numbers "+
				"between were rolled-back writes (no record "+
				"by design) or lost lines. The restore uses "+
				"the file.\n",
			was,
			scan.Highest,
			was-scan.Highest,
		)
	}
	aside := fmt.Sprintf(
		"%s.before-restore-%s-r%d",
		c.db,
		time.Now().UTC().Format("20060102T150405Z"),
		was,
	)
	if err := os.Rename(c.db, aside); err != nil {
		return err
	}
	fmt.Fprintf(
		c.out,
		"moved %s -> %s\n",
		filepath.Base(c.db),
		filepath.Base(aside),
	)
	fresh, err := OpenStore(c.db, NewRegistry(), NewLogger())
	if err != nil {
		return fmt.Errorf(
			"opening a fresh store: %w (the old one is at %s)",
			err,
			aside,
		)
	}
	r, err := ReplayFileTo(
		context.Background(),
		c.oplog,
		*to,
		NewLogger(),
		fresh.ApplyReplayed,
		func(sv, v string) error {
			_, err := fresh.DeleteNamespace(sv, v)
			return err
		},
	)
	if err != nil {
		fresh.Close()
		return fmt.Errorf(
			"replay to %d failed: %w (the old store is at %s; "+
				"move it back)",
			*to,
			err,
			aside,
		)
	}
	// the restore is a write in the record, numbered above everything the
	// file holds, and the range it undid is (to, next-1]: every record
	// above the named point up to and including the number the store stood
	// at
	next := was + 1
	if r.Highest >= next {
		next = r.Highest + 1
	}
	from := next - 1
	if err := fresh.SetRevision(next); err != nil {
		fresh.Close()
		return fmt.Errorf(
			"setting the revision: %w (the old store is at %s)",
			err,
			aside,
		)
	}
	gen := fresh.Generation()
	fresh.Close()
	sink, err := NewFileSink(c.oplog, gen, NewRegistry())
	if err != nil {
		return fmt.Errorf(
			"appending the restore record: %w (the store is "+
				"rebuilt at %d and the record is not; assess "+
				"will say so)",
			err,
			next,
		)
	}
	by := "flipr ctl"
	if l, _ := readLock(c.lock); l != nil {
		by = "flipr ctl:" + l.By
	}
	if err := sink.Write(map[string]any{
		"ts":       time.Now().UTC().Format(time.RFC3339Nano),
		"op":       "Restore",
		"service":  "*",
		"value":    fmt.Sprintf("to %d from %d", *to, from),
		"reason":   *reason,
		"caller":   by,
		"revision": next, "restore_to": *to, "restore_from": from,
	}); err != nil {
		return err
	}
	sink.Close()
	fmt.Fprintf(
		c.out,
		"replayed %d flips to revision %d; the restore is revision "+
			"%d (undoing %d..%d); new generation %s, so every "+
			"client republishes its declarations once\n",
		r.Applied,
		*to,
		next,
		*to+1,
		from,
		gen,
	)
	fmt.Fprintf(
		c.out,
		"the previous store is kept at %s for forensics; the way "+
			"back is `flipr ctl restore --to-revision %d` "+
			"(RUNBOOK-restore.md)\n",
		aside,
		from,
	)
	return nil
}

// The clients verb. `flipr ctl clients` reads and writes flipr@clients, the
// known set (clients.go), through the running server's RPC path with the
// tool's own identity, so a client roll is one command and never a bounce;
//
// when the server is not answering and the store is free, the same write
// goes to the files with the record appended, so the set can be prepared
// before flipr comes up.
//
//   clients                              the set, and whether other is refused
//   clients add LANG/TAG/HASH --reason   one entry (stamp.sh prints the triple)
//   clients remove LANG/TAG --reason     retire one (valued "retired ...")
//   clients refuse-other on|off --reason turn refusal on or off

// clients dispatches the subverb.
func (c *ctl) clients(args []string) error {
	if len(args) == 0 {
		return c.listClients()
	}
	// the shape is `clients VERB WHAT --reason "..."`: the subject first,
	// then the flags, because the flag package stops at the first word that
	// is not a flag
	verb, what := args[0], ""
	rest := args[1:]
	if len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
		what, rest = rest[0], rest[1:]
	}
	fs := flag.NewFlagSet("flipr ctl clients "+verb, flag.ExitOnError)
	reason := fs.String("reason", "", "why, recorded (required)")
	replace := fs.Bool(
		"replace",
		false,
		"add: overwrite an entry that already holds this lang/tag",
	)
	_ = fs.Parse(rest)
	switch verb {
	case "add":
		lang, tag, _, ok := splitClient(what)
		if !ok || *reason == "" {
			return fmt.Errorf(
				"usage: clients add LANG/TAG/HASH --reason " +
					"\"...\" (the triple is what " +
					"clients/bin/stamp.sh prints)",
			)
		}
		// one entry per lang/tag: two would resolve by whichever the
		// map walk met last, so a second is refused unless the person
		// says replace (a tag was moved before anything pinned it, say)
		if have, ok := c.knownEntry(lang, tag); ok && !*replace {
			return fmt.Errorf(
				"%s/%s is already known as %s; --replace to "+
					"overwrite it, or clients remove "+
					"first",
				lang,
				tag,
				have,
			)
		}
		return c.writeClientFlag(
			knownKey(lang, tag),
			&pb.Value{
				Kind: &pb.Value_StringValue{StringValue: what},
			},
			*reason,
		)
	case "remove":
		parts := strings.Split(what, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" ||
			*reason == "" {
			return fmt.Errorf(
				"usage: clients remove LANG/TAG --reason " +
					"\"...\"",
			)
		}
		// retired by value: there is no DeleteFlag on the wire, and a
		// value that is not a triple is not known (clients.go,
		// splitClient)
		return c.writeClientFlag(
			knownKey(parts[0], parts[1]),
			&pb.Value{
				Kind: &pb.Value_StringValue{
					StringValue: "retired " + time.Now().
						UTC().
						Format(time.RFC3339),
				},
			},
			*reason,
		)
	case "refuse-other":
		var on bool
		switch what {
		case "on":
			on = true
		case "off":
		default:
			return fmt.Errorf(
				"usage: clients refuse-other on|off --reason " +
					"\"...\"",
			)
		}
		if *reason == "" {
			return fmt.Errorf("--reason is required")
		}
		return c.writeClientFlag(
			keyRefuseOther,
			&pb.Value{Kind: &pb.Value_BoolValue{BoolValue: on}},
			*reason,
		)
	default:
		return fmt.Errorf(
			"usage: clients [add LANG/TAG/HASH | remove LANG/TAG " +
				"| refuse-other on|off] --reason \"...\"",
		)
	}
}

// knownEntry reports the triple flipr@clients holds for a lang/tag, from the
// server when it answers, else from the store; a retired entry is not held.
func (c *ctl) knownEntry(lang, tag string) (string, bool) {
	ns := c.clientsNamespace()
	for _, f := range ns.GetFlags() {
		if f.GetKey() == knownKey(lang, tag) {
			// game: allow width
			if _, _, _, ok := splitClient(f.GetValue().GetStringValue()); ok {
				return f.GetValue().GetStringValue(), true
			}
		}
	}
	return "", false
}

// clientsNamespace reads flipr@clients from the server or, failing that,
// the store; nil when neither has it.
func (c *ctl) clientsNamespace() *pb.Namespace {
	if body, err := c.rpc("GetNamespace", &pb.GetNamespaceRequest{
		Service: clientsService,
		Version: clientsVersion,
	}); err == nil {
		var resp pb.GetNamespaceResponse
		if protojson.Unmarshal(body, &resp) == nil {
			return resp.Namespace
		}
	}
	s, err := c.openOffline()
	if err != nil {
		return nil
	}
	defer s.Close()
	ns, _ := s.GetNamespace(clientsService, clientsVersion)
	return ns
}

// knownKey is the flag handle for one client: known.<lang>.<tag>, with the
// tag's dots made hyphens so the key stays one segment per part and the
// front end does not draw v1.1.0 as a three-level tree. The value is the
// truth; the key is where it hangs.
func knownKey(lang, tag string) string {
	return knownPrefix + strings.ToLower(
		lang,
	) + "." + strings.ToLower(
		strings.ReplaceAll(tag, ".", "-"),
	)
}

// listClients prints the set: from the server when it answers, else from the
// store offline.
func (c *ctl) listClients() error {
	var ns *pb.Namespace
	body, err := c.rpc(
		"GetNamespace",
		&pb.GetNamespaceRequest{
			Service: clientsService,
			Version: clientsVersion,
		},
	)
	if err == nil {
		var resp pb.GetNamespaceResponse
		if err := protojson.Unmarshal(body, &resp); err != nil {
			return err
		}
		ns = resp.Namespace
		fmt.Fprintf(
			c.out,
			"flipr@clients from %s (revision %d):\n",
			c.url,
			resp.Revision,
		)
	} else {
		fmt.Fprintf(c.out, "flipr at %s: not answering (%v); reading "+
			"the store\n", c.url, err)
		s, err := c.openOffline()
		if err != nil {
			return err
		}
		defer s.Close()
		ns, _ = s.GetNamespace(clientsService, clientsVersion)
		fmt.Fprintf(
			c.out,
			"flipr@clients from %s (revision %d):\n",
			c.db,
			s.Revision(),
		)
	}
	if ns == nil {
		fmt.Fprintln(
			c.out,
			"  (not declared yet; flipr declares it at boot)",
		)
		return nil
	}
	refuse := false
	for _, f := range ns.GetFlags() {
		switch {
		case f.GetKey() == keyRefuseOther:
			refuse = f.GetValue().GetBoolValue()
		case strings.HasPrefix(f.GetKey(), knownPrefix):
			v := f.GetValue().GetStringValue()
			mark := "known  "
			if _, _, _, ok := splitClient(v); !ok {
				mark = "retired"
			}
			fmt.Fprintf(
				c.out,
				"  %s %-28s %s\n",
				mark,
				f.GetKey(),
				v,
			)
		}
	}
	fmt.Fprintf(c.out, "  refuse.other: %v\n", refuse)
	return nil
}

// writeClientFlag writes one flag of flipr@clients: through the server when
// it answers (the next request sees it; no restart), else to the files with
// the record appended, which needs the store free.
func (c *ctl) writeClientFlag(key string, v *pb.Value, reason string) error {
	body, err := c.rpc(
		"SetFlag",
		&pb.SetFlagRequest{
			Service: clientsService,
			Version: clientsVersion,
			Key:     key,
			Value:   v,
			Reason:  reason,
		},
	)
	if err == nil {
		var resp pb.SetFlagResponse
		if err := protojson.Unmarshal(body, &resp); err != nil {
			return err
		}
		fmt.Fprintf(
			c.out,
			"flipr@clients/%s written through %s at revision %d; "+
				"the next request sees it\n",
			key,
			c.url,
			resp.Revision,
		)
		return nil
	}
	if !errors.Is(err, errNotAnswering) {
		return err
	}
	fmt.Fprintf(
		c.out,
		"flipr at %s: not answering (%v); writing the files\n",
		c.url,
		err,
	)
	s, err := c.openOffline()
	if err != nil {
		return err
	}
	defer s.Close()
	sink, err := NewFileSink(c.oplog, s.Generation(), NewRegistry())
	if err != nil {
		return err
	}
	defer sink.Close()
	l := NewOpLog(sink, NewRegistry(), NewLogger())
	vj, _ := protojson.Marshal(v)
	if _, err := s.Flip(
		clientsService,
		clientsVersion,
		key,
		v,
		func(*pb.Flag) error {
			return l.OpSync("SetFlag", map[string]any{
				"service":    clientsService,
				"version":    clientsVersion,
				"key":        key,
				"value_json": string(vj),
				"reason":     reason,
				"caller":     clientTool + "@" + Version,
				"revision":   s.Revision(),
			})
		},
	); err != nil {
		return err
	}
	fmt.Fprintf(
		c.out,
		"flipr@clients/%s written to %s at revision %d, recorded; "+
			"flipr reads it when it starts\n",
		key,
		c.db,
		s.Revision(),
	)
	return nil
}

// errNotAnswering says the server could not be reached at all, which is
// when a write may go to the files instead; any answer, refusal included,
// is the server speaking and is returned as it is.
var errNotAnswering = errors.New("not answering")

// rpc posts one request to the running server as the tool, with the tool's
// identity headers, and returns the body of a 200. A non-200 is an error
// carrying flipr's own message; a transport failure wraps errNotAnswering.
func (c *ctl) rpc(method string, req proto.Message) ([]byte, error) {
	body, err := protojson.Marshal(req)
	if err != nil {
		return nil, err
	}
	hr, err := http.NewRequest(
		http.MethodPost,
		c.url+"/flipr.v1.FliprService/"+method,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set("X-Flipr-Caller", clientTool+"@"+Version)
	hr.Header.Set(clientHeader, clientTool+"/"+Version+"/-")
	hc := &http.Client{Timeout: 5 * time.Second}
	resp, err := hc.Do(hr)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errNotAnswering, err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"%s answered %d to %s: %s",
			c.url,
			resp.StatusCode,
			method,
			strings.TrimSpace(string(out)),
		)
	}
	return out, nil
}

// The operator's reads and flips. `get`, `set`, `flags` and `export` are the
// verbs the shell scripts used to do with curl (`bin/flipr`, the export
// CronJob, the cluster's checks), moved into the tool so that they announce
// themselves as fliprctl, and every caller of the API is a known client.
//
// They talk to the running server only; the files are not read for a live
// question.
//
//   get SVC VER KEY                 one flag, as protojson
//   set svc ver key value --reason  one flip; value is a bool, int or string
//   flags [SVC VER]                 every flag, or one namespace's, per line
//   export                          every namespace as protojson, for backup

// get prints one flag.
func (c *ctl) get(args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("usage: get SERVICE VERSION KEY")
	}
	body, err := c.rpc(
		"GetFlag",
		&pb.GetFlagRequest{
			Service: args[0],
			Version: args[1],
			Key:     args[2],
		},
	)
	if err != nil {
		return err
	}
	fmt.Fprintln(c.out, strings.TrimSpace(string(body)))
	return nil
}

// set flips one flag with a reason. The value's type is read from its
// spelling: true or false is a bool, digits are an int, anything else is
// a string; flipr refuses a type that differs from the flag's.
func (c *ctl) set(args []string) error {
	if len(args) < 4 {
		return fmt.Errorf(
			"usage: set SERVICE VERSION KEY VALUE --reason \"...\"",
		)
	}
	fs := flag.NewFlagSet("flipr ctl set", flag.ExitOnError)
	reason := fs.String("reason", "", "why, recorded (required)")
	_ = fs.Parse(args[4:])
	if *reason == "" {
		return fmt.Errorf(
			"--reason is required: say why you are flipping " +
				"this; it is recorded",
		)
	}
	v := valueFromWord(args[3])
	body, err := c.rpc(
		"SetFlag",
		&pb.SetFlagRequest{
			Service: args[0],
			Version: args[1],
			Key:     args[2],
			Value:   v,
			Reason:  *reason,
		},
	)
	if err != nil {
		return err
	}
	fmt.Fprintln(c.out, strings.TrimSpace(string(body)))
	return nil
}

// valueFromWord reads a flag value from how a person spelled it.
func valueFromWord(w string) *pb.Value {
	switch w {
	case "true":
		return &pb.Value{Kind: &pb.Value_BoolValue{BoolValue: true}}
	case "false":
		return &pb.Value{Kind: &pb.Value_BoolValue{BoolValue: false}}
	}
	if n, err := strconv.ParseInt(w, 10, 64); err == nil {
		return &pb.Value{Kind: &pb.Value_IntValue{IntValue: n}}
	}
	return &pb.Value{Kind: &pb.Value_StringValue{StringValue: w}}
}

// flags prints every flag, one line each: namespace, key, value, expensive.
func (c *ctl) flags(args []string) error {
	var nss []*pb.Namespace
	switch len(args) {
	case 0:
		body, err := c.rpc(
			"ListNamespaces",
			&pb.ListNamespacesRequest{},
		)
		if err != nil {
			return err
		}
		var resp pb.ListNamespacesResponse
		if err := protojson.Unmarshal(body, &resp); err != nil {
			return err
		}
		nss = resp.Namespaces
	case 2:
		body, err := c.rpc(
			"GetNamespace",
			&pb.GetNamespaceRequest{
				Service: args[0],
				Version: args[1],
			},
		)
		if err != nil {
			return err
		}
		var resp pb.GetNamespaceResponse
		if err := protojson.Unmarshal(body, &resp); err != nil {
			return err
		}
		nss = []*pb.Namespace{resp.Namespace}
	default:
		return fmt.Errorf("usage: flags [SERVICE VERSION]")
	}
	for _, ns := range nss {
		for _, f := range ns.GetFlags() {
			mark := ""
			if f.GetExpensive() {
				mark = "expensive"
			}
			fmt.Fprintf(
				c.out,
				"%-32s %-28s %-10s %s\n",
				ns.GetService()+"@"+ns.GetVersion(),
				f.GetKey(),
				valueWord(f.GetValue()),
				mark,
			)
		}
	}
	return nil
}

// valueWord spells a value the way set reads one.
func valueWord(v *pb.Value) string {
	switch k := v.GetKind().(type) {
	case *pb.Value_BoolValue:
		return strconv.FormatBool(k.BoolValue)
	case *pb.Value_IntValue:
		return strconv.FormatInt(k.IntValue, 10)
	case *pb.Value_StringValue:
		return strconv.Quote(k.StringValue)
	}
	return "(no value)"
}

// export prints every namespace as protojson, the shape the nightly backup
// keeps (kube/50-export-cronjob.yaml) and `flipr publish` can read back.
func (c *ctl) export() error {
	body, err := c.rpc("ListNamespaces", &pb.ListNamespacesRequest{})
	if err != nil {
		return err
	}
	fmt.Fprintln(c.out, strings.TrimSpace(string(body)))
	return nil
}

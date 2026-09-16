package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fliprv1 "github.com/janearc/flipr-dist/gen/fliprpb"
)

// threeWrites builds the runbook's store: a publish (r1), a flip to false
// (r2), a flip back to true (r3), recorded the way the handlers record them.
// Returns the paths and the generation.
func threeWrites(t *testing.T) (db, oplog, gen string) {
	t.Helper()
	dir := t.TempDir()
	db = filepath.Join(dir, "flipr.db")
	oplog = db + ".oplog"
	reg := NewRegistry()
	store, err := OpenStore(db, reg, NewLogger())
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewFileSink(oplog, store.Generation(), reg)
	if err != nil {
		t.Fatal(err)
	}
	l := NewOpLog(sink, reg, NewLogger())
	gen = store.Generation()
	if _, err := store.PublishNamespace(&fliprv1.Namespace{Service: "s", Version: "v", Flags: []*fliprv1.Flag{
		{Key: "k", Value: &fliprv1.Value{Kind: &fliprv1.Value_BoolValue{BoolValue: true}}, Description: "a test flag"}}}); err != nil {
		t.Fatal(err)
	}
	_ = l.OpSync(
		"PublishNamespace",
		map[string]any{
			"service":  "s",
			"version":  "v",
			"flags":    1,
			"revision": store.Revision(),
		},
	)
	flipRecorded(t, store, l, false)
	flipRecorded(t, store, l, true)
	if store.Revision() != 3 {
		t.Fatalf("revision %d, want 3", store.Revision())
	}
	sink.Close()
	store.Close()
	return db, oplog, gen
}

// flipRecorded flips s@v/k with the record inside the write, as SetFlag does.
func flipRecorded(t *testing.T, store *Store, l *OpLog, v bool) {
	t.Helper()
	val := &fliprv1.Value{Kind: &fliprv1.Value_BoolValue{BoolValue: v}}
	if _, err := store.Flip("s", "v", "k", val, func(*fliprv1.Flag) error {
		vj := `{"boolValue":false}`
		if v {
			vj = `{"boolValue":true}`
		}
		return l.OpSync("SetFlag", map[string]any{"service": "s", "version": "v", "key": "k", "value_json": vj, "reason": "test", "revision": store.Revision()})
	}); err != nil {
		t.Fatal(err)
	}
}

// flagReads opens the store offline and reports s@v/k's value and revision.
func flagReads(
	t *testing.T,
	db string,
) (value bool, revision uint64, generation string) {
	t.Helper()
	s, err := OpenStore(db, NewRegistry(), NewLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f, ok := s.GetFlag("s", "v", "k")
	if !ok {
		t.Fatal("s@v/k is missing")
	}
	return f.GetValue().GetBoolValue(), s.Revision(), s.Generation()
}

// healed wipes the store file and heals a fresh one from the oplog, the
// way main.go does on a fresh generation.
func healed(t *testing.T, db, oplog string) {
	t.Helper()
	if err := os.Remove(db); err != nil {
		t.Fatal(err)
	}
	fresh, err := OpenStore(db, NewRegistry(), NewLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if !fresh.FreshGeneration() {
		t.Fatal("the reopened store should be a fresh generation")
	}
	if _, err := HealFromFile(context.Background(), fresh, oplog, NewLogger()); err != nil {
		t.Fatalf("heal: %v", err)
	}
}

// TestRestoreToARevisionIsTheRunbook walks RUNBOOK-restore.md's steps 3 to
// 5 against real files: a store with three writes, assess, restore to the
// second, assess again, and the way back, which is a second restore.
// Offline, as the runbook says.
func TestRestoreToARevisionIsTheRunbook(t *testing.T) {
	db, oplog, gen := threeWrites(t)
	c := &ctl{
		db:    db,
		oplog: oplog,
		lock:  lockPath(db),
		url:   "http://127.0.0.1:1",
		out:   &bytes.Buffer{},
	}
	// step 3: assess agrees
	if err := c.assess(); err != nil {
		t.Fatalf("assess before: %v", err)
	}
	// step 4: restore to 2 (the flip to false is the last kept)
	var out bytes.Buffer
	c.out = &out
	if err := c.restore([]string{"--to-revision", "2", "--reason", "the flip to true was wrong"}); err != nil {
		t.Fatalf("restore: %v\n%s", err, out.String())
	}
	// the publish is not a replayed record (declarations come back from the
	// clients on the new generation); the one flip below 3 is; the restore
	// itself is revision 4, above everything in the file
	if !strings.Contains(
		out.String(),
		"replayed 1 flips to revision 2; the restore is revision 4 "+
			"(undoing 3..3)",
	) {
		t.Fatalf("restore said: %s", out.String())
	}
	// the old store is kept beside the new one, named by its revision
	kept, _ := filepath.Glob(db + ".before-restore-*-r3")
	if len(kept) != 1 {
		t.Fatalf("the old store was not kept: %v", kept)
	}
	// step 5: assess agrees again, and the store says what revision 2 said
	c.out = &bytes.Buffer{}
	if err := c.assess(); err != nil {
		t.Fatalf("assess after: %v", err)
	}
	v, rev, g := flagReads(t, db)
	if g == gen {
		t.Fatal(
			"a restore mints a new generation so every client " +
				"republishes its declarations, which the " +
				"record does not hold",
		)
	}
	if v != false || rev != 4 {
		t.Fatalf(
			"after restore to 2 the flag reads false at revision "+
				"4: got %v at %d",
			v,
			rev,
		)
	}
	// the record says so, with the range as fields
	recs, _, err := c.oplogTail(1)
	if err != nil || recs[0].Op != "Restore" || recs[0].Revision != 4 ||
		recs[0].RestoreTo != 2 ||
		recs[0].RestoreFrom != 3 ||
		!strings.Contains(recs[0].Value, "to 2 from 3") {
		t.Fatalf("the restore record: %v %v", err, recs)
	}
	// the way back: restore to the revision the first restore left, 3; the
	// second restore stands, the first is undone by it, and the flag reads
	// true again at revision 5
	c.out = &bytes.Buffer{}
	if err := c.restore([]string{"--to-revision", "3", "--reason", "the restore was wrong"}); err != nil {
		t.Fatalf("the way back: %v", err)
	}
	if err := c.assess(); err != nil {
		t.Fatalf("assess after the way back: %v", err)
	}
	if v, rev, _ := flagReads(t, db); v != true || rev != 5 {
		t.Fatalf(
			"after the way back the flag reads true at revision "+
				"5: got %v at %d",
			v,
			rev,
		)
	}
	// to a revision at or above the current is refused
	if err := c.restore([]string{"--to-revision", "5", "--reason", "x"}); err == nil ||
		!strings.Contains(err.Error(), "below it") {
		t.Fatalf("restore to the current revision: %v", err)
	}
}

// TestAHealAfterARestoreComesBackRestored: the record and the restore must
// not break each other. A wipe heal from a file holding a Restore record
// reproduces the restored state, not the state before it; assess is quiet
// after the heal; and the first write after the heal numbers above every
// record in the file.
func TestAHealAfterARestoreComesBackRestored(t *testing.T) {
	db, oplog, _ := threeWrites(t)
	c := &ctl{
		db:    db,
		oplog: oplog,
		lock:  lockPath(db),
		url:   "http://127.0.0.1:1",
		out:   &bytes.Buffer{},
	}
	if err := c.restore([]string{"--to-revision", "2", "--reason", "the flip to true was wrong"}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	healed(t, db, oplog)
	if v, rev, _ := flagReads(t, db); v != false || rev != 4 {
		t.Fatalf(
			"healed after a restore to 2: the flag reads false "+
				"at revision 4, got %v at %d",
			v,
			rev,
		)
	}
	if err := c.assess(); err != nil {
		t.Fatalf("assess after a heal must be quiet: %v", err)
	}
	// a write after the heal is revision 5, not a number the file holds
	s, err := OpenStore(db, NewRegistry(), NewLogger())
	if err != nil {
		t.Fatal(err)
	}
	sink, _ := NewFileSink(oplog, s.Generation(), NewRegistry())
	flipRecorded(t, s, NewOpLog(sink, NewRegistry(), NewLogger()), true)
	if s.Revision() != 5 {
		t.Fatalf(
			"the first write after a heal is revision 5, got %d",
			s.Revision(),
		)
	}
	sink.Close()
	s.Close()
	if err := c.assess(); err != nil {
		t.Fatalf("assess after the write: %v", err)
	}
	// and the way back after all that still works: 3 is the flip to true
	// before the restore, then 5 is the flip to true after it
	healed(t, db, oplog)
	if v, rev, _ := flagReads(t, db); v != true || rev != 5 {
		t.Fatalf("healed again: true at 5, got %v at %d", v, rev)
	}
}

// TestARestoreAcrossAWipeNamesOneRecord: a heal continues the count from
// the file, so a write after a wipe never reuses a number, and a restore to
// a revision on the far side of the wipe applies exactly the records it
// should. Before this, a healed store counted its replayed flips and the
// next write stamped a number the file already held.
func TestARestoreAcrossAWipeNamesOneRecord(t *testing.T) {
	db, oplog, _ := threeWrites(t)
	c := &ctl{
		db:    db,
		oplog: oplog,
		lock:  lockPath(db),
		url:   "http://127.0.0.1:1",
		out:   &bytes.Buffer{},
	}
	healed(t, db, oplog)
	if _, rev, _ := flagReads(t, db); rev != 3 {
		t.Fatalf(
			"a healed store continues from the file's highest "+
				"stamp, 3; got %d (the count of replayed "+
				"flips is 2)",
			rev,
		)
	}
	if err := c.assess(); err != nil {
		t.Fatalf("assess after a heal: %v", err)
	}
	// a flip after the wipe: revision 4
	s, err := OpenStore(db, NewRegistry(), NewLogger())
	if err != nil {
		t.Fatal(err)
	}
	sink, _ := NewFileSink(oplog, s.Generation(), NewRegistry())
	flipRecorded(t, s, NewOpLog(sink, NewRegistry(), NewLogger()), false)
	sink.Close()
	s.Close()
	// restore to 3, the flip to true before the wipe: the post-wipe flip is
	// the one undone
	var out bytes.Buffer
	c.out = &out
	if err := c.restore([]string{"--to-revision", "3", "--reason", "the post-wipe flip was wrong"}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !strings.Contains(
		out.String(),
		"replayed 2 flips to revision 3; the restore is revision 5 "+
			"(undoing 4..4)",
	) {
		t.Fatalf("restore said: %s", out.String())
	}
	if v, rev, _ := flagReads(t, db); v != true || rev != 5 {
		t.Fatalf(
			"restored across the wipe: true at 5, got %v at %d",
			v,
			rev,
		)
	}
	if err := c.assess(); err != nil {
		t.Fatalf("assess: %v", err)
	}
}

// TestTheOfflineVerbsRefuseWhileTheServerHoldsTheStore: bbolt's exclusive
// lock is the guard the runbook relies on.
func TestTheOfflineVerbsRefuseWhileTheServerHoldsTheStore(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "flipr.db")
	held, err := OpenStore(db, NewRegistry(), NewLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	c := &ctl{
		db:    db,
		oplog: db + ".oplog",
		lock:  lockPath(db),
		url:   "http://127.0.0.1:1",
		out:   &bytes.Buffer{},
	}
	start := time.Now()
	err = c.assess()
	if err == nil ||
		!strings.Contains(err.Error(), "held by a running flipr") {
		t.Fatalf("assess against a held store: %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal(
			"the refusal took too long; the open timeout is " +
				"three seconds",
		)
	}
}

// TestLockAndUnlockWriteTheFile: the lock verb writes the file the server
// honours; unlock removes it; a reason is required; a lock on an unhappy
// flipr is refused unless the person says they know.
func TestLockAndUnlockWriteTheFile(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "flipr.db")
	c := &ctl{
		db:    db,
		oplog: db + ".oplog",
		lock:  lockPath(db),
		url:   "http://127.0.0.1:1",
		out:   &bytes.Buffer{},
	}
	if err := c.setLock([]string{"--all"}, true); err == nil ||
		!strings.Contains(err.Error(), "--reason") {
		t.Fatalf("a lock without a reason: %v", err)
	}
	if err := c.setLock([]string{"--all", "--namespace", "a@b", "--reason", "x"}, true); err == nil {
		t.Fatal("both --all and --namespace should be refused")
	}
	// flipr not answering at all: the health check cannot say unhappy, so
	// the lock is written
	if err := c.setLock([]string{"--namespace", "kingfisher@v1", "--reason", "restore rehearsal"}, true); err != nil {
		t.Fatal(err)
	}
	l, err := readLock(c.lock)
	if err != nil || l == nil || l.Scopes[0] != "kingfisher@v1" ||
		l.Reason != "restore rehearsal" ||
		l.By == "" {
		t.Fatalf("the lock file: %v %+v", err, l)
	}
	if err := c.setLock([]string{"--all", "--reason", "done"}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(c.lock); !os.IsNotExist(err) {
		t.Fatal("unlock did not remove the file")
	}
}

// TestStatusAndLogReadTheFilesAndTheServer: status prints from the server
// when it answers and from the files when it does not; log prints the tail.
func TestStatusAndLogReadTheFilesAndTheServer(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "flipr.db")
	oplog := db + ".oplog"
	reg := NewRegistry()
	store, err := OpenStore(db, reg, NewLogger())
	if err != nil {
		t.Fatal(err)
	}
	sink, _ := NewFileSink(oplog, store.Generation(), reg)
	l := NewOpLog(sink, reg, NewLogger())
	if _, err := store.PublishNamespace(&fliprv1.Namespace{Service: "s", Version: "v", Flags: []*fliprv1.Flag{
		{Key: "k", Value: &fliprv1.Value{Kind: &fliprv1.Value_BoolValue{BoolValue: true}}, Description: "a test flag"}}}); err != nil {
		t.Fatal(err)
	}
	_ = l.OpSync(
		"PublishNamespace",
		map[string]any{
			"service":  "s",
			"version":  "v",
			"flags":    1,
			"revision": store.Revision(),
			"caller":   "test",
		},
	)
	inst := NewInstruments(time.Now())
	srv := httptest.NewServer(
		newServer(store, l, inst, NewLogger()).routes(),
	)
	// with the server up: status reads its health, log reads the file
	var out bytes.Buffer
	c := &ctl{
		db:    db,
		oplog: oplog,
		lock:  lockPath(db),
		url:   srv.URL,
		out:   &out,
	}
	if err := c.status(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "healthy") ||
		!strings.Contains(out.String(), "revision 1") ||
		!strings.Contains(out.String(), "no lock") {
		t.Fatalf("status with the server up: %s", out.String())
	}
	out.Reset()
	if err := c.tail([]string{"-tail", "5"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "PublishNamespace s@v") ||
		!strings.Contains(out.String(), "by test") {
		t.Fatalf("log: %s", out.String())
	}
	// with the server down: status reads the files
	srv.Close()
	sink.Close()
	store.Close()
	out.Reset()
	c.url = "http://127.0.0.1:1"
	if err := c.status(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "not answering") ||
		!strings.Contains(out.String(), "revision 1") ||
		!strings.Contains(out.String(), "namespaces 1") {
		t.Fatalf("status with the server down: %s", out.String())
	}
	// a lock file shows on status
	if _, err := writeLock(lockPath(db), []string{"*"}, "rehearsal"); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := c.status(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "LOCKED (*)") {
		t.Fatalf("status with a lock: %s", out.String())
	}
}

// TestCtlMainDispatches: the subcommand parses its flags and verbs.
func TestCtlMainDispatches(t *testing.T) {
	if os.Getenv("FLIPR_CTL_CHILD") == "1" {
		ctlMain(strings.Split(os.Getenv("FLIPR_CTL_ARGS"), " "))
		return
	}
	dir := t.TempDir()
	db := filepath.Join(dir, "flipr.db")
	s, err := OpenStore(db, NewRegistry(), NewLogger())
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	run := func(args string) (string, int) {
		cmd := exec.Command(
			os.Args[0],
			"-test.run=TestCtlMainDispatches",
		)
		cmd.Env = append(
			os.Environ(),
			"FLIPR_CTL_CHILD=1",
			"FLIPR_CTL_ARGS="+args,
		)
		out, err := cmd.CombinedOutput()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
		return string(out), code
	}
	if out, code := run("-db " + db + " -url http://127.0.0.1:1 assess"); code != 0 ||
		!strings.Contains(out, "ASSESSED") {
		t.Fatalf("assess: %d %s", code, out)
	}
	if out, code := run("-db " + db + " nonsense"); code != 2 ||
		!strings.Contains(out, "usage") {
		t.Fatalf("an unknown verb: %d %s", code, out)
	}
	if _, code := run("-db " + db + " -url http://127.0.0.1:1 lock --all"); code != 1 {
		t.Fatalf("a lock without a reason should exit 1, got %d", code)
	}
}

// TestClientsVerbWritesTheSetThroughTheServerAndTheFiles: `clients add`
// goes through a running flipr and the next request sees it; with the
// server down it goes to the files with the record; `remove` retires by
// value; `refuse-other on` refuses a curl on the next request. No restart
// in any of it.
func TestClientsVerbWritesTheSetThroughTheServerAndTheFiles(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "flipr.db")
	oplog := db + ".oplog"
	reg := NewInstruments(time.Now())
	store, err := OpenStore(db, reg.Registry(), NewLogger())
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewFileSink(oplog, store.Generation(), reg.Registry())
	if err != nil {
		t.Fatal(err)
	}
	l := NewOpLog(sink, reg.Registry(), NewLogger())
	if err := declareClients(store, l); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(
		newServer(store, l, reg, NewLogger()).routes(),
	)

	var out bytes.Buffer
	c := &ctl{
		db:    db,
		oplog: oplog,
		lock:  lockPath(db),
		url:   srv.URL,
		out:   &out,
	}
	if err := c.clients([]string{"add", "go/v1.1.0/abc123def456", "--reason", "clients/go/v1.1.0 cut"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	if !strings.Contains(out.String(), "known.go.v1-1-0 written through") {
		t.Fatalf("add said: %s", out.String())
	}
	// a second entry for the same lang/tag is refused unless replaced
	if err := c.clients([]string{"add", "go/v1.1.0/ffffffffffff", "--reason", "oops"}); err == nil ||
		!strings.Contains(err.Error(), "already known") {
		t.Fatalf("a duplicate lang/tag: %v", err)
	}
	if err := c.clients([]string{"add", "go/v1.1.0/abc123def456", "--reason", "the same again", "--replace"}); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if code, _ := postAs(t, srv, "go/v1.1.0/abc123def456", "Ping", `{}`); code != http.StatusOK {
		t.Fatal("known now")
	}
	if n := reg.Registry().Counter(mClientRequests, "go/v1.1.0", "Ping").Value(); n != 1 {
		t.Fatalf("the client is counted by name after the add: %d", n)
	}
	out.Reset()
	if err := c.clients([]string{"refuse-other", "on", "--reason", "audit clean"}); err != nil {
		t.Fatalf("refuse-other on: %v", err)
	}
	if code, body := postAs(t, srv, "", "Ping", `{}`); code != http.StatusForbidden ||
		!strings.Contains(body, "unknown_client") {
		t.Fatalf("a curl after refuse-other on: %d %s", code, body)
	}
	// the tool itself is still heard: it lists and removes while refusal is
	// on
	out.Reset()
	if err := c.clients(nil); err != nil {
		t.Fatalf("list: %v", err)
	}
	if !strings.Contains(out.String(), "known   known.go.v1-1-0") ||
		!strings.Contains(out.String(), "refuse.other: true") {
		t.Fatalf("list said: %s", out.String())
	}
	if err := c.clients([]string{"remove", "go/v1.1.0", "--reason", "superseded"}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if code, _ := postAs(t, srv, "go/v1.1.0/abc123def456", "Ping", `{}`); code != http.StatusForbidden {
		t.Fatal(
			"a removed client is other, and refused while " +
				"refusal is on",
		)
	}
	if err := c.clients([]string{"refuse-other", "off", "--reason", "done"}); err != nil {
		t.Fatal(err)
	}

	// the server goes down; the write goes to the files with the record
	srv.Close()
	sink.Close()
	store.Close()
	c.url = "http://127.0.0.1:1"
	out.Reset()
	if err := c.clients([]string{"add", "py/v1.1.0/0123456789ab", "--reason", "prepared before boot"}); err != nil {
		t.Fatalf("offline add: %v", err)
	}
	if !strings.Contains(out.String(), "written to") ||
		!strings.Contains(out.String(), "recorded") {
		t.Fatalf("offline add said: %s", out.String())
	}
	if err := c.assess(); err != nil {
		t.Fatalf("assess after the offline write: %v", err)
	}
	recs, _, err := c.oplogTail(1)
	if err != nil || recs[0].Op != "SetFlag" ||
		recs[0].Key != "known.py.v1-1-0" ||
		recs[0].Caller != clientTool+"@"+Version {
		t.Fatalf("the offline write's record: %v %v", err, recs)
	}
	out.Reset()
	if err := c.clients(nil); err != nil ||
		!strings.Contains(out.String(), "known   known.py.v1-1-0") ||
		!strings.Contains(out.String(), "retired known.go.v1-1-0") {
		t.Fatalf("offline list: %v %s", err, out.String())
	}
	// usage errors are errors, not writes
	for _, bad := range [][]string{{"add", "nonsense"}, {"add", "go/v1/abc"}, {"remove", "go"}, {"refuse-other", "maybe", "--reason", "x"}, {"what"}} {
		if err := c.clients(bad); err == nil {
			t.Fatalf("%v should be refused", bad)
		}
	}
}

// TestTheOperatorsReadsAndFlipsGoThroughTheTool: get, set, flags and export
// against a running flipr, announcing themselves as fliprctl (so they are
// known with refusal on), with set's reason required and the value's type
// read from its spelling.
func TestTheOperatorsReadsAndFlipsGoThroughTheTool(t *testing.T) {
	reg := NewInstruments(time.Now())
	store, err := OpenStore(
		filepath.Join(t.TempDir(), "flipr.db"),
		reg.Registry(),
		NewLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	sink := &captureSink{}
	l := NewOpLog(sink, reg.Registry(), NewLogger())
	if err := declareClients(store, l); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(
		newServer(store, l, reg, NewLogger()).routes(),
	)
	t.Cleanup(func() { srv.Close(); store.Close() })
	postAs(
		t,
		srv,
		"",
		"PublishNamespace",
		`{"namespace":{"service":"dodo","version":"v1","flags":[{"key":"fetch.enabled","value":{"boolValue":true},"description":"on: fetches. off: does not."},{"key":"fetch.limit","value":{"intValue":"3"},"description":"how many"}]}}`,
	)
	var out bytes.Buffer
	c := &ctl{url: srv.URL, out: &out}
	// refusal on: the tool is still heard, being this binary
	if err := c.clients([]string{"refuse-other", "on", "--reason", "test"}); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := c.get([]string{"dodo", "v1", "fetch.enabled"}); err != nil ||
		!strings.Contains(
			strings.ReplaceAll(out.String(), " ", ""),
			`"boolValue":true`,
		) {
		t.Fatalf("get: %v %s", err, out.String())
	}
	if err := c.set([]string{"dodo", "v1", "fetch.enabled", "false"}); err == nil ||
		!strings.Contains(err.Error(), "reason") {
		t.Fatalf("set without a reason is refused by the tool: %v", err)
	}
	out.Reset()
	if err := c.set([]string{"dodo", "v1", "fetch.enabled", "false", "--reason", "pausing"}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if f, _ := store.GetFlag("dodo", "v1", "fetch.enabled"); f.GetValue().
		GetBoolValue() {
		t.Fatal("the flip did not land")
	}
	// a type read from its spelling, and flipr's refusal of a type change
	// comes back as flipr said it
	if err := c.set([]string{"dodo", "v1", "fetch.limit", "yes", "--reason", "x"}); err == nil ||
		!strings.Contains(err.Error(), "400") {
		t.Fatalf(
			"a string onto an int flag: flipr refuses, the tool "+
				"relays: %v",
			err,
		)
	}
	if err := c.set([]string{"dodo", "v1", "fetch.limit", "7", "--reason", "more"}); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := c.flags(nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "dodo@v1") ||
		!strings.Contains(out.String(), "fetch.limit") ||
		!strings.Contains(out.String(), " 7 ") ||
		!strings.Contains(out.String(), "flipr@clients") {
		t.Fatalf("flags: %s", out.String())
	}
	out.Reset()
	if err := c.flags([]string{"dodo", "v1"}); err != nil ||
		strings.Contains(out.String(), "flipr@clients") {
		t.Fatalf("flags of one namespace: %v %s", err, out.String())
	}
	out.Reset()
	if err := c.export(); err != nil ||
		!strings.Contains(out.String(), `"namespaces"`) ||
		!strings.Contains(out.String(), `"fetch.limit"`) {
		t.Fatalf("export: %v %s", err, out.String())
	}
	if err := c.get([]string{"dodo"}); err == nil {
		t.Fatal("usage")
	}
	if n := reg.Registry().Counter(mClientRequests, clientTool, "SetFlag").Value(); n != 4 {
		t.Fatalf("the tool's flips are counted as fliprctl: %d", n)
	}
}

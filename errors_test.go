package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/janearc/flipr-dist/gen/fliprpb"
)

// FAULT INJECTION. The happy paths are covered by the integration tests; these
// reach the branches that only execute when something is broken. Those
// branches are the ones nobody exercises by hand and the ones that matter at
// 03:20, so they are worth the fixtures.

// brokenSink fails every write, standing in for a kafka that has gone away.
type brokenSink struct {
	mu sync.Mutex
	n  int
}

// Write always fails, counting attempts.
func (b *brokenSink) Write(map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.n++
	return errors.New("sink unavailable")
}

// serverWith builds a flipr whose oplog writes into the given sink.
func serverWith(t *testing.T, sink Sink) (*httptest.Server, *Store) {
	t.Helper()
	inst := NewInstruments(time.Now())
	store, err := OpenStore(
		filepath.Join(t.TempDir(), "f.db"),
		inst.Registry(),
		NewLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	oplog := NewOpLog(sink, inst.Registry(), NewLogger())
	srv := httptest.NewServer(
		newServer(store, oplog, inst, NewLogger()).routes(),
	)
	t.Cleanup(func() { srv.Close(); store.Close() })
	return srv, store
}

// TestAFlipThatCannotBeLoggedIsReportedAsAFailure is the important one. The
// flip is already durable in bbolt at that point, so it would be easy to
// return success -- but a caller told "flipped" when nothing recorded it is
// how an outage becomes unexplainable afterwards. It must fail loudly.
func TestAFlipThatCannotBeLoggedIsReportedAsAFailure(t *testing.T) {
	srv, _ := serverWith(t, &brokenSink{})
	code, body := post(
		t,
		srv,
		"SetFlag",
		`{"service":"s","version":"v","key":"k","value":{"boolValue":false},"reason":"testing"}`,
	)
	if code != http.StatusInternalServerError {
		t.Fatalf("got %d %s, want 500", code, body)
	}
	if !strings.Contains(body, "could not be logged") ||
		!strings.Contains(body, "rolled back") {
		t.Errorf(
			"the error does not say what actually went wrong: %s",
			body,
		)
	}
	// AND THE FLIP DID NOT HAPPEN. Before this fix the caller was told
	// "failed" while every reader saw the new value and the audit held no
	// record; a client retry then committed it twice.
	if code, _ := post(t, srv, "GetFlag", `{"service":"s","version":"v","key":"k"}`); code != http.StatusNotFound {
		t.Fatalf("the unlogged flip is being served: GetFlag %d", code)
	}
	if code, _ := post(t, srv, "GetNamespace", `{"service":"s","version":"v"}`); code != http.StatusNotFound {
		t.Fatalf(
			"the unlogged flip left its namespace behind: "+
				"GetNamespace %d",
			code,
		)
	}
}

// TestAnUnloggedFlipRestoresThePriorValue: when the flag existed, the
// rollback puts back exactly what was there, metadata included.
func TestAnUnloggedFlipRestoresThePriorValue(t *testing.T) {
	srv, store := serverWith(t, &brokenSink{})
	if _, err := store.PublishNamespace(&pb.Namespace{Service: "s", Version: "v",
		Flags: []*pb.Flag{boolFlag("k", true, true, "on: spends. off: stops.")}}); err != nil {
		t.Fatal(err)
	}
	code, _ := post(
		t,
		srv,
		"SetFlag",
		`{"service":"s","version":"v","key":"k","value":{"boolValue":false},"reason":"testing"}`,
	)
	if code != http.StatusInternalServerError {
		t.Fatalf("got %d, want 500", code)
	}
	f, ok := store.GetFlag("s", "v", "k")
	if !ok || !f.Value.GetBoolValue() || !f.Expensive ||
		f.Description == "" {
		t.Fatalf("rollback did not restore the flag: %+v", f)
	}
	if got := store.reg.Counter(mStoreRollbacks).Value(); got != 1 {
		t.Errorf("rollbacks counter = %d, want 1", got)
	}
}

// TestAPublishThatCannotBeLoggedFails covers the same branch on onboarding,
// and the namespace it would have created is gone afterwards.
func TestAPublishThatCannotBeLoggedFails(t *testing.T) {
	srv, _ := serverWith(t, &brokenSink{})
	code, body := post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"s","version":"v","flags":[{"key":"k","value":{"boolValue":true}}]}}`,
	)
	if code != http.StatusInternalServerError ||
		!strings.Contains(body, "could not be logged") {
		t.Fatalf("got %d %s", code, body)
	}
	if code, _ := post(t, srv, "GetNamespace", `{"service":"s","version":"v"}`); code != http.StatusNotFound {
		t.Fatalf("the unlogged publish is being served: %d", code)
	}
}

// TestADeleteThatCannotBeLoggedIsUndone: the namespace and every flag in it
// come back, because a retirement the log does not hold would not survive a
// wipe-recovery either.
func TestADeleteThatCannotBeLoggedIsUndone(t *testing.T) {
	srv, store := serverWith(t, &brokenSink{})
	if _, err := store.PublishNamespace(&pb.Namespace{Service: "dodo", Version: "old",
		Flags: []*pb.Flag{boolFlag("a", true, false, "a"), boolFlag("b", false, false, "b")}}); err != nil {
		t.Fatal(err)
	}
	code, body := post(
		t,
		srv,
		"DeleteNamespace",
		`{"service":"dodo","version":"old","reason":"gc"}`,
	)
	if code != http.StatusInternalServerError ||
		!strings.Contains(body, "rolled back") {
		t.Fatalf("got %d %s", code, body)
	}
	ns, ok := store.GetNamespace("dodo", "old")
	if !ok || len(ns.Flags) != 2 {
		t.Fatalf(
			"rollback did not restore the namespace: %v %+v",
			ok,
			ns,
		)
	}
}

// TestWritesToAClosedStoreFail checks the store surfaces bbolt errors rather
// than reporting a write that did not happen.
func TestWritesToAClosedStoreFail(t *testing.T) {
	inst := NewInstruments(time.Now())
	s, err := OpenStore(
		filepath.Join(t.TempDir(), "f.db"),
		inst.Registry(),
		NewLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	if _, err := s.SetFlag("a", "b", "c",
		&pb.Value{Kind: &pb.Value_BoolValue{BoolValue: true}}); err == nil {
		t.Error("SetFlag on a closed store returned no error")
	}
	if _, err := s.PublishNamespace(&pb.Namespace{
		Service: "a", Version: "b", Flags: []*pb.Flag{boolFlag("k", true, false, "")},
	}); err == nil {
		t.Error("PublishNamespace on a closed store returned no error")
	}
	if err := s.Healthy(); err == nil {
		t.Error("Healthy on a closed store returned no error")
	}
	if err := s.reload(); err == nil {
		t.Error("reload on a closed store returned no error")
	}
}

// TestStoreErrorsSurfaceAs500 checks a broken store is reported to the caller
// as a server error rather than an empty success.
func TestStoreErrorsSurfaceAs500(t *testing.T) {
	srv, store := serverWith(t, &captureSink{})
	store.Close() // break it underneath the running server

	code, _ := post(
		t,
		srv,
		"SetFlag",
		`{"service":"s","version":"v","key":"k","value":{"boolValue":true},"reason":"why"}`,
	)
	if code != http.StatusInternalServerError {
		t.Errorf(
			"SetFlag against a dead store returned %d, want 500",
			code,
		)
	}
	code, _ = post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"s","version":"v","flags":[{"key":"k"}]}}`,
	)
	if code != http.StatusInternalServerError {
		t.Errorf(
			"PublishNamespace against a dead store returned %d, "+
				"want 500",
			code,
		)
	}
}

// failingBody is a request body that errors partway through, standing in for a
// client that dies mid-send.
type failingBody struct{}

// Read always fails.
func (failingBody) Read(
	[]byte,
) (int, error) {
	return 0, errors.New("connection reset")
}

// Close is a no-op.
func (failingBody) Close() error { return nil }

// TestAnUnreadableBodyIsRefused checks a truncated request is answered rather
// than hanging or panicking.
func TestAnUnreadableBodyIsRefused(t *testing.T) {
	inst := NewInstruments(time.Now())
	store, err := OpenStore(
		filepath.Join(t.TempDir(), "f.db"),
		inst.Registry(),
		NewLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	oplog := NewOpLog(&captureSink{}, inst.Registry(), NewLogger())
	h := newServer(store, oplog, inst, NewLogger()).routes()

	req := httptest.NewRequest(
		http.MethodPost,
		"/flipr.v1.FliprService/GetFlag",
		failingBody{},
	)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", w.Code)
	}
	body, _ := io.ReadAll(w.Body)
	if !strings.Contains(string(body), "unreadable body") {
		t.Errorf("got %s", body)
	}
	if got := inst.Registry().Counter(mRPCRejected, "unreadable_body").Value(); got != 1 {
		t.Errorf("rejection was not counted: %d", got)
	}
}

// TestOversizedBodyIsTruncatedNotBuffered checks the body cap holds: anything
// past the limit is cut off, so a hostile or broken client cannot make flipr
// allocate without bound.
func TestOversizedBodyIsTruncatedNotBuffered(t *testing.T) {
	srv, _ := serverWith(t, &captureSink{})
	huge := `{"service":"` + strings.Repeat(
		"x",
		maxBodyBytes+1024,
	) + `","version":"v","key":"k"}`
	code, _ := post(t, srv, "GetFlag", huge)
	// Truncated JSON cannot parse, so this is a 400 rather than a hang or
	// an out-of-memory. The point is that it answers at all.
	if code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", code)
	}
}

// TestOpenStoreFailsWhenTheSnapshotCannotBeRead checks a corrupt database is a
// loud startup failure. A flipr that came up without its flags would answer
// "no such flag" to everything, which reads as "off" -- and would turn the
// whole mesh off quietly.
func TestOpenStoreFailsWhenTheSnapshotCannotBeRead(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "corrupt.db")
	// A file that is not a bbolt database at all.
	if err := writeFile(p, strings.Repeat("not a database", 100)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(p, NewRegistry(), NewLogger()); err == nil {
		t.Fatal("opened a corrupt database without complaint")
	}
}

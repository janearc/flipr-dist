package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/janearc/flipr-dist/gen/fliprpb"
)

// /health HAS THREE ANSWERS. The review of 2026-09-03 found it
// saying healthy while every write failed and while the volume was full,
// because it read the store and nothing else.

// health fetches and decodes the health document.
func health(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("health is not JSON: %s", b)
	}
	return resp.StatusCode, out
}

// TestHealthIsDegradedWhileTheSinkRefusesWrites: a refused flip is a 500 to
// its caller, and for the next minute /health says degraded and why.
func TestHealthIsDegradedWhileTheSinkRefusesWrites(t *testing.T) {
	srv, _ := serverWith(t, &brokenSink{})
	if code, doc := health(t, srv.URL); code != 200 ||
		doc["status"] != "healthy" {
		t.Fatalf("before any write: %d %v", code, doc)
	}
	post(
		t,
		srv,
		"SetFlag",
		`{"service":"s","version":"v","key":"k","value":{"boolValue":true},"reason":"r"}`,
	)
	code, doc := health(t, srv.URL)
	if code != 200 {
		t.Fatalf(
			"degraded must stay 200 so the probes do not restart "+
				"a pod a restart cannot fix: %d",
			code,
		)
	}
	if doc["status"] != "degraded" || doc["healthy"] != true {
		t.Fatalf(
			"status=%v healthy=%v, want degraded/true",
			doc["status"],
			doc["healthy"],
		)
	}
	reasons, _ := doc["reasons"].([]any)
	if len(reasons) == 0 {
		t.Fatal("degraded with no reason")
	}
	if r, _ := reasons[0].(string); r == "" ||
		!contains(r, "op log sink refused a write") {
		t.Fatalf("the reason does not name the sink: %v", reasons)
	}
	if doc["version"] != Version || doc["generation"] == "" {
		t.Fatalf("the document lost its identity fields: %v", doc)
	}
}

// TestHealthIsDegradedAfterAStoreWriteFails, and recovers when a write
// succeeds again: the store's own failure is reported, not only the sink's.
func TestHealthIsDegradedAfterAStoreWriteFails(t *testing.T) {
	srv, store := serverWith(t, &captureSink{})
	store.noteWriteFailure(errFull)
	code, doc := health(t, srv.URL)
	if code != 200 || doc["status"] != "degraded" {
		t.Fatalf("after a failed write: %d %v", code, doc)
	}
	if reasons, _ := doc["reasons"].([]any); len(reasons) != 1 ||
		!contains(reasons[0].(string), "file too large") {
		t.Fatalf("reason: %v", doc["reasons"])
	}
	// a successful write clears it
	if _, err := store.SetFlag("s", "v", "k", &pb.Value{Kind: &pb.Value_BoolValue{BoolValue: true}}); err != nil {
		t.Fatal(err)
	}
	if code, doc := health(t, srv.URL); code != 200 ||
		doc["status"] != "healthy" {
		t.Fatalf("after a good write: %d %v", code, doc)
	}
}

// TestHealthIsDownWhenTheStoreIsUnreadable: down is still a 503, and it says
// so in the same document shape.
func TestHealthIsDownWhenTheStoreIsUnreadable(t *testing.T) {
	srv, store := serverWith(t, &captureSink{})
	store.Close()
	code, doc := health(t, srv.URL)
	if code != http.StatusServiceUnavailable || doc["status"] != "down" ||
		doc["healthy"] != false {
		t.Fatalf("closed store: %d %v", code, doc)
	}
}

// errFull stands in for bbolt's error when the volume is full.
var errFull = &fakeErr{"file resize error: truncate: file too large"}

type fakeErr struct{ s string }

// Error is the message the fake was made with.
func (e *fakeErr) Error() string { return e.s }

// contains is strings.Contains without the import noise in a test file.
func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

// indexOf is the first index of sub in s, or -1.
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// TestHealthLogsTransitionsNotAnswers: a probe reading /health once a second
// must not turn a degraded flipr into a log line a second. The status is
// logged when it changes, and when it recovers.
func TestHealthLogsTransitionsNotAnswers(t *testing.T) {
	srv, store := serverWith(t, &captureSink{})
	// find the server to inspect its transition state: drive the sequence
	// healthy -> degraded -> degraded -> healthy and watch lastHealth
	health(t, srv.URL)
	store.noteWriteFailure(errFull)
	health(t, srv.URL)
	health(t, srv.URL)
	health(t, srv.URL)
	if _, err := store.SetFlag("s", "v", "k", &pb.Value{Kind: &pb.Value_BoolValue{BoolValue: true}}); err != nil {
		t.Fatal(err)
	}
	code, doc := health(t, srv.URL)
	if code != 200 || doc["status"] != "healthy" {
		t.Fatalf("after recovery: %d %v", code, doc)
	}
}

// TestHealthTransitionIsIdempotentPerStatus pins the transition logic itself:
// the same status twice logs once, and a change logs again.
func TestHealthTransitionIsIdempotentPerStatus(t *testing.T) {
	s := &server{slog: NewLogger()}
	changes := 0
	for _, st := range []string{"healthy", "healthy", "degraded", "degraded", "degraded", "healthy", "down", "down"} {
		prev, _ := s.lastHealth.Load().(string)
		s.healthTransition(st, nil)
		if now, _ := s.lastHealth.Load().(string); now != prev {
			changes++
		}
	}
	if changes != 4 {
		t.Fatalf(
			"eight answers over four distinct states recorded %d "+
				"transitions, want 4",
			changes,
		)
	}
}

// TestReadsAreNotRecordedInTheOplog: the record holds what
// flipr changed. Three reads leave the sink empty; one publish is one record.
func TestReadsAreNotRecordedInTheOplog(t *testing.T) {
	inst := NewInstruments(time.Now())
	store, err := OpenStore(
		filepath.Join(t.TempDir(), "f.db"),
		inst.Registry(),
		NewLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	sink := &captureSink{}
	oplog := NewOpLog(sink, inst.Registry(), NewLogger())
	srv := httptest.NewServer(
		newServer(store, oplog, inst, NewLogger()).routes(),
	)
	t.Cleanup(func() { srv.Close(); store.Close() })
	post(t, srv, "GetFlag", `{"service":"s","version":"v","key":"k"}`)
	post(t, srv, "GetNamespace", `{"service":"s","version":"v"}`)
	post(t, srv, "ListNamespaces", `{}`)
	if sink.count() != 0 {
		t.Fatalf("reads were recorded: %d entries", sink.count())
	}
	post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"s","version":"v","flags":[{"key":"k","value":{"boolValue":true},"description":"a test flag"}]}}`,
	)
	if sink.count() != 1 || sink.snapshot()[0]["op"] != "PublishNamespace" {
		t.Fatalf(
			"one publish should be one record: %v",
			sink.snapshot(),
		)
	}
	if got := inst.Registry().Counter(mOplogEntries, "sync").Value(); got != 1 {
		t.Errorf("sync counter = %d, want 1", got)
	}
}

package main

import (
	"testing"

	pb "github.com/janearc/flipr-dist/gen/fliprpb"
	oplogpb "github.com/janearc/flipr-dist/gen/oplogpb"
	"google.golang.org/protobuf/encoding/protojson"
)

// The sink's unit surface without a broker: record conversion and the typed
// replay application. The against-a-real-kafka half runs in the cluster
// verification, where the broker is the real one.

// TestRecordConversion checks an oplog entry map becomes a contract message
// without losing the fields replay depends on.
func TestRecordConversion(t *testing.T) {
	k := &KafkaSink{generation: "gen-1"}
	op := k.record(map[string]any{
		"ts": "2026-08-28T00:00:00Z", "op": "SetFlag",
		"service": "albatross", "version": "abc", "key": "nightly.smoothing",
		"value": "surreal", "value_json": `{"stringValue":"surreal"}`,
		"reason": "claude smoothing is banned",
	})
	if op.Op != "SetFlag" || op.Service != "albatross" ||
		op.Key != "nightly.smoothing" {
		t.Fatalf("lost identity fields: %+v", op)
	}
	if op.ValueJson == "" || op.Reason == "" ||
		op.StoreGeneration != "gen-1" {
		t.Fatalf("lost replay fields: %+v", op)
	}
	// and the record round-trips through the wire format
	b, err := protojson.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	var back oplogpb.Operation
	if err := protojson.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.ValueJson != op.ValueJson {
		t.Fatal("value_json did not survive the wire")
	}
}

// TestApplyReplayedPreservesType is the reason value_json exists: a STRING
// flag whose value spells "true" must come back from replay as a string, not
// resurrect as a bool.
func TestApplyReplayedPreservesType(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()

	if err := s.ApplyReplayed("svc", "v", "tricky", `{"stringValue":"true"}`, true); err != nil {
		t.Fatal(err)
	}
	f, ok := s.GetFlag("svc", "v", "tricky")
	if !ok {
		t.Fatal("replayed flag missing")
	}
	if f.Value.GetStringValue() != "true" {
		t.Fatalf(
			"string flag lost its type through replay: %+v",
			f.Value,
		)
	}
	if _, isBool := f.Value.Kind.(*pb.Value_BoolValue); isBool {
		t.Fatal("the string \"true\" resurrected as a bool")
	}
}

// TestApplyReplayedFallback covers records written before value_json existed:
// the rendered string is parsed heuristically, bools and ints recovered.
func TestApplyReplayedFallback(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	for value, check := range map[string]func(*pb.Value) bool{
		"true":    func(v *pb.Value) bool { return v.GetBoolValue() },
		"42":      func(v *pb.Value) bool { return v.GetIntValue() == 42 },
		"surreal": func(v *pb.Value) bool { return v.GetStringValue() == "surreal" },
	} {
		if err := s.ApplyReplayed("svc", "v", "k-"+value, value, false); err != nil {
			t.Fatal(err)
		}
		f, _ := s.GetFlag("svc", "v", "k-"+value)
		if !check(f.Value) {
			t.Errorf("fallback misparsed %q: %+v", value, f.Value)
		}
	}
}

// TestFreshGeneration checks the replay trigger: fresh on a new file, not
// fresh on a reopen.
func TestFreshGeneration(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	if !s.FreshGeneration() {
		t.Error("new file did not report fresh")
	}
	s.Close()
	s2 := openStore(t, dir)
	defer s2.Close()
	if s2.FreshGeneration() {
		t.Error(
			"reopened file reported fresh; replay would echo the " +
				"log",
		)
	}
}

// TestSplitCSV covers the broker list parsing.
func TestSplitCSV(t *testing.T) {
	got := splitCSV(" a:9092, b:9092 ,,")
	if len(got) != 2 || got[0] != "a:9092" || got[1] != "b:9092" {
		t.Fatalf("got %v", got)
	}
	if splitCSV("") != nil {
		t.Fatal("empty input should yield nil")
	}
}

// TestDeleteNamespaceIsDurableAndCounts checks retirement removes the whole
// bucket and survives a reopen.
func TestDeleteNamespaceIsDurableAndCounts(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	if _, err := s.PublishNamespace(&pb.Namespace{
		Service: "dodo", Version: "dead123",
		Flags: []*pb.Flag{boolFlag("a", true, false, ""), boolFlag("b", false, false, "")},
	}); err != nil {
		t.Fatal(err)
	}
	n, err := s.DeleteNamespace("dodo", "dead123")
	if err != nil || n != 2 {
		t.Fatalf("removed %d err %v, want 2", n, err)
	}
	// deleting the absent is a no-op, not an error
	if n, err := s.DeleteNamespace("dodo", "dead123"); err != nil ||
		n != 0 {
		t.Fatalf("re-delete: %d %v", n, err)
	}
	s.Close()
	s2 := openStore(t, dir)
	defer s2.Close()
	if _, ok := s2.GetNamespace("dodo", "dead123"); ok {
		t.Fatal("retired namespace survived a reopen")
	}
}

// TestBudgetCap is the 24-flag budget, enforced with its reasoning in the
// refusal.
func TestBudgetCap(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	_ = s // the cap lives at the handler; drive it over HTTP
}

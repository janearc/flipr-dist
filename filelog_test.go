package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/janearc/flipr-dist/gen/fliprpb"
)

// THE OPLOG FILE: the record, the mirror, and the replay. These are the
// proofs the throwaway cluster will repeat with a kill; here they are the
// same shapes in-process.

// TestEveryRecordIsWrittenThroughAndFsynced: the oplog holds flips only,
// and each one is on disk, in order, before Write returns.
func TestEveryRecordIsWrittenThroughAndFsynced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flipr.oplog")
	s, err := NewFileSink(path, "gen-1", NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(map[string]any{"op": "PublishNamespace", "service": "s", "version": "v", "flags_json": `[]`}); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); !strings.Contains(
		string(b),
		`"op":"PublishNamespace"`,
	) {
		t.Fatalf(
			"the first record was not on disk when Write "+
				"returned: %q",
			b,
		)
	}
	if err := s.Write(map[string]any{"op": "SetFlag", "service": "s", "version": "v", "key": "k", "value_json": `{"boolValue":true}`, "reason": "r"}); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 2 {
		t.Fatalf(
			"two flips must be two records on disk, in order; "+
				"got %d lines: %q",
			len(lines),
			b,
		)
	}
	if !strings.Contains(lines[0], `"op":"PublishNamespace"`) ||
		!strings.Contains(lines[1], `"op":"SetFlag"`) ||
		!strings.Contains(lines[1], `"storeGeneration":"gen-1"`) {
		t.Fatalf("records out of order or missing fields: %q", b)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestReplayFromFileRestoresInOrderAndRefusesGarbage: the file replays like
// the topic did, with the same refusal on a record it cannot apply.
func TestReplayFromFileRestoresInOrderAndRefusesGarbage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "flipr.oplog")
	s, err := NewFileSink(path, "gen-1", NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	must := func(e map[string]any) {
		if err := s.Write(e); err != nil {
			t.Fatal(err)
		}
	}
	must(map[string]any{"op": "GetFlag", "service": "a", "key": "x"})
	must(
		map[string]any{
			"op":         "SetFlag",
			"service":    "a",
			"version":    "v",
			"key":        "k1",
			"value_json": `{"boolValue":true}`,
		},
	)
	must(
		map[string]any{
			"op":         "SetFlag",
			"service":    "a",
			"version":    "v",
			"key":        "k1",
			"value_json": `{"boolValue":false}`,
		},
	)
	must(
		map[string]any{
			"op":         "SetFlag",
			"service":    "dodo",
			"version":    "old",
			"key":        "z",
			"value_json": `{"intValue":"3"}`,
		},
	)
	must(
		map[string]any{
			"op":      "DeleteNamespace",
			"service": "dodo",
			"version": "old",
		},
	)
	s.Close()

	store := openStore(t, dir)
	defer store.Close()
	r, err := ReplayFile(
		context.Background(),
		path,
		NewLogger(),
		store.ApplyReplayed,
		func(sv, v string) error { _, err := store.DeleteNamespace(sv, v); return err },
	)
	if err != nil || r.Applied != 4 {
		t.Fatalf("applied=%d err=%v, want 4 and nil", r.Applied, err)
	}
	f, ok := store.GetFlag("a", "v", "k1")
	if !ok || f.Value.GetBoolValue() {
		t.Fatalf("the later flip must win: %v %v", ok, f.GetValue())
	}
	if _, ok := store.GetNamespace("dodo", "old"); ok {
		t.Fatal("a retired namespace resurrected from the file")
	}

	// garbage in the middle refuses the whole store
	if err := os.WriteFile(path, append([]byte("this is not a record\n"), mustRead(t, path)...), 0o600); err != nil {
		t.Fatal(err)
	}
	store2 := openStore(t, t.TempDir())
	defer store2.Close()
	r, err = ReplayFile(
		context.Background(),
		path,
		NewLogger(),
		store2.ApplyReplayed,
		func(sv, v string) error { _, err := store2.DeleteNamespace(sv, v); return err },
	)
	if err == nil || !strings.Contains(err.Error(), "skipped 1 of 6") {
		t.Fatalf("garbage was not refused: n=%d err=%v", r.Applied, err)
	}
}

// TestReplayIgnoresAPartialLastLine: a process killed mid-write leaves a line
// without its newline; that is not a record and does not refuse the store.
func TestReplayIgnoresAPartialLastLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flipr.oplog")
	s, _ := NewFileSink(path, "g", NewRegistry())
	if err := s.Write(map[string]any{"op": "SetFlag", "service": "a", "version": "v", "key": "k", "value_json": `{"boolValue":true}`}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	f.WriteString(
		`{"op":"SetFlag","service":"a","version":"v","key":"k2","valueJson":"{\"boolVal`,
	)
	f.Close()
	store := openStore(t, t.TempDir())
	defer store.Close()
	r, err := ReplayFile(
		context.Background(),
		path,
		NewLogger(),
		store.ApplyReplayed,
		func(sv, v string) error { _, err := store.DeleteNamespace(sv, v); return err },
	)
	if err != nil || r.Applied != 1 {
		t.Fatalf(
			"applied=%d err=%v; the partial line must be ignored",
			r.Applied,
			err,
		)
	}
}

// TestAbsentOrEmptyFileRestoresNothingWithoutError: first boot.
func TestAbsentOrEmptyFileRestoresNothingWithoutError(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	defer store.Close()
	r, err := ReplayFile(
		context.Background(),
		filepath.Join(dir, "none.oplog"),
		NewLogger(),
		store.ApplyReplayed,
		nil,
	)
	if err != nil || r.Applied != 0 {
		t.Fatalf("absent: %d %v", r.Applied, err)
	}
	empty := filepath.Join(dir, "empty.oplog")
	os.WriteFile(empty, nil, 0o600)
	r, err = ReplayFile(
		context.Background(),
		empty,
		NewLogger(),
		store.ApplyReplayed,
		nil,
	)
	if err != nil || r.Applied != 0 {
		t.Fatalf("empty: %d %v", r.Applied, err)
	}
}

// TestTheMirrorNeverFailsAFlip: with the file as the record, a kafka mirror
// that refuses is counted and logged, and the write succeeds.
func TestTheMirrorNeverFailsAFlip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flipr.oplog")
	reg := NewRegistry()
	fs, _ := NewFileSink(path, "g", reg)
	defer fs.Close()
	tee := NewTeeSink(fs, &brokenSink{}, reg, NewLogger())
	if err := tee.Write(map[string]any{"op": "SetFlag", "service": "a", "version": "v", "key": "k", "value_json": `{"boolValue":true}`}); err != nil {
		t.Fatalf("a mirror failure failed the flip: %v", err)
	}
	if reg.Counter(mOplogMirrorErrs).Value() != 1 {
		t.Fatal("the mirror failure was not counted")
	}
	if b, _ := os.ReadFile(path); !strings.Contains(
		string(b),
		`"op":"SetFlag"`,
	) {
		t.Fatal("the record is not in the file")
	}
	// and a file failure IS returned: the record is the truth
	closed, _ := NewFileSink(
		filepath.Join(t.TempDir(), "x.oplog"),
		"g",
		NewRegistry(),
	)
	closed.Close()
	tee2 := NewTeeSink(closed, &captureSink{}, reg, NewLogger())
	if err := tee2.Write(map[string]any{"op": "SetFlag"}); err == nil {
		t.Fatal("a file failure must fail the write")
	}
}

// TestWipeRecoveryFromTheFile is the property the whole file exists for: the
// store directory is lost, the oplog file survives on the same volume, and a
// fresh store replays it to the same flags under a new generation.
func TestWipeRecoveryFromTheFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "flipr.oplog")
	store := openStore(t, dir)
	reg := NewRegistry()
	fs, _ := NewFileSink(path, store.Generation(), reg)
	l := NewOpLog(fs, reg, NewLogger())
	// the shape the handlers use: the record inside the write
	v := &pb.Value{Kind: &pb.Value_StringValue{StringValue: "surreal"}}
	if _, err := store.Flip("albatross", "v1", "nightly.smoothing", v, func(f *pb.Flag) error {
		return l.OpSync("SetFlag", map[string]any{"service": "albatross", "version": "v1", "key": "nightly.smoothing", "value_json": `{"stringValue":"surreal"}`, "reason": "r"})
	}); err != nil {
		t.Fatal(err)
	}
	fs.Close()
	gen1 := store.Generation()
	store.Close()

	// the wipe: the store file is gone, the oplog file is not
	if err := os.Remove(filepath.Join(dir, "flipr.db")); err != nil {
		t.Fatal(err)
	}
	fresh := openStore(t, dir)
	defer fresh.Close()
	if !fresh.FreshGeneration() || fresh.Generation() == gen1 {
		t.Fatal("the reopened store should be a fresh generation")
	}
	r, err := ReplayFile(
		context.Background(),
		path,
		NewLogger(),
		fresh.ApplyReplayed,
		func(sv, v string) error { _, err := fresh.DeleteNamespace(sv, v); return err },
	)
	if err != nil || r.Applied != 1 {
		t.Fatalf("applied=%d err=%v", r.Applied, err)
	}
	f, ok := fresh.GetFlag("albatross", "v1", "nightly.smoothing")
	if !ok || f.Value.GetStringValue() != "surreal" {
		t.Fatalf("the flip did not come back: %v %v", ok, f.GetValue())
	}
}

// TestReplayHonoursTheRestoresThatStand: the pure logic behind a heal after
// a restore. A file of r1..r4, a Restore at 5 undoing (2, 4], r6, and a
// second Restore at 7 undoing (4, 6] (the way back from the first) has the
// second standing and the first undone; unbounded replay applies 1, 2, 3,
// 4; bounded at 5 it honours the first restore and applies 1 and 2; bounded
// at 3 it sees no restore and applies 1, 2, 3.
func TestReplayHonoursTheRestoresThatStand(t *testing.T) {
	all := []restorePoint{
		{Stamp: 5, To: 2, From: 4},
		{Stamp: 7, To: 4, From: 6},
	}
	if got := standing(all, 0); len(got) != 1 || got[0].Stamp != 7 {
		t.Fatalf(
			"standing over the whole file: %v, want the second "+
				"only",
			got,
		)
	}
	if got := standing(all, 5); len(got) != 1 || got[0].Stamp != 5 {
		t.Fatalf("standing as of 5: %v, want the first only", got)
	}
	if got := standing(all, 3); len(got) != 0 {
		t.Fatalf("standing as of 3: %v, want none", got)
	}
	force := standing(all, 0)
	want := map[uint64]bool{
		1: false,
		2: false,
		3: false,
		4: false,
		5: true,
		6: true,
		7: false,
	}
	for rev, undone := range want {
		if (undoneBy(force, rev) != nil) != undone {
			t.Fatalf(
				"revision %d undone=%v, want %v",
				rev,
				!undone,
				undone,
			)
		}
	}
	// standing walks the file by position, so a file whose Restore records
	// are out of stamp order (somebody edited it) is refused at the scan
	path := filepath.Join(t.TempDir(), "edited.oplog")
	fs, _ := NewFileSink(path, "g", NewRegistry())
	_ = fs.Write(
		map[string]any{
			"op":           "Restore",
			"revision":     uint64(7),
			"restore_to":   uint64(4),
			"restore_from": uint64(6),
		},
	)
	_ = fs.Write(
		map[string]any{
			"op":           "Restore",
			"revision":     uint64(5),
			"restore_to":   uint64(2),
			"restore_from": uint64(4),
		},
	)
	fs.Close()
	if _, err := scanOplog(context.Background(), path); err == nil ||
		!strings.Contains(err.Error(), "was edited") {
		t.Fatalf("an out-of-order file must be refused: %v", err)
	}
}

// mustRead reads a file or fails the test.
func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

var _ = errors.New

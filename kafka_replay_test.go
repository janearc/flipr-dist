package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/protobuf/proto"

	oplogpb "github.com/janearc/flipr-dist/gen/oplogpb"
)

// REPLAY, against an in-process fake broker (franz-go's kfake). Nothing here
// leaves the process. These are the regressions for two replay bugs: the
// review of 2026-09-03 drove exactly these shapes through a scratchpad copy
// and found replay ending early on a slow first fetch, and reporting success
// over records it had skipped.

const replayTopic = "flipr.oplog"

// frame wraps a payload the way blm/emit does: magic 0, a 4-byte schema id,
// one zero byte for the message index, then the protobuf.
func frame(payload []byte) []byte {
	out := make([]byte, 0, len(payload)+6)
	out = append(out, 0)
	var id [4]byte
	binary.BigEndian.PutUint32(id[:], 1)
	out = append(out, id[:]...)
	out = append(out, 0)
	return append(out, payload...)
}

// setRecord builds a framed SetFlag operation.
func setRecord(service, version, key, valueJSON string) []byte {
	b, _ := proto.Marshal(&oplogpb.Operation{
		Ts: time.Now().UTC().Format(time.RFC3339Nano), Op: "SetFlag",
		Service: service, Version: version, Key: key, Value: "x", ValueJson: valueJSON,
	})
	return frame(b)
}

// readRecord builds a framed GetFlag operation, the kind that dominates the
// topic and carries nothing to restore.
func readRecord(service, version, key string) []byte {
	b, _ := proto.Marshal(&oplogpb.Operation{
		Ts: time.Now().UTC().Format(time.RFC3339Nano), Op: "GetFlag",
		Service: service, Version: version, Key: key, Found: true,
	})
	return frame(b)
}

// deleteRecord builds a framed DeleteNamespace operation.
func deleteRecord(service, version string) []byte {
	b, _ := proto.Marshal(&oplogpb.Operation{
		Ts: time.Now().
			UTC().
			Format(time.RFC3339Nano),
		Op:      "DeleteNamespace",
		Service: service, Version: version, Reason: "test",
	})
	return frame(b)
}

// fakeTopic starts a one-broker fake cluster holding the records, in order,
// and returns its listen addresses. firstFetchDelay, when non-zero, holds the
// first Fetch response for that long: the cold-kafka shape.
func fakeTopic(
	t *testing.T,
	records [][]byte,
	firstFetchDelay time.Duration,
) []string {
	t.Helper()
	c, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(1, replayTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	if firstFetchDelay > 0 {
		// the hook is consumed by its first call, so only the first
		// fetch is slow; every later one answers at once, as a warmed
		// broker does
		c.ControlKey(
			int16(kmsg.Fetch),
			func(kmsg.Request) (kmsg.Response, error, bool) {
				time.Sleep(firstFetchDelay)
				return nil, nil, false
			},
		)
	}
	if len(records) == 0 {
		return c.ListenAddrs()
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(replayTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx := context.Background()
	var batch []*kgo.Record
	for i, r := range records {
		batch = append(batch, &kgo.Record{Value: r})
		if len(batch) == 2000 || i == len(records)-1 {
			if err := cl.ProduceSync(ctx, batch...).FirstErr(); err != nil {
				t.Fatal(err)
			}
			batch = nil
		}
	}
	return c.ListenAddrs()
}

// runReplay replays the fake topic into a fresh store.
func runReplay(t *testing.T, brokers []string) (int, *Store, error) {
	t.Helper()
	store := openStore(t, t.TempDir())
	t.Cleanup(func() { store.Close() })
	n, err := Replay(
		context.Background(),
		brokers,
		replayTopic,
		NewLogger(),
		store.ApplyReplayed,
		func(s, v string) error { _, err := store.DeleteNamespace(s, v); return err },
	)
	return n, store, err
}

// TestReplayReadsToTheEndOffsetNotAQuietSecond: a broker that
// takes longer than a second to answer its first fetch used to make replay
// return success with nothing restored. The end offset is the finish line.
func TestReplayReadsToTheEndOffsetNotAQuietSecond(t *testing.T) {
	recs := make([][]byte, 0, 100)
	for i := 0; i < 100; i++ {
		recs = append(
			recs,
			setRecord(
				"svc",
				"v",
				fmt.Sprintf("k%02d", i),
				`{"boolValue":true}`,
			),
		)
	}
	brokers := fakeTopic(t, recs, 1500*time.Millisecond)
	n, store, err := runReplay(t, brokers)
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatalf(
			"applied %d of 100 flips: replay ended before the "+
				"end offset",
			n,
		)
	}
	if _, ok := store.GetFlag("svc", "v", "k99"); !ok {
		t.Fatal("the last flip in the log is missing from the store")
	}
}

// TestReplayRefusesAStoreMissingRecords: two flips with garbage
// between them and one flip whose value cannot be applied. Replay must not
// call that a success.
func TestReplayRefusesAStoreMissingRecords(t *testing.T) {
	recs := [][]byte{
		setRecord("a", "v", "k1", `{"boolValue":false}`),
		[]byte("this is not a record"),
		frame([]byte{0xff, 0xff, 0xff}),
		setRecord("a", "v", "k2", `{"boolValue":false}`),
		setRecord("a", "v", "k3", `not json`),
	}
	brokers := fakeTopic(t, recs, 0)
	n, _, err := runReplay(t, brokers)
	if err == nil {
		t.Fatal("replay skipped three records and reported success")
	}
	if !strings.Contains(err.Error(), "skipped 3 of 5") {
		t.Fatalf("the refusal does not say what was skipped: %v", err)
	}
	if n != 2 {
		t.Fatalf("applied = %d, want the 2 good flips", n)
	}
}

// TestReplayOfAnEmptyTopicIsImmediate: a topic with nothing in it restores
// nothing, quickly, without error. First boot looks like this.
func TestReplayOfAnEmptyTopicIsImmediate(t *testing.T) {
	brokers := fakeTopic(t, nil, 0)
	start := time.Now()
	n, _, err := runReplay(t, brokers)
	if err != nil || n != 0 {
		t.Fatalf("empty topic: applied=%d err=%v", n, err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("empty replay took %s", time.Since(start))
	}
}

// TestReplayAppliesDeletesInOrder: a namespace retired after its flips stays
// retired through a wipe-recovery, and one flipped after that comes back.
func TestReplayAppliesDeletesInOrder(t *testing.T) {
	recs := [][]byte{
		setRecord("dodo", "old", "k1", `{"boolValue":true}`),
		deleteRecord("dodo", "old"),
		setRecord("dodo", "new", "k2", `{"boolValue":true}`),
	}
	brokers := fakeTopic(t, recs, 0)
	n, store, err := runReplay(t, brokers)
	if err != nil || n != 3 {
		t.Fatalf("applied=%d err=%v", n, err)
	}
	if _, ok := store.GetNamespace("dodo", "old"); ok {
		t.Fatal("a retired namespace resurrected from the log")
	}
	if _, ok := store.GetFlag("dodo", "new", "k2"); !ok {
		t.Fatal("the flip after the delete is missing")
	}
}

// TestReplayWalksPastReadRecords: the topic is mostly reads, which carry
// nothing to restore and must not count as skipped.
func TestReplayWalksPastReadRecords(t *testing.T) {
	recs := make([][]byte, 0, 5005)
	for i := 0; i < 5000; i++ {
		recs = append(recs, readRecord("svc", "v", "flag"))
		if i%1000 == 0 {
			recs = append(
				recs,
				setRecord(
					"svc",
					"v",
					fmt.Sprintf("k%d", i),
					`{"intValue":"7"}`,
				),
			)
		}
	}
	brokers := fakeTopic(t, recs, 0)
	n, _, err := runReplay(t, brokers)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("applied %d, want the 5 flips among 5000 reads", n)
	}
}

// TestReplayDeadlineGrowsWithTheLog: a constant 30 seconds
// refused a log of 4000 flips that replay could have restored. The deadline
// is arithmetic on what the broker reports.
func TestReplayDeadlineGrowsWithTheLog(t *testing.T) {
	if got := replayDeadline(0); got != replayBase {
		t.Fatalf("empty log: %s", got)
	}
	if got := replayDeadline(4000); got <= 30*time.Second {
		t.Fatalf("4000 records: %s, the constant that failed", got)
	}
	if got, want := replayDeadline(200000), replayBase+2000*time.Second; got != want {
		t.Fatalf("200000 records: %s, want %s", got, want)
	}
}

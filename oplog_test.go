package main

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// captureSink collects entries in memory so a test can assert on them.
type captureSink struct {
	mu       sync.Mutex
	entries  []map[string]any
	failNext bool
}

// Write records an entry, or returns an error once when armed to fail.
func (c *captureSink) Write(e map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failNext {
		c.failNext = false
		return errors.New("sink is broken")
	}
	c.entries = append(c.entries, e)
	return nil
}

// count returns how many entries were captured.
func (c *captureSink) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// snapshot returns a copy of the entries captured so far. The oplog's
// goroutine appends while a test reads, so a reader that takes the slice
// without the lock is a data race; every reader goes through
// here.
func (c *captureSink) snapshot() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]map[string]any, len(c.entries))
	copy(out, c.entries)
	return out
}

// TestOpSyncIsDurableBeforeItReturns is the property the whole write path
// exists for: a buffered entry does not survive kill -9, so a flip that was
// acknowledged must already be in the sink when the call returns.
func TestOpSyncIsDurableBeforeItReturns(t *testing.T) {
	sink := &captureSink{}
	reg := NewRegistry()
	l := NewOpLog(sink, reg, NewLogger())
	if err := l.OpSync("SetFlag", map[string]any{"key": "k"}); err != nil {
		t.Fatal(err)
	}
	// No drain, no wait: it must already be there.
	if sink.count() != 1 {
		t.Fatalf(
			"OpSync returned before the entry reached the sink: "+
				"%d entries",
			sink.count(),
		)
	}
	if got := reg.Counter(mOplogEntries, "sync").Value(); got != 1 {
		t.Errorf("sync counter = %d, want 1", got)
	}
}

// TestOpSyncReportsSinkFailure checks a broken sink surfaces as an error
// rather than being swallowed, because the caller must not report a flip as
// logged when it was not.
func TestOpSyncReportsSinkFailure(t *testing.T) {
	sink := &captureSink{failNext: true}
	reg := NewRegistry()
	l := NewOpLog(sink, reg, NewLogger())
	if err := l.OpSync("SetFlag", nil); err == nil {
		t.Fatal("want an error from a failing sink")
	}
	if got := reg.Counter(mOplogSinkErrs).Value(); got != 1 {
		t.Errorf("sink error counter = %d, want 1", got)
	}
}

// TestEntryCarriesTimestampAndOp checks the shape of what lands in the log.
func TestEntryCarriesTimestampAndOp(t *testing.T) {
	l := NewOpLog(&captureSink{}, NewRegistry(), NewLogger())
	e := l.entry("SetFlag", map[string]any{"key": "k"})
	if e["op"] != "SetFlag" || e["ts"] == "" || e["key"] != "k" {
		t.Fatalf("bad entry: %+v", e)
	}
}

// TestStdoutSinkWrites checks the default sink marshals without error.
func TestStdoutSinkWrites(t *testing.T) {
	if err := newStdoutSink().Write(map[string]any{"op": "test"}); err != nil {
		t.Fatal(err)
	}
}

// slowSink takes a while per write, standing in for a kafka that answers but
// cannot keep up.
type slowSink struct {
	mu    sync.Mutex
	per   time.Duration
	wrote int
}

// Write sleeps, then counts.
func (s *slowSink) Write(map[string]any) error {
	time.Sleep(s.per)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wrote++
	return nil
}

// TestFlipsAreNeverDropped: the write path waits for the sink however slow
// it is, because a flip nobody recorded is the failure this service exists
// to prevent. Reads no longer touch the sink at all.
func TestFlipsAreNeverDropped(t *testing.T) {
	sink := &slowSink{per: 150 * time.Millisecond}
	reg := NewRegistry()
	l := NewOpLog(sink, reg, NewLogger())
	start := time.Now()
	if err := l.OpSync("SetFlag", map[string]any{"key": "k"}); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 150*time.Millisecond {
		t.Fatal(
			"OpSync returned before the slow sink accepted the " +
				"record",
		)
	}
}

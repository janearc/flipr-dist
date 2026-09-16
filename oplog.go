package main

import (
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// The oplog is the record of what flipr changed: one record per flip,
// written synchronously and durable before the caller is answered.
//
// Reads are counted in the request metrics and never recorded here. Every
// read was a record once, buffered and dropped under pressure, and the file
// grew twenty kilobytes in three minutes on a quiet cluster with nothing
// flipped.
//
// Replay walks the whole record, so a record that holds reads is a recovery
// that gets slower for every dashboard that polls. Decided the same day: the
// oplog holds mutations only, and there is no buffer to fill, no read to drop
// and no hole in the audit of reads because there is no audit of reads.
//
// The sink is an interface: the file beside the store is the record, kafka a
// mirror while the bus exists, stdout when nothing is configured.
type Sink interface {
	Write(entry map[string]any) error
}

type stdoutSink struct {
	mu  sync.Mutex
	enc *json.Encoder
}

// newStdoutSink writes one JSON object per line to stdout.
func newStdoutSink() *stdoutSink {
	return &stdoutSink{enc: json.NewEncoder(os.Stdout)}
}

// Write emits one entry, serialised against concurrent writers.
func (s *stdoutSink) Write(e map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enc.Encode(e)
}

type OpLog struct {
	sink Sink
	reg  *Registry
	log  *Logger

	// lastErr is when the sink last refused a write, as unix nanoseconds,
	// or zero. /health reads it: a sink that failed within the last minute
	// is a degraded flipr, whatever the store says.
	lastErr atomic.Int64
}

// sinkErrorWindow is how long after a sink failure /health keeps saying so.
const sinkErrorWindow = time.Minute

// noteSinkError counts a failed write and remembers when.
func (l *OpLog) noteSinkError() {
	l.reg.Counter(mOplogSinkErrs).Inc()
	l.lastErr.Store(time.Now().UnixNano())
}

// SinkFailedRecently reports whether the sink refused a write inside the
// window, and when.
func (l *OpLog) SinkFailedRecently() (bool, time.Time) {
	ns := l.lastErr.Load()
	if ns == 0 {
		return false, time.Time{}
	}
	at := time.Unix(0, ns)
	return time.Since(at) < sinkErrorWindow, at
}

// NewOpLog returns a log that writes every record through to the sink.
func NewOpLog(sink Sink, reg *Registry, lg *Logger) *OpLog {
	return &OpLog{sink: sink, reg: reg, log: lg}
}

// entry stamps an operation with a timestamp and its fields.
func (l *OpLog) entry(op string, fields map[string]any) map[string]any {
	e := map[string]any{
		"ts": time.Now().UTC().Format(time.RFC3339Nano),
		"op": op,
	}
	for k, v := range fields {
		e[k] = v
	}
	return e
}

// OpSync is the write path, and the only path: the entry is durable in the sink
// before the caller returns a response.
//
// A flip acknowledged to a caller and then lost from the record is exactly the
// unexplainable event this service was built after, and writes are rare by
// nature, so paying for them synchronously costs nothing anyone can measure.
func (l *OpLog) OpSync(op string, fields map[string]any) error {
	err := l.sink.Write(l.entry(op, fields))
	if err != nil {
		l.noteSinkError()
		return err
	}
	l.reg.Counter(mOplogEntries, "sync").Inc()
	return nil
}

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// The operational log -- flipr's syslog, in this estate's sense of the word.
//
// Structured JSON to stdout, which is the fleet's transport: k3s writes
// container stdout to the node's log files, and the cluster's log pipeline
// tails those into kafka and out to the host. The cold order puts logstash
// ahead of flipr, so these lines are collected from flipr's first breath.
//
// No syslogd, no file handling, no rotation -- that is the pipeline's job, and
// doing it here too would be two ways to do one thing.
//
// Two streams, deliberately, do NOT merge them:
//
//   the oplog (oplog.go)  what flipr did. Every operation, 100%, bound for
//                          kafka, replayable. It is an audit and a commit.
//   this logger            how flipr is. Rejections, reloads, sink trouble,
//                          store births. It is diagnostics, and its coverage
//                          rule: logging goes wherever
//                          there is instrumentation. If a counter can move,
//                          a line can say why.

// Logger is a leveled structured logger. Zero dependencies, one mutex, JSON
// lines. Boring on purpose.
type Logger struct {
	mu    sync.Mutex
	out   *json.Encoder
	debug atomic.Bool
}

// NewLogger builds the process logger. Debug lines are gated by FLIPR_DEBUG,
// everything else always emits -- a service this central does not get quiet
// modes for warnings.
func NewLogger() *Logger {
	l := &Logger{out: json.NewEncoder(os.Stdout)}
	l.debug.Store(os.Getenv("FLIPR_DEBUG") != "")
	return l
}

// SetDebugFromFlag follows the log.level flag: "debug" opens the gate, any
// other set value closes it, and an empty value (no flag anywhere) leaves the
// environment's choice standing rather than fighting it.
func (l *Logger) SetDebugFromFlag(level string) {
	switch level {
	case "debug":
		l.debug.Store(true)
	case "":
		// no flag declared: the FLIPR_DEBUG environment default stands
	default:
		l.debug.Store(false)
	}
}

// line emits one record. Fields ride flat beside the standard keys.
func (l *Logger) line(level, msg string, fields map[string]any) {
	rec := map[string]any{
		"ts":    time.Now().UTC().Format(time.RFC3339Nano),
		"level": level,
		"msg":   msg,
	}
	for k, v := range fields {
		rec[k] = v
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.out.Encode(rec); err != nil {
		// the logger must never take the service down; say so on stderr
		// and carry on serving
		fmt.Fprintf(
			os.Stderr,
			`{"level":"error",`+
				`"msg":"logger encode failed","err":%q}`+"\n",
			err.Error(),
		)
	}
}

// Info reports normal operation worth a line.
func (l *Logger) Info(
	msg string,
	fields map[string]any,
) {
	l.line("info", msg, fields)
}

// Warn reports something degraded that the service survived.
func (l *Logger) Warn(
	msg string,
	fields map[string]any,
) {
	l.line("warn", msg, fields)
}

// Error reports a failure a caller or operator will feel.
func (l *Logger) Error(
	msg string,
	fields map[string]any,
) {
	l.line("error", msg, fields)
}

// Debug reports detail, only when FLIPR_DEBUG is set.
func (l *Logger) Debug(msg string, fields map[string]any) {
	if l.debug.Load() {
		l.line("debug", msg, fields)
	}
}

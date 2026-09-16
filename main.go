package main

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// flipr. Small on purpose.
//
// It has to be simple, robust and fast, and it cannot go down, so it is the
// smallest, tightest code that does the job, written as though the entire
// mesh depends on it, because it does.
//
// This file owns the process: flags, listener, signals. The behaviour lives in
// server.go, which is drivable by a test without any of that.

// descriptor is flipr's own contract, embedded so /api can serve it. Built by
// buf from the same source the Go types were generated from.
//
//go:embed descriptor.binpb
var descriptor []byte

// Version is the commit hash this binary was built from, injected by the build
// (-ldflags -X).
//
// One version identity, not two: the pod's image tag, the namespace scheme's
// version key, and what Ping reports are all the same fact, which is what lets
// a client notice it is talking to something it does not expect. "dev" only
// ever appears from a bare `go build`, which is not a deployable artifact.
var Version = "dev"

// maxBodyBytes caps a request body. Flag payloads are tiny; anything near this
// is a mistake or an attack, and either way it should not be buffered.
const maxBodyBytes = 1 << 20

// main starts flipr and blocks until it is signalled to stop; `flipr ctl`
// is the operator's tool over the files (ctl.go).
func main() {
	if len(os.Args) > 1 && os.Args[1] == "ctl" {
		ctlMain(os.Args[2:])
		return
	}
	addr := flag.String(
		"addr",
		envOr("FLIPR_ADDR", "127.0.0.1:15100"),
		"listen address",
	)
	dbPath := flag.String(
		"db",
		envOr("FLIPR_DB", "flipr.db"),
		"path to the bbolt file",
	)
	brokers := flag.String(
		"kafka",
		envOr("FLIPR_KAFKA_BROKERS", ""),
		"kafka brokers for the oplog mirror (empty: no mirror)",
	)
	topic := flag.String(
		"topic",
		envOr("FLIPR_OPLOG_TOPIC", "flipr.oplog"),
		"oplog topic on the mirror",
	)
	oplogPath := flag.String(
		"oplog",
		envOr("FLIPR_OPLOG_FILE", "-"),
		"the oplog file (\"-\": <db>.oplog beside the store; \"\": "+
			"none, stdout)",
	)
	flag.Parse()
	if *oplogPath == "-" {
		*oplogPath = *dbPath + ".oplog"
	}

	started := time.Now()
	inst := NewInstruments(started)
	lg := NewLogger()

	store, err := OpenStore(*dbPath, inst.Registry(), lg)
	if err != nil {
		// Fail loudly at boot. A flipr that starts without its store
		// would answer "no such flag" to everything, which reads as
		// "off" and would silently turn the mesh off.
		fatal("cannot open store", err)
	}
	defer store.Close()

	// The oplog's home is a file beside the store (decided 2026-09-05; the
	// bus is being retired). While kafka still exists it is a mirror:
	//
	// written second, never the reason a flip fails. With no file
	// configured the record goes to stdout, which keeps a bare local run
	// honest.
	//
	// The second half of the wipe self-heal. A fresh generation means the
	// store lost everything; the oplog holds every operator flip that ever
	// reached it, so re-apply them before the listener opens.
	//
	// The file is preferred when it exists and holds records; the mirror is
	// the source only for a store that predates the file. Declared defaults
	// are the clients' half; this is flipr's.
	var sink Sink = newStdoutSink()
	var fileSink *FileSink
	if *oplogPath != "" {
		fs, err := NewFileSink(
			*oplogPath,
			store.Generation(),
			inst.Registry(),
		)
		if err != nil {
			fatal("cannot open the oplog file", err)
		}
		defer fs.Close()
		fileSink = fs
		sink = fs
	}
	if *brokers != "" {
		registry := envOr(
			"FLIPR_SCHEMA_REGISTRY",
			"http://schema-registry:8081",
		)
		ks, err := NewKafkaSink(
			context.Background(),
			splitCSV(*brokers),
			registry,
			*topic,
			store.Generation(),
			lg,
		)
		if err != nil {
			// Fail loudly at boot: a flipr that silently downgraded
			// its audit would break the 100%-to-the-record
			// requirement invisibly.
			fatal("kafka sink refused to construct", err)
		}
		defer ks.Close()
		if fileSink != nil {
			sink = NewTeeSink(fileSink, ks, inst.Registry(), lg)
		} else {
			sink = ks
		}
	}
	if store.FreshGeneration() {
		fileHasRecords := false
		if *oplogPath != "" {
			if fi, err := os.Stat(*oplogPath); err == nil &&
				fi.Size() > 0 {
				fileHasRecords = true
			}
		}
		switch {
		case fileHasRecords:
			lg.Warn(
				"fresh store with an oplog file present: "+
					"replaying the file to restore "+
					"operator flips",
				map[string]any{"path": *oplogPath},
			)
			r, err := HealFromFile(
				context.Background(),
				store,
				*oplogPath,
				lg,
			)
			if err != nil {
				fatal(
					"oplog replay failed; refusing to "+
						"serve a half-restored store",
					err,
				)
			}
			lg.Info(
				"oplog replay complete",
				map[string]any{
					"flips_restored": r.Applied,
					"revision":       r.Highest,
					"source":         "file",
				},
			)
		case *brokers != "":
			lg.Warn(
				"fresh store with kafka configured and no "+
					"oplog file: replaying the topic to "+
					"restore operator flips",
				nil,
			)
			n, err := Replay(
				context.Background(),
				splitCSV(*brokers),
				*topic,
				lg,
				store.ApplyReplayed,
				func(service, version string) error {
					_, err := store.DeleteNamespace(
						service, version,
					)
					return err
				},
			)
			if err != nil {
				fatal(
					"oplog replay failed; refusing to "+
						"serve a half-restored store",
					err,
				)
			}
			lg.Info(
				"oplog replay complete",
				map[string]any{
					"flips_restored": n,
					"source":         "kafka",
				},
			)
		default:
			lg.Warn(
				"fresh store with no oplog to replay: "+
					"serving declared defaults only",
				nil,
			)
		}
	}
	oplog := NewOpLog(sink, inst.Registry(), lg)
	// flipr's own namespace, flipr@clients, declared like any service's:
	// idempotent, recorded, an operator's values kept (clients.go)
	if err := declareClients(store, oplog); err != nil {
		fatal("declaring flipr@clients", err)
	}
	// the lock file beside the store: a person's word, honoured within a
	// second, recorded in the oplog when it changes (lock.go)
	locks := &lockWatch{
		path: lockPath(*dbPath),
		log:  lg,
		rec: func(op string, l *Lock) {
			_ = oplog.OpSync(op, map[string]any{
				"service": strings.Join(
					l.Scopes,
					",",
				),
				"reason":   l.Reason,
				"caller":   "flipr ctl:" + l.By,
				"revision": store.Revision(),
			})
		},
	}

	srv := &http.Server{
		Addr: *addr,
		Handler: newServerLocked(
			store,
			oplog,
			inst,
			lg,
			locks,
		).routes(),
		// Bounded so one stalled client cannot hold a connection
		// forever.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		// Startup time is a stated requirement ("very little startup
		// time"), so it is measured rather than assumed, and fixed here
		// rather than read at scrape time.
		inst.MarkServing()
		ns, flags, expensive := store.CountFlags()
		lg.Info("flipr listening", map[string]any{
			"addr": *addr, "db": *dbPath, "version": Version,
			"generation": store.Generation(),
			"namespaces": ns,
			"flags":      flags,
			"expensive":  expensive,
			"startup_us": time.Since(started).Microseconds(),
		})
		if err := srv.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			fatal("listen failed", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	lg.Info("flipr stopped", nil)
}

// splitCSV splits a comma-separated broker list, tolerating spaces.
func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// envOr returns the environment value for k, or def when it is unset or empty.
func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// fatal reports an unrecoverable startup problem and exits non-zero.
func fatal(msg string, err error) {
	fmt.Fprintf(
		os.Stderr,
		`{"level":"fatal","msg":%q,"err":%q}`+"\n",
		msg,
		err.Error(),
	)
	os.Exit(1)
}

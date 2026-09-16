package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"

	oplogpb "github.com/janearc/flipr-dist/gen/oplogpb"
)

// The oplog as a file. One protojson Operation per line, appended to a file
// beside the store, on the same volume that survives the node.
//
// It is the record of what flipr did and the replay source after a wipe, which
// is what the kafka topic was, without a JVM that has to be up before flipr can
// start, a six second publish timeout on every flip, a retention clock, a
// schema registry that lost 35 records, or a 9094 path. Decided 2026-09-05:
//
// the bus goes; this is what replaces it for flipr.
//
// Durability follows the operation. A flip (SetFlag, PublishNamespace,
// DeleteNamespace) is fsynced before Write returns, because those are the
// records a wipe-recovery needs and the caller is waiting.
//
// A read record is appended through a buffer and reaches the file on the next
// flip, on the periodic flush, or on close; a kill -9 can lose the last few
// read records, which is the same promise the async kafka path made, and the
// count that mattered (44) is unchanged.
//
// Total order is the file's order, under one mutex, which is the property
// replay depends on and the reason the topic had one partition.
//
// The revision is global to the file. Every write stamps its record with the
// revision it created; a fresh store that heals from the file continues from
// the highest stamp in it (main.go sets it after replay), and a restore takes
// the next number above everything it undid.
//
// So a number names one record forever and a restore can be named by one.
//
// A restore is a record, NOT AN EDIT. `flipr ctl restore --to-revision N`
// rebuilds the store from the records stamped at or below N and appends one
// Restore record carrying the range it undid, (N, from], as fields. Nothing is
// removed from the file.
//
// Replay honours the restores that still stand: a Restore is undone by a later
// Restore whose range holds its stamp (that is the way back: restore to the
// revision the first restore left), and the records inside a standing range are
// skipped.
//
// So a store healed after a restore comes back as the restore left it, and a
// replay bounded at a revision sees only the restores that had happened by
// then, which is the state as of that revision, exactly.
//
// Kafka stays as a mirror while it exists: TeeSink writes the file first,
// then the bus, and a bus failure is counted and logged rather than returned,
// because the file is the record now and a flip must not fail for a mirror.

// FileSink appends Operation records to a file.
type FileSink struct {
	mu         sync.Mutex
	f          *os.File
	w          *bufio.Writer
	path       string
	generation string
	reg        *Registry
}

// NewFileSink opens (or creates, 0600) the oplog file for appending.
func NewFileSink(path, generation string, reg *Registry) (*FileSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("oplog file: %w", err)
	}
	s := &FileSink{
		f:          f,
		w:          bufio.NewWriterSize(f, 64<<10),
		path:       path,
		generation: generation,
		reg:        reg,
	}
	reg.DeclareGauge(
		"flipr_oplog_file_bytes",
		"Size of the oplog file on disk.",
		func() int64 {
			fi, err := os.Stat(path)
			if err != nil {
				return -1
			}
			return fi.Size()
		},
	)
	return s, nil
}

// Write appends one record, flushed and fsynced before it returns.
func (s *FileSink) Write(e map[string]any) error {
	line, err := protojson.Marshal(recordFromEntry(e, s.generation))
	if err != nil {
		return fmt.Errorf("oplog file: encode: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.w.Write(line); err != nil {
		return fmt.Errorf("oplog file: write: %w", err)
	}
	if err := s.w.WriteByte('\n'); err != nil {
		return fmt.Errorf("oplog file: write: %w", err)
	}
	// every record is a flip: flushed and fsynced before the
	// caller is answered, so a kill -9 after the answer loses nothing
	if err := s.w.Flush(); err != nil {
		return fmt.Errorf("oplog file: flush: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("oplog file: fsync: %w", err)
	}
	return nil
}

// Flush pushes buffered bytes to the file without an fsync. Every Write
// flushes and fsyncs itself, so this matters only to Close.
func (s *FileSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Flush()
}

// Close flushes, fsyncs and closes.
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.w.Flush(); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	return s.f.Close()
}

// Path is where the file is.
func (s *FileSink) Path() string { return s.path }

// TeeSink writes the file first and then a mirror. The mirror's failures are
// counted under flipr_oplog_mirror_errors_total and logged, never returned:
// the file is the record.
type TeeSink struct {
	file   Sink
	mirror Sink
	reg    *Registry
	log    *Logger
}

// NewTeeSink pairs the record with its mirror.
func NewTeeSink(file, mirror Sink, reg *Registry, lg *Logger) *TeeSink {
	return &TeeSink{file: file, mirror: mirror, reg: reg, log: lg}
}

// Write is the file's verdict; the mirror is best effort.
func (t *TeeSink) Write(e map[string]any) error {
	if err := t.file.Write(e); err != nil {
		return err
	}
	if err := t.mirror.Write(e); err != nil {
		t.reg.Counter(mOplogMirrorErrs).Inc()
		t.log.Warn(
			"oplog mirror write failed; the record is in the file",
			map[string]any{"err": err.Error(), "op": e["op"]},
		)
	}
	return nil
}

// recordFromEntry converts an oplog entry map into the contract message. One
// conversion for every sink, so the file and the bus carry the same record.
func recordFromEntry(e map[string]any, generation string) *oplogpb.Operation {
	str := func(key string) string {
		if v, ok := e[key].(string); ok {
			return v
		}
		return ""
	}
	op := &oplogpb.Operation{
		Ts:              str("ts"),
		Op:              str("op"),
		Service:         str("service"),
		Version:         str("version"),
		Key:             str("key"),
		Value:           str("value"),
		Reason:          str("reason"),
		ValueJson:       str("value_json"),
		Remote:          str("remote"),
		Caller:          str("caller"),
		FliprVersion:    Version,
		StoreGeneration: generation,
	}
	if b, ok := e["found"].(bool); ok {
		op.Found = b
	}
	if n, ok := e["revision"].(uint64); ok {
		op.Revision = n
	}
	if n, ok := e["restore_to"].(uint64); ok {
		op.RestoreTo = n
	}
	if n, ok := e["restore_from"].(uint64); ok {
		op.RestoreFrom = n
	}
	switch n := e["flags"].(type) {
	case int:
		op.Flags = int32(n)
	case int32:
		op.Flags = n
	}
	return op
}

// Replayed is what a replay reports: the flips it applied, and the highest
// revision stamped on any record in the file, bound or no bound, which is
// where a healed store's count continues from and the number a restore's
// own record goes above.
type Replayed struct {
	Applied int
	Highest uint64
}

// restorePoint is one Restore record: its own stamp and the range it undid.
type restorePoint struct {
	Stamp, To, From uint64
}

// oplogScan is the first pass over the file: every Restore record in file
// order, the highest stamp, the count of records that parse and the count
// that do not.
type oplogScan struct {
	Restores []restorePoint
	Highest  uint64
	Records  int
	Bad      int
}

// scanOplog reads the file once for what replay and `flipr ctl` need to know
// before applying anything. An absent file scans as empty; a line that does not
// parse is counted, not fatal, so the caller decides.
//
// Restore records must appear in the file in the order of their stamps, which
// they do when the tool wrote them, since each is numbered above everything
// before it; a file where they do not is a file somebody edited, and it is
// refused rather than guessed at, because `standing` walks them by position.
func scanOplog(ctx context.Context, path string) (oplogScan, error) {
	var out oplogScan
	err := eachRecord(
		ctx,
		path,
		func(int, error) { out.Bad++ },
		func(op *oplogpb.Operation) error {
			out.Records++
			if op.Revision > out.Highest {
				out.Highest = op.Revision
			}
			if op.Op == "Restore" {
				if n := len(out.Restores); n > 0 &&
					op.Revision <= out.Restores[n-1].Stamp {
					return fmt.Errorf(
						"oplog: Restore record %d "+
							"follows Restore "+
							"record %d in the "+
							"file; the file was "+
							"edited and is "+
							"refused",
						op.Revision,
						out.Restores[n-1].Stamp,
					)
				}
				out.Restores = append(
					out.Restores,
					restorePoint{
						Stamp: op.Revision,
						To:    op.RestoreTo,
						From:  op.RestoreFrom,
					},
				)
			}
			return nil
		},
	)
	return out, err
}

// standing picks, from the Restore records stamped at or below the bound
// (zero: all of them), the ones still in force. Latest first: the last
// restore always stands, and an earlier one stands unless a standing later
// one undid the range its stamp sits in.
func standing(all []restorePoint, bound uint64) []restorePoint {
	var out []restorePoint
	for i := len(all) - 1; i >= 0; i-- {
		r := all[i]
		if bound != 0 && r.Stamp > bound {
			continue
		}
		if undoneBy(out, r.Stamp) == nil {
			out = append(out, r)
		}
	}
	return out
}

// undoneBy reports the standing restore whose range holds the revision, or
// nil: a record so held was undone and replay skips it.
func undoneBy(standing []restorePoint, rev uint64) *restorePoint {
	for i := range standing {
		if standing[i].To < rev && rev <= standing[i].From {
			return &standing[i]
		}
	}
	return nil
}

// eachRecord walks the file's complete lines up to the size it had when the
// walk began, parsing each; an unparseable line is reported through `bad` (nil:
// it is an error) and the walk goes on, so the caller can count them and refuse
// the store afterwards rather than stop at the first.
//
// An absent or empty file walks nothing. A final line without its newline is a
// process that died mid-write, not a record: ignored with a warning.
func eachRecord(
	ctx context.Context,
	path string,
	bad func(line int, err error),
	each func(*oplogpb.Operation) error,
) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("oplog: open: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("oplog: stat: %w", err)
	}
	end := fi.Size()
	r := bufio.NewReaderSize(io.LimitReader(f, end), 1<<20)
	lineNo := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := r.ReadBytes('\n')
		if len(line) == 0 && err == io.EOF {
			return nil
		}
		lineNo++
		if err != nil && err != io.EOF {
			return fmt.Errorf(
				"oplog: read at line %d: %w",
				lineNo,
				err,
			)
		}
		if err == io.EOF && len(line) > 0 {
			return nil
		}
		var op oplogpb.Operation
		// game: allow width
		if err := protojson.Unmarshal(line[:len(line)-1], &op); err != nil {
			if bad == nil {
				return fmt.Errorf(
					"oplog: unparseable record at line "+
						"%d: %w",
					lineNo,
					err,
				)
			}
			bad(lineNo, err)
			continue
		}
		if err := each(&op); err != nil {
			return err
		}
	}
}

// HealFromFile is the wipe self-heal: replay the file onto a fresh store and
// continue the count from the highest stamp in it, so the writes that follow
// number above every record there is.
//
// The flips replayed bumped the count on their way in; without the last step a
// healed store would count its flips and stamp numbers the file already holds.
func HealFromFile(
	ctx context.Context,
	store *Store,
	path string,
	lg *Logger,
) (Replayed, error) {
	r, err := ReplayFile(
		ctx,
		path,
		lg,
		store.ApplyReplayed,
		func(service, version string) error {
			_, err := store.DeleteNamespace(service, version)
			return err
		},
	)
	if err != nil {
		return r, err
	}
	if err := store.SetRevision(r.Highest); err != nil {
		return r, fmt.Errorf(
			"oplog replay: setting the revision the file ends "+
				"at: %w",
			err,
		)
	}
	return r, nil
}

// ReplayFile applies every SetFlag and DeleteNamespace in the file, oldest
// first, up to the size the file had when replay began: the file's end offset,
// read once, is the finish line, as the topic's was. A record that cannot be
// parsed or applied makes replay fail, as with the topic (48).
//
// An absent or empty file restores nothing and is not an error: first boot.
//
// The caller sets the store's revision to Highest afterwards, so the count
// continues where the file left off.
func ReplayFile(ctx context.Context, path string, lg *Logger,
	apply func(service, version, key, value string, typed bool) error,
	remove func(service, version string) error) (Replayed, error) {
	return ReplayFileTo(ctx, path, 0, lg, apply, remove)
}

// ReplayFileTo is ReplayFile stopping at a revision: records stamped above `to`
// are not applied, and only the Restore records stamped at or below it are
// honoured, so the result is the store as it stood at revision `to`
// (RUNBOOK-restore.md). Zero means no bound.
//
// Records without a stamp (written before 2026-09-06) precede every stamped one
// and are applied.
func ReplayFileTo(ctx context.Context, path string, to uint64, lg *Logger,
	apply func(service, version, key, value string, typed bool) error,
	remove func(service, version string) error) (Replayed, error) {

	var out Replayed
	fi, err := os.Stat(path)
	if os.IsNotExist(err) {
		lg.Info(
			"oplog replay: no file; nothing to restore",
			map[string]any{"path": path},
		)
		return out, nil
	}
	if err != nil {
		return out, fmt.Errorf("oplog replay: stat: %w", err)
	}
	if fi.Size() == 0 {
		lg.Info(
			"oplog replay: the file is empty; nothing to restore",
			map[string]any{"path": path},
		)
		return out, nil
	}
	lg.Info(
		"oplog replay: reading the file to the end offset taken at "+
			"start",
		map[string]any{"path": path, "bytes": fi.Size()},
	)

	// pass one: the restores in force as of the bound, and the highest
	// stamp
	scan, err := scanOplog(ctx, path)
	if err != nil {
		return out, fmt.Errorf("oplog replay: %w", err)
	}
	out.Highest = scan.Highest
	force := standing(scan.Restores, to)
	for _, r := range force {
		lg.Info(
			"oplog replay: honouring a restore; the records it "+
				"undid are skipped",
			map[string]any{
				"restore": r.Stamp,
				"to":      r.To,
				"from":    r.From,
			},
		)
	}

	// pass two: apply
	skipped, seen := 0, 0
	err = eachRecord(ctx, path, func(line int, perr error) {
		seen++
		skipped++
		lg.Error(
			"oplog replay: unparseable record; the store will be "+
				"refused",
			map[string]any{"line": line, "err": perr.Error()},
		)
	}, func(op *oplogpb.Operation) error {
		seen++
		if to != 0 && op.Revision > to {
			// past the named point: the record stays in the file,
			// unapplied
			return nil
		}
		if r := undoneBy(force, op.Revision); r != nil {
			// undone by a restore that stands: the record stays,
			// unapplied
			return nil
		}
		switch op.Op {
		case "DeleteNamespace":
			if err := remove(op.Service, op.Version); err != nil {
				skipped++
				lg.Error(
					"oplog replay: delete failed; the "+
						"store will be refused",
					map[string]any{
						"service": op.Service,
						"version": op.Version,
						"err":     err.Error(),
					},
				)
				return nil
			}
			out.Applied++
		case "SetFlag":
			payload := op.ValueJson
			if payload == "" {
				payload = op.Value
			}
			// game: allow width
			if err := apply(op.Service, op.Version, op.Key, payload, op.ValueJson != ""); err != nil {
				skipped++
				lg.Error(
					"oplog replay: apply failed; the "+
						"store will be refused",
					map[string]any{
						"service": op.Service,
						"key":     op.Key,
						"err":     err.Error(),
					},
				)
				return nil
			}
			out.Applied++
		default:
			// reads, publishes, pings, locks and restores carry
			// nothing to apply
		}
		return nil
	})
	if err != nil {
		return out, fmt.Errorf("oplog replay: %w", err)
	}
	if skipped > 0 {
		return out, fmt.Errorf(
			"oplog replay skipped %d of %d records it could not "+
				"parse or apply; refusing to serve a store "+
				"that is missing them",
			skipped,
			seen,
		)
	}
	return out, nil
}

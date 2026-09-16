package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"

	"google.golang.org/protobuf/encoding/protojson"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	pb "github.com/janearc/flipr-dist/gen/fliprpb"
)

// The store is bbolt plus an in-memory snapshot, and the split is the whole
// performance story.
//
// bbolt is durability only. It is never on the read path. Every read is served
// from an immutable snapshot held in an atomic.Value, so a read takes a pointer
// load and a map lookup with no lock, no allocation and no disk.
//
// That is what makes "hundreds of calls per second, flipr just has to take it"
// uninteresting rather than a design constraint -- we are three orders of
// magnitude below where it gets hard.
//
// Writes are rare by nature (someone flips a flag) so they take the slow path:
// durable to bbolt first, then a fresh snapshot published. A write that has
// returned has hit the disk.
//
// bbolt rather than RocksDB. RocksDB in Go means cgo, and the store is not on
// the read path, so the cgo bought nothing measurable here.

var bucketFlags = []byte("flags")

// bucketMeta holds facts about the store itself, currently one: its
// generation. Minted when the file is created, never rewritten, so a store
// that was wiped and recreated wears a visibly different generation. That is
// how a wiped flipr tells every client the truth on their next read.
var bucketMeta = []byte("meta")
var keyGeneration = []byte("generation")

// keyRevision holds the store revision: bumped by one inside every write's
// own transaction, so it is exact, stamped into every oplog record, carried
// in every answer, and the number a restore names: the checkpoint. It is
// monotonic across the whole record, not one generation:
//
// a fresh store with nothing to replay starts at zero, and one that heals
// from the oplog file continues from the highest stamp in it (SetRevision,
// after replay), so a number names one record forever.
var keyRevision = []byte("revision")

// The global scope. The log level can be set for one application or for all
// of them, which means flipr has a global scope.
//
// The global scope is an ordinary namespace with a reserved name -- no special
// storage, no special write path, SetFlag and PublishNamespace work on it
// unchanged.
//
// What is special is reads: GetFlag falls back to the global scope when the
// service's own namespace lacks the key, and GetNamespace returns the service's
// flags overlaid on the global ones, so a client's cache carries the merged
// truth without the client knowing the mechanism exists.
//
// Resolution is server-side on purpose: every language's client gets identical
// precedence without reimplementing it, which is the same reason the wire
// refuses unknown fields server-side.
//
// Precedence: the service's own flag always wins over the global one.
//
// The version is a fixed sentinel because "all applications" transcends any
// deploy -- a global flag does not change meaning when one service redeploys.
const GlobalService = "_global"
const GlobalVersion = "_"

type nsKey struct {
	service string
	version string
}

// String renders a namespace key as it is stored: service@version.
func (n nsKey) String() string { return n.service + "@" + n.version }

// snapshot is immutable once published. Readers never lock; writers build a
// new one and swap it in.
type snapshot map[nsKey]map[string]*pb.Flag

type Store struct {
	db         *bolt.DB
	snap       atomic.Value // holds snapshot
	revision   atomic.Uint64
	reg        *Registry
	log        *Logger
	generation string
	fresh      bool

	// lastWriteErr is the most recent write failure the store could not
	// undo, or nil: a bbolt error on commit (the volume full, the file
	// unwritable), or a rollback that failed after an unlogged write. A
	// later successful write clears it. /health reads it.
	lastWriteErr atomic.Value // holds writeFailure

	// wmu orders writers. bbolt serialises the commits, but each writer
	// then rebuilds and publishes a snapshot, and nothing ordered one
	// writer's publish against another's commit.
	//
	// So A could open its read transaction before B committed and store
	// its older snapshot afterwards, and readers served B's flag at its
	// old value until the next write.
	//
	// Holding this across commit and reload makes the published snapshot
	// always the last commit's. Readers never take it.
	wmu sync.Mutex
}

// OpenStore opens (or creates) the bbolt file and loads the first snapshot.
func OpenStore(path string, reg *Registry, lg *Logger) (*Store, error) {
	// A short open timeout rather than the default forever: if another
	// flipr holds the file we want to fail loudly at boot, not hang looking
	// healthy.
	db, err := bolt.Open(
		path,
		0600,
		&bolt.Options{Timeout: 3 * time.Second},
	)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	var generation string
	var revision uint64
	fresh := false
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketFlags)
		if err != nil {
			return err
		}
		meta, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return err
		}
		// The generation is minted exactly once, at file creation, and
		// read forever after. A fresh file -- first boot OR a wipe, the
		// store cannot tell the difference and does not need to -- gets
		// a new one.
		if g := meta.Get(keyGeneration); g != nil {
			generation = string(g)
			if r := meta.Get(keyRevision); len(r) == 8 {
				revision = binary.BigEndian.Uint64(r)
			}
			return nil
		}
		fresh = true
		generation = fmt.Sprintf(
			"%d-%d", time.Now().UnixNano(), os.Getpid(),
		)
		// A fresh generation is either first boot or a wipe, and the
		// store cannot tell which -- so it says so loudly and lets the
		// reader decide. A wipe that only clients notice is a wipe the
		// operator reads about in someone else's incident.
		lg.Warn("store generation MINTED: this store is fresh "+
			"(first boot, or the previous store was wiped)",
			map[string]any{"generation": generation, "path": path})
		return meta.Put(keyGeneration, []byte(generation))
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating buckets: %w", err)
	}
	s := &Store{
		db:         db,
		reg:        reg,
		log:        lg,
		generation: generation,
		fresh:      fresh,
	}
	s.revision.Store(revision)
	if err := s.reload(); err != nil {
		db.Close()
		return nil, err
	}
	// Read at scrape time rather than pushed on every write: cheaper, and a
	// gauge computed from the live snapshot cannot go stale.
	reg.DeclareGauge("flipr_namespaces", "Namespaces held.",
		func() int64 { n, _, _ := s.CountFlags(); return int64(n) })
	reg.DeclareGauge("flipr_flags", "Flags held.",
		func() int64 { _, f, _ := s.CountFlags(); return int64(f) })
	reg.DeclareGauge(
		"flipr_expensive_flags",
		"Flags marked expensive. The emergency page is exactly this "+
			"set.",
		func() int64 { _, _, e := s.CountFlags(); return int64(e) },
	)
	reg.DeclareGauge(
		"flipr_db_size_bytes",
		"Size of the bbolt file on disk.",
		func() int64 {
			fi, err := os.Stat(path)
			if err != nil {
				// visible as wrong rather than plausibly zero
				return -1
			}
			return fi.Size()
		},
	)
	return s, nil
}

// Close releases the bbolt file.
func (s *Store) Close() error { return s.db.Close() }

// reload rebuilds the whole snapshot from disk. Called once at startup and
// after each write. Rebuilding everything rather than patching in place is
// deliberate: it is O(flags), flags are few, and it removes any chance of the
// snapshot and the disk disagreeing.
func (s *Store) reload() error {
	start := time.Now()
	next := snapshot{}
	err := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketFlags)
		if root == nil {
			return nil
		}
		return root.ForEachBucket(func(k []byte) error {
			ns := root.Bucket(k)
			key, err := parseNSKey(string(k))
			if err != nil {
				return err
			}
			flags := map[string]*pb.Flag{}
			if err := ns.ForEach(func(fk, fv []byte) error {
				if fv == nil {
					return nil // nested bucket, not a flag
				}
				var f pb.Flag
				if err := proto.Unmarshal(fv, &f); err != nil {
					return fmt.Errorf(
						"flag %s/%s: %w",
						key, fk, err,
					)
				}
				flags[string(fk)] = &f
				return nil
			}); err != nil {
				return err
			}
			next[key] = flags
			return nil
		})
	})
	if err != nil {
		return err
	}
	s.snap.Store(next)
	s.reg.Counter(mStoreReload).Inc()
	s.reg.Histogram(mStoreReloadS).ObserveSince(start)
	ns, flags, expensive := 0, 0, 0
	for _, f := range next {
		ns++
		flags += len(f)
		for _, fl := range f {
			if fl.Expensive {
				expensive++
			}
		}
	}
	// flipr eats its own dogfood: the logger's debug gate follows the
	// log.level flag -- flipr's own namespace winning over the global
	// scope, same precedence every other service gets.
	//
	// Read from the snapshot just built, so a SetFlag on log.level takes
	// effect on the very next line.
	s.log.SetDebugFromFlag(levelFrom(next))
	s.log.Debug("snapshot reloaded", map[string]any{
		"namespaces": ns, "flags": flags, "expensive": expensive,
		"took_us": time.Since(start).Microseconds(),
	})
	return nil
}

// current returns the snapshot readers are served from.
func (s *Store) current() snapshot { return s.snap.Load().(snapshot) }

// GetFlag serves one flag from the snapshot. No disk, no lock. A key absent
// from the service's own namespace falls back to the global scope, so "set it
// for all applications" is one write rather than one per service.
func (s *Store) GetFlag(service, version, key string) (*pb.Flag, bool) {
	defer s.reg.Histogram(mStoreReadS).ObserveSince(time.Now())
	cur := s.current()
	if flags, ok := cur[nsKey{service, version}]; ok {
		if f, ok := flags[key]; ok {
			return f, ok
		}
	}
	// the global scope answers for itself directly; everything else falls
	// back
	if service == GlobalService {
		return nil, false
	}
	if flags, ok := cur[nsKey{GlobalService, GlobalVersion}]; ok {
		f, ok := flags[key]
		return f, ok
	}
	return nil, false
}

// GetNamespace returns one service-at-a-version, its flags overlaid on the
// global scope -- so a client's startup cache carries the merged truth and
// check() on a globally-set key just works, in every language, with zero
// client-side precedence code.
func (s *Store) GetNamespace(service, version string) (*pb.Namespace, bool) {
	defer s.reg.Histogram(mStoreReadS).ObserveSince(time.Now())
	cur := s.current()
	flags, ok := cur[nsKey{service, version}]
	if !ok {
		return nil, false
	}
	if service == GlobalService {
		return &pb.Namespace{
			Service: service,
			Version: version,
			Flags:   sorted(flags),
		}, true
	}
	merged := map[string]*pb.Flag{}
	for k, f := range cur[nsKey{GlobalService, GlobalVersion}] {
		merged[k] = f
	}
	for k, f := range flags {
		merged[k] = f // the service's own flag always wins
	}
	return &pb.Namespace{
		Service: service,
		Version: version,
		Flags:   sorted(merged),
	}, true
}

// ListNamespaces returns every namespace held, for the home page.
func (s *Store) ListNamespaces() []*pb.Namespace {
	defer s.reg.Histogram(mStoreReadS).ObserveSince(time.Now())
	cur := s.current()
	out := make([]*pb.Namespace, 0, len(cur))
	for k, flags := range cur {
		out = append(
			out,
			&pb.Namespace{
				Service: k.service,
				Version: k.version,
				Flags:   sorted(flags),
			},
		)
	}
	sortNamespaces(out)
	return out
}

// SetFlag is the flip. Durable before it returns.
//
// The value and budget rules are checked inside the write transaction, against
// the flag as it is on disk at that moment, so two flips racing for the 24th
// slot or the same key cannot both pass a check taken earlier. A refusal is
// returned as a *Refusal and nothing was written.
func (s *Store) SetFlag(
	service, version, key string,
	v *pb.Value,
) (*pb.Flag, error) {
	return s.setFlag(service, version, key, v, true, nil)
}

// setFlag is SetFlag with the rules switchable.
//
// Replay turns them off: the op log is the record of what happened, and a flip
// that was legal when it was written (a 25th key, a retype, from before the
// rules existed) must come back exactly as it was, or a wipe-recovery would
// refuse to start on its own history. New flips always enforce.
func (s *Store) setFlag(
	service, version, key string,
	v *pb.Value,
	enforce bool,
	record func(*pb.Flag) error,
) (*pb.Flag, error) {
	defer s.reg.Histogram(mStoreWriteS).ObserveSince(time.Now())
	var out *pb.Flag
	err := s.transact(service, version, func(tx *bolt.Tx) error {
		ns, err := tx.Bucket(bucketFlags).
			CreateBucketIfNotExists(
				[]byte(nsKey{service, version}.String()),
			)
		if err != nil {
			return err
		}
		// Preserve description and expensive: a flip changes the value.
		// Those two are declared at publish time by the service that
		// owns the flag, and silently clearing them on a flip would
		// empty the emergency page the first time anyone used it.
		f := &pb.Flag{Key: key}
		raw := ns.Get([]byte(key))
		if raw != nil {
			if err := proto.Unmarshal(raw, f); err != nil {
				return err
			}
		}
		if enforce {
			var existing *pb.Flag
			if raw != nil {
				existing = f
			}
			if r := checkValue(existing, v); r != nil {
				return r
			}
			if raw == nil && ns.Stats().KeyN >= flagBudget {
				return refuse(
					"over_budget",
					"%s@%s already holds %d flags, "+
						"flipr's budget per "+
						"namespace; a flip may not "+
						"create a %dth",
					service,
					version,
					flagBudget,
					flagBudget+1,
				)
			}
		}
		f.Value = v
		f.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		enc, err := proto.Marshal(f)
		if err != nil {
			return err
		}
		out = f
		return ns.Put([]byte(key), enc)
	}, func() error {
		if record == nil {
			return nil
		}
		return record(out)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Flip is SetFlag with the oplog record written inside the write: record runs
// after the commit and the snapshot publish, and if it fails the flag is put
// back exactly as it was and the caller gets an *Unlogged.
//
// A flip that was acknowledged always left a trace, and a flip that left no
// trace never happened.
func (s *Store) Flip(
	service, version, key string,
	v *pb.Value,
	record func(*pb.Flag) error,
) (*pb.Flag, error) {
	return s.setFlag(service, version, key, v, true, record)
}

// PublishNamespace is build-time onboarding, and it is idempotent because a
// redeploy of the same commit republishes the same namespace.
//
// It does NOT clobber the value of a flag that already exists. A deploy
// declares which flags exist and what they mean; an operator decides what they
// are set to. If publishing reset values, every deploy would silently undo an
// emergency flip -- which is precisely when a deploy is most likely to happen.
func (s *Store) PublishNamespace(n *pb.Namespace) (int, error) {
	return s.Publish(n, nil)
}

// Publish is PublishNamespace with the oplog record written inside the
// write, rolled back if the record cannot be written. See Flip.
func (s *Store) Publish(n *pb.Namespace, record func(int) error) (int, error) {
	defer s.reg.Histogram(mStoreWriteS).ObserveSince(time.Now())
	count := 0
	err := s.transact(n.Service, n.Version, func(tx *bolt.Tx) error {
		ns, err := tx.Bucket(bucketFlags).
			CreateBucketIfNotExists(
				[]byte(nsKey{n.Service, n.Version}.String()),
			)
		if err != nil {
			return err
		}
		for _, f := range n.Flags {
			merged := &pb.Flag{
				Key:         f.Key,
				Value:       f.Value,
				Description: f.Description,
				Expensive:   f.Expensive,
				UpdatedAt: time.Now().
					UTC().
					Format(time.RFC3339Nano),
			}
			if raw := ns.Get([]byte(f.Key)); raw != nil {
				var existing pb.Flag
				err := proto.Unmarshal(raw, &existing)
				if err != nil {
					return err
				}
				// keep the operator's value, take the deploy's
				// metadata
				merged.Value = existing.Value
				merged.UpdatedAt = existing.UpdatedAt
			}
			raw, err := proto.Marshal(merged)
			if err != nil {
				return err
			}
			if err := ns.Put([]byte(f.Key), raw); err != nil {
				return err
			}
			count++
		}
		return nil
	}, func() error {
		if record == nil {
			return nil
		}
		return record(count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// levelFrom resolves the effective log.level for flipr itself from a
// snapshot: flipr's own namespace first, then the global scope, else "".
func levelFrom(snap snapshot) string {
	reserved := []nsKey{
		{"flipr", Version},
		{GlobalService, GlobalVersion},
	}
	for _, k := range reserved {
		if flags, ok := snap[k]; ok {
			if f, ok := flags["log.level"]; ok {
				return f.Value.GetStringValue()
			}
		}
	}
	return ""
}

// FreshGeneration reports whether THIS open minted the generation -- first
// boot or a wipe -- which is the condition for replaying the oplog.
func (s *Store) FreshGeneration() bool { return s.fresh }

// ApplyReplayed re-applies one operator flip from the immutable log during
// replay. It goes through SetFlag -- durable, metadata-preserving -- and the
// caller (Replay) runs before the oplog exists, so nothing re-logs and the
// topic cannot echo.
func (s *Store) ApplyReplayed(
	service, version, key, value string,
	typed bool,
) error {
	v := &pb.Value{}
	if typed {
		// value is protojson of flipr.v1.Value: the exact type survives
		if err := protojson.Unmarshal([]byte(value), v); err != nil {
			return fmt.Errorf("replay value_json: %w", err)
		}
		_, err := s.setFlag(service, version, key, v, false, nil)
		return err
	}
	switch value {
	case "true":
		v.Kind = &pb.Value_BoolValue{BoolValue: true}
	case "false":
		v.Kind = &pb.Value_BoolValue{BoolValue: false}
	default:
		if n, err := strconv.ParseInt(value, 10, 64); err == nil {
			v.Kind = &pb.Value_IntValue{IntValue: n}
		} else {
			v.Kind = &pb.Value_StringValue{StringValue: value}
		}
	}
	_, err := s.setFlag(service, version, key, v, false, nil)
	return err
}

// DeleteNamespace removes one service@version whole: the bucket, durably,
// then a fresh snapshot. The caller (the handler, or replay) owns writing
// the oplog record -- the store just makes it true.
func (s *Store) DeleteNamespace(service, version string) (int, error) {
	return s.Retire(service, version, nil)
}

// Retire is DeleteNamespace with the oplog record written inside the write,
// rolled back (the namespace and every flag in it restored) if the record
// cannot be written. See Flip.
func (s *Store) Retire(
	service, version string,
	record func(int) error,
) (int, error) {
	defer s.reg.Histogram(mStoreWriteS).ObserveSince(time.Now())
	removed := 0
	err := s.transact(service, version, func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketFlags)
		key := []byte(nsKey{service, version}.String())
		ns := root.Bucket(key)
		if ns == nil {
			// already absent: deleting nothing is not an error
			return nil
		}
		removed = ns.Stats().KeyN
		return root.DeleteBucket(key)
	}, func() error {
		if record == nil {
			return nil
		}
		return record(removed)
	})
	if err != nil {
		return 0, err
	}
	return removed, nil
}

// writeFailure is what lastWriteErr holds: the error and when.
type writeFailure struct {
	err error
	at  time.Time
}

// noteWriteFailure records a write the store could not complete or undo.
func (s *Store) noteWriteFailure(err error) {
	s.lastWriteErr.Store(writeFailure{err: err, at: time.Now()})
}

// LastWriteFailure reports the most recent write failure that a later write
// has not cleared, if any.
func (s *Store) LastWriteFailure() (writeFailure, bool) {
	v, ok := s.lastWriteErr.Load().(writeFailure)
	if !ok || v.err == nil {
		return writeFailure{}, false
	}
	return v, true
}

// Unlogged is a write that was committed and then could not be recorded in
// the oplog. RolledBack says whether the store was returned to what it was.
//
// When it was not, RollbackErr says why, and the store now serves a value the
// log does not hold, which is the one state this service must never be quiet
// about.
type Unlogged struct {
	Err         error
	RolledBack  bool
	RollbackErr error
}

// Error renders the failure for a caller.
func (u *Unlogged) Error() string {
	if u.RolledBack {
		return "could not be logged, so it was rolled back and did " +
			"not happen: " + u.Err.Error()
	}
	return "could not be logged AND could not be rolled back; the " +
		"store now holds a value the op log does not: " +
		u.Err.Error() + " (rollback: " + u.RollbackErr.Error() + ")"
}

// Unwrap exposes the oplog error.
func (u *Unlogged) Unwrap() error { return u.Err }

// transact is the ONE write path: under the writer lock, capture the namespace
// as it is, run the mutation durably, publish the snapshot, then write the
// oplog record.
//
// If the record cannot be written the namespace is put back byte for byte and
// the snapshot republished, so that a caller told "failed" is telling the truth
// and readers never keep serving a value the audit does not hold.
//
// Before this, the flip stayed in the store, the caller got a 500, and a client
// retry committed it twice.
func (s *Store) transact(
	service, version string,
	mutate func(tx *bolt.Tx) error,
	record func() error,
) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	key := []byte(nsKey{service, version}.String())
	var prior map[string][]byte
	var existed bool
	var next uint64
	if err := s.db.Update(func(tx *bolt.Tx) error {
		prior, existed = captureNamespace(tx, key)
		if err := mutate(tx); err != nil {
			return err
		}
		// the revision moves with the write, inside its transaction: a
		// commit and its number are one fact
		next = s.revision.Load() + 1
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], next)
		return tx.Bucket(bucketMeta).Put(keyRevision, b[:])
	}); err != nil {
		// A refusal is the caller's mistake and the store is fine;
		// anything else is bbolt saying it could not commit, which
		// /health must say.
		var ref *Refusal
		if !errors.As(err, &ref) {
			s.noteWriteFailure(err)
		}
		return err
	}
	s.revision.Store(next)
	if err := s.reload(); err != nil {
		s.noteWriteFailure(err)
		return err
	}
	// a write that committed clears the last failure: the store can write
	// again
	s.lastWriteErr.Store(writeFailure{})
	if record == nil {
		return nil
	}
	err := record()
	if err == nil {
		return nil
	}
	u := &Unlogged{Err: err}
	if rerr := s.db.Update(func(tx *bolt.Tx) error {
		return restoreNamespace(tx, key, prior, existed)
	}); rerr != nil {
		u.RollbackErr = rerr
	} else if rerr := s.reload(); rerr != nil {
		u.RollbackErr = rerr
	} else {
		u.RolledBack = true
		s.reg.Counter(mStoreRollbacks).Inc()
	}
	if !u.RolledBack {
		s.noteWriteFailure(u)
		s.log.Error(
			"a write could not be logged AND could not be rolled "+
				"back; the store now serves a value the op "+
				"log does not hold",
			map[string]any{
				"service":      service,
				"version":      version,
				"err":          err.Error(),
				"rollback_err": u.RollbackErr.Error(),
			},
		)
	}
	return u
}

// captureNamespace copies one namespace's flags exactly as stored, so a
// write can be undone byte for byte. existed is false when there was no
// such namespace, in which case undoing means removing it.
func captureNamespace(tx *bolt.Tx, key []byte) (map[string][]byte, bool) {
	ns := tx.Bucket(bucketFlags).Bucket(key)
	if ns == nil {
		return nil, false
	}
	out := map[string][]byte{}
	_ = ns.ForEach(func(k, v []byte) error {
		if v != nil {
			out[string(k)] = append([]byte(nil), v...)
		}
		return nil
	})
	return out, true
}

// restoreNamespace puts a namespace back exactly as captureNamespace saw it.
func restoreNamespace(
	tx *bolt.Tx,
	key []byte,
	prior map[string][]byte,
	existed bool,
) error {
	root := tx.Bucket(bucketFlags)
	if root.Bucket(key) != nil {
		if err := root.DeleteBucket(key); err != nil {
			return err
		}
	}
	if !existed {
		return nil
	}
	ns, err := root.CreateBucket(key)
	if err != nil {
		return err
	}
	for k, v := range prior {
		if err := ns.Put([]byte(k), v); err != nil {
			return err
		}
	}
	return nil
}

// Generation reports the store's birth id: stable across restarts of the same
// file, different after a wipe.
func (s *Store) Generation() string { return s.generation }

// SetRevision moves the revision to where the record says it is: after a
// replay, to the highest stamp in the file, so the writes that follow number
// above every record that exists; after a restore, to the restore's own number.
//
// The replayed flips bumped the count on their way in, so without this a healed
// store would count its flips and stamp numbers the file already holds. Called
// by main.go after the heal and by `flipr ctl restore`, never by a handler.
func (s *Store) SetRevision(revision uint64) error {
	if err := s.db.Update(func(tx *bolt.Tx) error {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], revision)
		return tx.Bucket(bucketMeta).Put(keyRevision, b[:])
	}); err != nil {
		return err
	}
	s.revision.Store(revision)
	return nil
}

// Revision reports the store revision, exact, read from memory. A
// rolled-back write keeps its number; the count never rewinds, across
// heals and restores included (SetRevision), so a number names one attempt
// forever.
func (s *Store) Revision() uint64 { return s.revision.Load() }

// CountFlags totals namespaces, flags and expensive flags across the snapshot.
func (s *Store) CountFlags() (namespaces, flags, expensive int) {
	for _, fs := range s.current() {
		namespaces++
		for _, f := range fs {
			flags++
			if f.Expensive {
				expensive++
			}
		}
	}
	return
}

// Healthy reports whether the store can actually be read right now, rather
// than whether it once opened. /health returning an uncritical 200 is the
// failure this whole effort keeps finding.
func (s *Store) Healthy() error {
	return s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketFlags) == nil {
			return fmt.Errorf("flags bucket missing")
		}
		return nil
	})
}

// parseNSKey splits a stored service@version key, rightmost @ wins so a
// service name containing @ cannot corrupt the parse.
func parseNSKey(s string) (nsKey, error) {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '@' {
			return nsKey{s[:i], s[i+1:]}, nil
		}
	}
	return nsKey{}, fmt.Errorf("malformed namespace key %q", s)
}

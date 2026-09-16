package main

// THE LOCK IS A FILE, and a person writes it. `flipr.lock` beside the store on
// the host volume, written by `flipr ctl lock` and removed by `flipr ctl
// unlock`, never by an RPC, never by flipr itself, never by a session on its
// own judgement.
//
// Flipr never locks a namespace; locking is always external to flipr and done
// by a person. A lock says a client may keep running without flipr, which
// reverses the normal answer of taking the client down, so the decision is a
// person's each time.
//
// Why a file and not an RPC: an RPC on the wire is something any client
// could call, and a self-declared caller header is not a person. A file on
// the host volume is written by whoever holds the host, which is the meat.
//
// It survives a bounce, it works with the server up (the runbook's step 1)
// and with the server down, and the server needs nothing new on its
// contract to honour it: it answers 423 Locked with the reason and a
// Retry-After, which every published client already understands.
//
// The server reads the file on every request through a one-second cache keyed
// on the file's mtime, so a lock takes effect within a second and costs a stat
// per second, not per request.
//
// When the lock changes the server writes one Lock or Unlock record to the
// oplog with the reason, so the record says who stopped the world and why; the
// revision does not move, because nothing in the store changed.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"sync"
	"time"
)

// Lock is the file's contents: which namespaces, why, when, by whom.
type Lock struct {
	// Scopes is "*" for the whole store, else "service@version" entries.
	Scopes []string  `json:"scopes"`
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
	By     string    `json:"by"`
}

// Covers says whether this lock stops reads and writes for a namespace.
func (l *Lock) Covers(service, version string) bool {
	if l == nil {
		return false
	}
	ns := service + "@" + version
	for _, s := range l.Scopes {
		if s == "*" || s == ns {
			return true
		}
	}
	return false
}

// lockPath is the file beside the store.
func lockPath(dbPath string) string { return dbPath + ".lock" }

// readLock reads the file; nil when there is none.
func readLock(path string) (*Lock, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var l Lock
	if err := json.Unmarshal(b, &l); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(l.Scopes) == 0 || l.Reason == "" {
		return nil, fmt.Errorf(
			"%s: a lock names its scopes and its reason",
			path,
		)
	}
	return &l, nil
}

// writeLock writes the file atomically (a rename), stamping who and when.
func writeLock(path string, scopes []string, reason string) (*Lock, error) {
	by := "unknown"
	if u, err := user.Current(); err == nil {
		by = u.Username
	}
	l := &Lock{Scopes: scopes, Reason: reason, At: time.Now().UTC(), By: by}
	b, _ := json.MarshalIndent(l, "", "  ")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return nil, err
	}
	return l, os.Rename(tmp, path)
}

// lockWatch is the server's cached view of the file.
type lockWatch struct {
	path string
	log  *Logger
	rec  func(op string, l *Lock) // writes the oplog record on a change

	mu      sync.Mutex
	checked time.Time
	mtime   time.Time
	size    int64
	lock    *Lock
	broken  error
}

// lockCheckEvery bounds the stat rate: a lock takes effect within this.
const lockCheckEvery = time.Second

// current returns the lock in force, re-reading the file at most once a
// second. An unreadable file is a lock: a person wrote something there and
// the safe reading of a malformed lock is that the world is stopped until
// they fix it, which the reason says.
func (w *lockWatch) current() *Lock {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	if now.Sub(w.checked) < lockCheckEvery {
		return w.effective()
	}
	w.checked = now
	fi, err := os.Stat(w.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if w.lock != nil || w.broken != nil {
			w.log.Info(
				"lock released",
				map[string]any{"path": w.path},
			)
			if w.rec != nil && w.lock != nil {
				w.rec("Unlock", w.lock)
			}
		}
		w.lock, w.broken, w.mtime, w.size = nil, nil, time.Time{}, 0
		return nil
	case err != nil:
		w.broken = err
		return w.effective()
	}
	if fi.ModTime().Equal(w.mtime) && fi.Size() == w.size {
		return w.effective()
	}
	w.mtime, w.size = fi.ModTime(), fi.Size()
	l, err := readLock(w.path)
	if err != nil {
		w.broken = err
		w.log.Error(
			"lock file unreadable; the store is LOCKED until a "+
				"person fixes it",
			map[string]any{"path": w.path, "err": err.Error()},
		)
		return w.effective()
	}
	w.broken = nil
	w.lock = l
	w.log.Warn(
		"lock in force",
		map[string]any{
			"scopes": l.Scopes,
			"reason": l.Reason,
			"by":     l.By,
			"at":     l.At.Format(time.RFC3339),
		},
	)
	if w.rec != nil {
		w.rec("Lock", l)
	}
	return l
}

// effective is the lock as the server applies it: a broken file locks all.
func (w *lockWatch) effective() *Lock {
	if w.broken != nil {
		return &Lock{
			Scopes: []string{"*"},
			Reason: "lock file unreadable: " + w.broken.Error(),
			By:     "?",
		}
	}
	return w.lock
}

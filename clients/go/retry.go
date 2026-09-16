package flipr

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"time"
)

// Retry is the bounded backoff every call makes before it is reported as
// failed. The policy is the contract's (clients/CONTRACT.md section 6): three
// attempts, a 50 millisecond base doubling to a one second ceiling, full
// jitter.
//
// That budget covers a dropped packet, a connection reset or an edge that has
// not learned a new pod yet; it does not and should not span a flipr restart,
// which is what the caller's OnUnknown policy is for.
//
// Until 2026-09-04 this client made one attempt and a dropped packet was a
// failed read. gaggle's topology document recorded a single
// lost packet refusing a job; that was this, inherited.
//
// Every flipr RPC is a POST, and every one this client makes is safe to repeat:
// Ping and GetFlag read, PublishNamespace is idempotent by flipr's own contract
// (it never overwrites an operator's value), and this client never calls
// SetFlag.
//
// A retry helper that skipped POSTs would retry nothing here, so this one does
// not skip them.
type Retry struct {
	// Attempts is the total number of tries, the first included. Zero means
	// DefaultAttempts. One means no retry.
	Attempts int
	// Base is the wait after the first failure; each later wait doubles it,
	// up to Max. The actual sleep is uniform in [0, wait): full jitter, so
	// a fleet that lost flipr at the same instant does not ask again at the
	// same instant. Zero means the default.
	Base time.Duration
	// Max caps the wait. Zero means the default.
	Max time.Duration
}

// The defaults, named so a reader (or a check) can find the policy without
// reading the code around it.
const (
	DefaultAttempts = 3
	DefaultBase     = 50 * time.Millisecond
	DefaultMax      = time.Second

	// LockCeiling bounds how long a lock is remembered from one Retry-After
	// (CONTRACT.md section 9); LockDefault is the deadline when flipr sent
	// none. A lock is an operator's word, and the client answers from the
	// remembered lock at this cadence rather than storming.
	LockCeiling = 30 * time.Second
	LockDefault = 5 * time.Second
)

// withDefaults fills the zero fields.
func (r Retry) withDefaults() Retry {
	if r.Attempts <= 0 {
		r.Attempts = DefaultAttempts
	}
	if r.Base <= 0 {
		r.Base = DefaultBase
	}
	if r.Max <= 0 {
		r.Max = DefaultMax
	}
	if r.Max < r.Base {
		r.Max = r.Base
	}
	return r
}

// wait returns the sleep before the next attempt, given how many have
// failed so far (1 after the first failure). Exponential, capped, then
// jittered over the whole range.
func (r Retry) wait(failed int) time.Duration {
	w := r.Base
	for i := 1; i < failed && w < r.Max; i++ {
		w *= 2
	}
	if w > r.Max {
		w = r.Max
	}
	if w <= 0 {
		return 0
	}
	return rand.N(w)
}

// retryable says whether a failed attempt is worth another.
//
// Transport errors and timeouts are; so is any 5xx, which is flipr or the edge
// saying it could not answer rather than that the request was wrong; so is 429,
// flipr asking for a wait; so is a 404 whose body is not flipr's own JSON,
// which is the edge answering for a flipr that is not there.
//
// Any other 4xx is the contract speaking, and asking again would get the same
// answer. A cancelled context is the caller speaking, and is not retried.
func retryable(err error, status int) bool {
	if err == nil {
		return status >= 500 || status == 429
	}
	// an answer arrived; the status decides. A 423 is not retried inside
	// the call: the lock is the answer, and the client remembers it
	// (flipr.go).
	var se *statusError
	if errors.As(err, &se) {
		switch {
		case se.status >= 500, se.status == 429:
			return true
		case se.status == 404:
			return !se.fromFlipr()
		}
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	// url.Error wraps connection refused, EOF and friends; anything else
	// the transport reports is a transport failure
	return true
}

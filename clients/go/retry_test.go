package flipr

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// THE RETRY, measured rather than described: with one attempt, a
// dropped packet was a failed read.

// flaky answers 503 for the first n requests to any method, then behaves
// like a healthy flipr with one flag on.
func flaky(t *testing.T, failFirst int32) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := calls.Add(1)
			if n <= failFirst {
				http.Error(
					w,
					"flipr is having a moment",
					http.StatusServiceUnavailable,
				)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			switch {
			case strings.HasSuffix(r.URL.Path, "/Ping"):
				_, _ = w.Write(
					[]byte(`{"storeGeneration":"g1"}`),
				)
			case strings.HasSuffix(r.URL.Path, "/GetFlag"):
				_, _ = w.Write(
					[]byte(
						`{"flag":{"key":"x.enabled","value":{"boolValue":true}}}`,
					),
				)
			case strings.HasSuffix(r.URL.Path, "/GetNamespace"):
				_, _ = w.Write(
					[]byte(
						`{"namespace":{"service":"s","version":"v1","flags":[{"key":"x.enabled","value":{"boolValue":true}}]}}`,
					),
				)
			default:
				_, _ = w.Write([]byte(`{}`))
			}
		}),
	)
	t.Cleanup(srv.Close)
	return srv, &calls
}

// quiet is a logger that discards, for tests that do not read the lines.
func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(discard{}, nil)) }

type discard struct{}

// Write drops the bytes and reports them written.
func (discard) Write(p []byte) (int, error) { return len(p), nil }

// TestOneDroppedAnswerIsRetriedNotFailed: the first Ping answers 503, the
// second answers; the client makes exactly two requests and the read is
// known.
func TestOneDroppedAnswerIsRetriedNotFailed(t *testing.T) {
	srv, calls := flaky(t, 1)
	c, err := New(
		Config{
			Service:   "s",
			Version:   "v1",
			EnvPrefix: "S",
			URL:       srv.URL,
			Declared: []Flag{
				{Key: "x.enabled", On: false, Desc: "x"},
			},
			OnUnknown: Refuse,
			Log:       quiet(),
			CacheTTL:  NoCache,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if c.Mode() != "flipr" {
		t.Fatalf(
			"one 503 at boot dropped the client to env mode; "+
				"calls=%d",
			calls.Load(),
		)
	}
	// boot made Ping (503, then ok) and PublishNamespace: three requests
	if got := calls.Load(); got != 3 {
		t.Fatalf(
			"boot made %d requests, want 3 (a failed ping, its "+
				"retry, the publish)",
			got,
		)
	}
	on, known, why := c.Check("x.enabled")
	if !on || !known || why != "" {
		t.Fatalf(
			"check after boot: on=%v known=%v why=%q",
			on,
			known,
			why,
		)
	}
}

// TestExhaustionIsReportedWithTheCount: a flipr that never answers is given
// exactly Attempts tries per call, within the backoff budget, and the error
// says so.
func TestExhaustionIsReportedWithTheCount(t *testing.T) {
	srv, calls := flaky(t, 1<<30)
	c, err := New(
		Config{
			Service:   "s",
			Version:   "v1",
			EnvPrefix: "S",
			URL:       srv.URL,
			Declared: []Flag{
				{Key: "x.enabled", On: false, Desc: "x"},
			},
			OnUnknown: Refuse,
			Log:       quiet(),
			Retry:     Retry{Attempts: 3},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	// boot: three tries at Ping, then flipr mode with the policy applied
	if got := calls.Load(); got != 3 {
		t.Fatalf("boot made %d requests, want 3", got)
	}
	if c.Mode() != "flipr" {
		t.Fatal(
			"a configured flipr that never answers at boot stays " +
				"in flipr mode; the policy applies",
		)
	}
	// drive rpc directly for the error text and the budget
	calls.Store(0)
	start := time.Now()
	_, perr := c.ping()
	took := time.Since(start)
	if perr == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(perr.Error(), "after 3 attempts over") ||
		!strings.Contains(perr.Error(), "503") {
		t.Fatalf(
			"error does not carry the count and the last answer: "+
				"%v",
			perr,
		)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("made %d requests, want 3", got)
	}
	// two waits, each at most the ceiling, plus the requests themselves
	if took > 2*DefaultMax+2*time.Second {
		t.Fatalf(
			"three attempts took %s; the backoff is not bounded",
			took,
		)
	}
}

// TestAContractRefusalIsNotRetried: a 4xx is the contract speaking, and one
// request is enough to hear it.
func TestAContractRefusalIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			http.Error(
				w,
				`{"error":"off-contract request"}`,
				http.StatusBadRequest,
			)
		}),
	)
	t.Cleanup(srv.Close)
	c := &Client{
		cfg:     Config{Service: "s", Version: "v1", Log: quiet()},
		base:    srv.URL,
		metrics: noMetrics{},
		hc: &http.Client{
			Timeout: time.Second,
		},
		retry: Retry{}.withDefaults(),
	}
	_, err := c.ping()
	if err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("want the 400 back: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("a 400 was retried: %d requests", got)
	}
	if strings.Contains(err.Error(), "attempts") {
		t.Fatalf(
			"a single attempt should not be reported as many: %v",
			err,
		)
	}
}

// TestConnectionRefusedIsRetried: nothing listening is a transport failure,
// tried Attempts times.
func TestConnectionRefusedIsRetried(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	c := &Client{
		cfg:     Config{Service: "s", Version: "v1", Log: quiet()},
		base:    url,
		metrics: noMetrics{},
		hc: &http.Client{
			Timeout: time.Second,
		},
		retry: Retry{Attempts: 2}.withDefaults(),
	}
	start := time.Now()
	_, err := c.ping()
	if err == nil || !strings.Contains(err.Error(), "after 2 attempts") {
		t.Fatalf("want two attempts reported: %v", err)
	}
	if time.Since(start) > DefaultMax+time.Second {
		t.Fatal("the wait between attempts exceeded the ceiling")
	}
}

// TestBackoffIsExponentialCappedAndJittered pins the arithmetic: the wait
// after n failures is uniform in [0, min(Max, Base*2^(n-1))).
func TestBackoffIsExponentialCappedAndJittered(t *testing.T) {
	r := Retry{
		Base: 50 * time.Millisecond,
		Max:  250 * time.Millisecond,
	}.withDefaults()
	for failed, ceiling := range map[int]time.Duration{
		1: 50 * time.Millisecond, 2: 100 * time.Millisecond, 3: 200 * time.Millisecond,
		4: 250 * time.Millisecond, 9: 250 * time.Millisecond,
	} {
		var lo, hi time.Duration = time.Hour, 0
		for i := 0; i < 200; i++ {
			w := r.wait(failed)
			if w < 0 || w >= ceiling {
				t.Fatalf(
					"after %d failures: wait %s outside "+
						"[0, %s)",
					failed,
					w,
					ceiling,
				)
			}
			if w < lo {
				lo = w
			}
			if w > hi {
				hi = w
			}
		}
		if hi-lo < ceiling/4 {
			t.Errorf(
				"after %d failures: 200 waits spanned only "+
					"%s of %s; that is not full jitter",
				failed,
				hi-lo,
				ceiling,
			)
		}
	}
	d := Retry{}.withDefaults()
	if d.Attempts != DefaultAttempts || d.Base != DefaultBase ||
		d.Max != DefaultMax {
		t.Fatalf("defaults: %+v", d)
	}
	if one := (Retry{Attempts: 1}).withDefaults(); one.Attempts != 1 {
		t.Fatal("Attempts: 1 must mean one try")
	}
}

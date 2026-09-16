package flipr

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fliprv1 "github.com/janearc/flipr-dist/clients/go/flipr/v1"
)

// A fake flipr, shaped like the real one on the wire: protojson over plain
// HTTP at /flipr.v1.FliprService/<Method>.
type fake struct {
	mu        sync.Mutex
	gen       string
	values    map[string]bool
	published []string
	down      bool
	calls     []string
}

// newFake is a flipr with one generation and no flags set.
func newFake() *fake {
	return &fake{gen: "g1", values: map[string]bool{}}
}

// serve answers the client's RPCs from the fake's state, recording each call.
func (f *fake) serve(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f.mu.Lock()
			down := f.down
			method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			f.calls = append(f.calls, method)
			f.mu.Unlock()
			if down {
				http.Error(
					w,
					"flipr is down",
					http.StatusServiceUnavailable,
				)
				return
			}
			body, _ := io.ReadAll(r.Body)
			var req map[string]any
			_ = json.Unmarshal(body, &req)
			w.Header().Set("Content-Type", "application/json")
			switch method {
			case "Ping":
				f.mu.Lock()
				gen := f.gen
				f.mu.Unlock()
				_, _ = w.Write(
					[]byte(
						`{"storeGeneration":"` + gen + `"}`,
					),
				)
			case "PublishNamespace":
				f.mu.Lock()
				ns, _ := req["namespace"].(map[string]any)
				f.published = append(
					f.published,
					ns["service"].(string)+"@"+ns["version"].(string),
				)
				f.mu.Unlock()
				_, _ = w.Write([]byte(`{}`))
			case "GetFlag":
				key, _ := req["key"].(string)
				f.mu.Lock()
				v := f.values[key]
				f.mu.Unlock()
				_, _ = w.Write(
					[]byte(
						`{"flag":{"key":"` + key + `","value":{"boolValue":` + boolStr(
							v,
						) + `}}}`,
					),
				)
			case "GetNamespace":
				f.mu.Lock()
				flags := []string{}
				for k, v := range f.values {
					flags = append(
						flags,
						`{"key":"`+k+`","value":{"boolValue":`+boolStr(
							v,
						)+`}}`,
					)
				}
				f.mu.Unlock()
				_, _ = w.Write(
					[]byte(
						`{"namespace":{"service":"testsvc","version":"v1","flags":[` + strings.Join(
							flags,
							",",
						) + `]}}`,
					),
				)
			case "SetFlag":
				key, _ := req["key"].(string)
				val, _ := req["value"].(map[string]any)
				on, _ := val["boolValue"].(bool)
				f.mu.Lock()
				f.values[key] = on
				f.mu.Unlock()
				_, _ = w.Write(
					[]byte(
						`{"flag":{"key":"` + key + `","value":{"boolValue":` + boolStr(
							on,
						) + `}}}`,
					),
				)
			case "ListNamespaces":
				_, _ = w.Write(
					[]byte(
						`{"namespaces":[{"service":"testsvc","version":"v1"},{"service":"other","version":"v1"}]}`,
					),
				)
			default:
				http.Error(
					w,
					"no such method",
					http.StatusNotFound,
				)
			}
		}),
	)
	t.Cleanup(srv.Close)
	return srv
}

// boolStr is a bool as the text the environment mode reads.
func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// quietLog discards, for tests that do not read the lines.
func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// cfg is a client configuration for the test service at url.
func cfg(url string, p Policy) Config {
	return Config{
		Service: "testsvc", Version: "v1", URL: url, EnvPrefix: "TESTSVC",
		Declared: []Flag{
			{
				Key:  "thing.enabled",
				On:   true,
				Desc: "on: does the thing. off: does not.",
			},
		},
		OnUnknown: p, Log: quietLog(),
		// the tests read fresh so a flip lands on the next check; the
		// cache has its own test
		CacheTTL: NoCache,
	}
}

// THE POINT OF THE PACKAGE. The same unanswered read must produce opposite
// behaviour depending on what the caller is guarding, and both must report
// known=false so the caller can publish its own uncertainty.
func TestUnknownSplitsOnPolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy Policy
		wantOn bool
	}{
		{"a spend gate refuses", Refuse, false},
		{"an instrument holds", Hold, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake()
			f.values["thing.enabled"] = true
			srv := f.serve(t)
			c, err := New(cfg(srv.URL, tc.policy))
			if err != nil {
				t.Fatal(err)
			}
			// a good read first, so Hold has something to hold
			if on, known, _ := c.Check("thing.enabled"); !on ||
				!known {
				t.Fatalf(
					"first read: on=%v known=%v",
					on,
					known,
				)
			}
			f.mu.Lock()
			f.down = true
			f.mu.Unlock()
			on, known, why := c.Check("thing.enabled")
			if known {
				t.Fatal("a dead flipr was reported as known")
			}
			if on != tc.wantOn {
				t.Fatalf(
					"on=%v want %v (why: %s)",
					on,
					tc.wantOn,
					why,
				)
			}
			if why == "" {
				t.Fatal("no reason given for an unknown read")
			}
		})
	}
}

// Hold with nothing yet read falls back to the DECLARED default, never to a
// bare false -- a collector declared on must not be silenced by a flipr that
// was never reachable.
func TestHoldWithNoPriorReadUsesDeclaredDefault(t *testing.T) {
	f := newFake()
	srv := f.serve(t)
	c, err := New(cfg(srv.URL, Hold))
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.down = true
	f.mu.Unlock()
	on, known, _ := c.Check("thing.enabled")
	if known || !on {
		t.Fatalf("on=%v known=%v; declared default is on", on, known)
	}
}

// An operator flip must land on the next check, not after a restart: the cache
// is a performance optimisation, never a source of truth.
func TestValueIsReadFreshEveryCheck(t *testing.T) {
	// CacheTTL NoCache: the pre-contract behaviour, still available
	f := newFake()
	f.values["thing.enabled"] = true
	srv := f.serve(t)
	c, _ := New(cfg(srv.URL, Refuse))
	if on, _, _ := c.Check("thing.enabled"); !on {
		t.Fatal("expected on")
	}
	f.mu.Lock()
	f.values["thing.enabled"] = false
	f.mu.Unlock()
	if on, known, why := c.Check("thing.enabled"); on || !known {
		t.Fatalf(
			"flip did not land: on=%v known=%v why=%s",
			on,
			known,
			why,
		)
	}
}

// A wiped store must be re-seeded with the declarations, or every flag an
// operator never touched silently disappears.
func TestStoreWipeRepublishes(t *testing.T) {
	f := newFake()
	srv := f.serve(t)
	c, _ := New(cfg(srv.URL, Refuse))
	f.mu.Lock()
	before := len(f.published)
	f.gen = "g2"
	f.mu.Unlock()
	c.Check("thing.enabled")
	f.mu.Lock()
	after := len(f.published)
	f.mu.Unlock()
	if after != before+1 {
		t.Fatalf("publishes: %d -> %d, want one more", before, after)
	}
}

// TestPublishesAtBoot: New publishes the service's namespace, once.
func TestPublishesAtBoot(t *testing.T) {
	f := newFake()
	srv := f.serve(t)
	if _, err := New(cfg(srv.URL, Refuse)); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.published) != 1 || f.published[0] != "testsvc@v1" {
		t.Fatalf("published: %v", f.published)
	}
}

// A flipr that is configured and down at boot is NOT env mode and NOT an
// error: the client stays in flipr mode and the policy applies to every read
// (Refuse refuses: an outage must not make a service more willing to spend),
// the next answer is the promotion, and the declarations reach flipr then.
func TestDeadAtBootAppliesThePolicyUntilFliprAnswers(t *testing.T) {
	f := newFake()
	f.down = true
	srv := f.serve(t)
	c0 := cfg(srv.URL, Refuse)
	c0.Retry = Retry{Attempts: 1}
	c, err := New(c0)
	if err != nil {
		t.Fatalf("a dead flipr failed the process: %v", err)
	}
	if c.Mode() != "flipr" {
		t.Fatalf(
			"mode: %s, want flipr (env mode is chosen by an "+
				"empty URL, never fallen into)",
			c.Mode(),
		)
	}
	if on, known, why := c.Check("thing.enabled"); on || known ||
		!strings.Contains(why, "STOP") {
		t.Fatalf(
			"Refuse against a dead flipr: on=%v known=%v why=%s",
			on,
			known,
			why,
		)
	}
	if c.Up() {
		t.Fatal("Up() must be false while flipr does not answer")
	}
	// flipr comes back: the next check is the promotion, and the
	// declarations are published on it
	f.mu.Lock()
	f.down = false
	f.values["thing.enabled"] = true
	f.mu.Unlock()
	if on, known, why := c.Check("thing.enabled"); !on || !known {
		t.Fatalf(
			"after flipr answers: on=%v known=%v why=%s",
			on,
			known,
			why,
		)
	}
	if !c.Up() {
		t.Fatal("Up() must be true once flipr answers")
	}
	f.mu.Lock()
	published := len(f.published)
	f.mu.Unlock()
	if published != 1 {
		t.Fatalf(
			"the declarations were published %d times after "+
				"flipr answered, want once",
			published,
		)
	}
}

// TestEnvModeReadsThePrefixedVariable: with no URL the client reads flags
// from environment variables under its prefix.
func TestEnvModeReadsThePrefixedVariable(t *testing.T) {
	c, err := New(cfg("", Refuse))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.envVar("thing.enabled"); got != "TESTSVC_THING_ENABLED" {
		t.Fatalf("envVar: %s", got)
	}
	t.Setenv("TESTSVC_THING_ENABLED", "0")
	if on, known, _ := c.Check("thing.enabled"); on || !known {
		t.Fatalf("on=%v known=%v", on, known)
	}
	// a non-boolean is the operator's own answer, not an outage: fall back
	// to the declaration and say so, rather than interpreting it
	t.Setenv("TESTSVC_THING_ENABLED", "yes please")
	on, known, why := c.Check("thing.enabled")
	if !on || !known || !strings.Contains(why, "not a boolean") {
		t.Fatalf("on=%v known=%v why=%q", on, known, why)
	}
}

// A key nobody declared is not this service's to authorize.
func TestUndeclaredKeyIsNeverOnByDefault(t *testing.T) {
	c, _ := New(cfg("", Refuse))
	if on, _, _ := c.Check("nobody.declared.this"); on {
		t.Fatal("an undeclared key defaulted on")
	}
}

// Misconfiguration is fatal; a down flipr is not. The distinction is that an
// operator can flip their way out of one and not the other.
func TestConfigValidation(t *testing.T) {
	base := cfg("", Refuse)
	for _, tc := range []struct {
		name string
		mut  func(*Config)
	}{
		{"no service", func(c *Config) { c.Service = "" }},
		{"no version", func(c *Config) { c.Version = "" }},
		{"no env prefix", func(c *Config) { c.EnvPrefix = "" }},
		{"no unknown policy", func(c *Config) { c.OnUnknown = PolicyUnset }},
		{"no logger", func(c *Config) { c.Log = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.mut(&c)
			if _, err := New(c); err == nil {
				t.Fatal("accepted an invalid config")
			}
		})
	}
}

// Every call carries attribution, so flipr's oplog can rule this client out.
func TestEveryCallDeclaresItsCaller(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen = append(seen, r.Header.Get("X-Flipr-Caller"))
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"storeGeneration":"g1"}`))
		}),
	)
	defer srv.Close()
	c, _ := New(cfg(srv.URL, Refuse))
	_ = c
	if len(seen) == 0 {
		t.Fatal("no calls made")
	}
	for _, h := range seen {
		if h != "testsvc@v1" {
			t.Fatalf("caller header: %q", h)
		}
	}
}

// TestAFlipLandsWithinTheCacheTTL: inside the TTL a check is a map read and
// costs flipr nothing; after it, the next check refreshes and the flip lands.
func TestAFlipLandsWithinTheCacheTTL(t *testing.T) {
	f := newFake()
	f.values["thing.enabled"] = true
	srv := f.serve(t)
	c0 := cfg(srv.URL, Refuse)
	c0.CacheTTL = 200 * time.Millisecond
	c, err := New(c0)
	if err != nil {
		t.Fatal(err)
	}
	if on, _, _ := c.Check("thing.enabled"); !on {
		t.Fatal("expected on")
	}
	f.mu.Lock()
	f.values["thing.enabled"] = false
	before := len(f.calls)
	f.mu.Unlock()
	if on, known, _ := c.Check("thing.enabled"); !on || !known {
		t.Fatal("inside the TTL the cached value must be served")
	}
	f.mu.Lock()
	made := f.calls[before:]
	f.mu.Unlock()
	// inside the TTL a check pings (always check flipr is there) and
	// fetches nothing
	if len(made) != 1 || made[0] != "Ping" {
		t.Fatalf("a check inside the TTL made %v, want one Ping", made)
	}
	time.Sleep(250 * time.Millisecond)
	if on, known, why := c.Check("thing.enabled"); on || !known {
		t.Fatalf(
			"after the TTL the flip must land: on=%v known=%v "+
				"why=%s",
			on,
			known,
			why,
		)
	}
}

// TestEveryRequestNamesTheCallerAndTheClient (CONTRACT.md sections 1, 8).
func TestEveryRequestNamesTheCallerAndTheClient(t *testing.T) {
	var caller, client string
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			caller, client = r.Header.Get(
				"X-Flipr-Caller",
			), r.Header.Get(
				"X-Flipr-Client",
			)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"storeGeneration":"g1"}`))
		}),
	)
	t.Cleanup(srv.Close)
	c, err := New(cfg(srv.URL, Refuse))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.ping()
	if caller != "testsvc@v1" {
		t.Fatalf("X-Flipr-Caller = %q", caller)
	}
	if client != "go/"+ClientTag+"/"+ClientHash ||
		!strings.HasPrefix(client, "go/") {
		t.Fatalf("X-Flipr-Client = %q", client)
	}
}

// TestALockIsItsOwnOutcome (CONTRACT.md section 9): a 423 is neither missing
// nor unreachable and is not retried inside the call; the lock is remembered
// with its Retry-After deadline, and inside it every check answers with the
// policy's answer and makes NO request; the first check after it makes one;
// the policy applies; Locked says why; and the next good answer clears it.
func TestALockIsItsOwnOutcome(t *testing.T) {
	var locked atomic.Bool
	locked.Store(true)
	var calls atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			if locked.Load() {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusLocked)
				_, _ = w.Write(
					[]byte(
						`{"error":"locked: restore to 4127"}`,
					),
				)
				return
			}
			switch {
			case strings.HasSuffix(r.URL.Path, "/Ping"):
				_, _ = w.Write(
					[]byte(`{"storeGeneration":"g1"}`),
				)
			case strings.HasSuffix(r.URL.Path, "/GetNamespace"):
				_, _ = w.Write(
					[]byte(
						`{"namespace":{"service":"testsvc","version":"v1","flags":[{"key":"thing.enabled","value":{"boolValue":true}}]}}`,
					),
				)
			default:
				_, _ = w.Write([]byte(`{}`))
			}
		}),
	)
	t.Cleanup(srv.Close)
	m := &countingMetrics{}
	c0 := cfg(srv.URL, Hold)
	c0.Retry = Retry{Attempts: 3}
	c0.Metrics = m
	// New pings, meets the lock, stays in flipr mode and remembers it
	c, err := New(c0)
	if err != nil {
		t.Fatal(err)
	}
	// New made one request (the boot ping, locked) and remembered the lock
	if got := calls.Load(); got != 1 {
		t.Fatalf(
			"boot made %d requests, want one (a lock is not "+
				"retried inside the call)",
			got,
		)
	}
	// inside the deadline: the policy's answer, no request
	start := time.Now()
	on, known, why := c.Check("thing.enabled")
	if known || !on || !strings.Contains(why, "423") {
		t.Fatalf(
			"a lock under Hold should hold the declared default "+
				"and say so: on=%v known=%v why=%s",
			on,
			known,
			why,
		)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("a remembered lock must answer at once")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf(
			"a check inside the lock's deadline made a request: "+
				"%d total",
			got,
		)
	}
	if is, reason := c.Locked(); !is ||
		!strings.Contains(reason, "restore to 4127") {
		t.Fatalf("Locked() = %v %q", is, reason)
	}
	if m.outcomes["locked"] == 0 {
		t.Fatalf(
			"the locked outcome was not counted for the "+
				"remembered answer: %v",
			m.outcomes,
		)
	}
	// after the deadline, still locked: exactly one request, and a new
	// deadline
	time.Sleep(1100 * time.Millisecond)
	if _, known, _ := c.Check("thing.enabled"); known {
		t.Fatal("still locked: the read must be unknown")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf(
			"the first check after the deadline made %d requests "+
				"in total, want 2",
			got,
		)
	}
	// after the next deadline, unlocked: the store answers and the lock
	// clears
	locked.Store(false)
	time.Sleep(1100 * time.Millisecond)
	if on, known, _ := c.Check("thing.enabled"); !on || !known {
		t.Fatal(
			"after the deadline and the unlock the read must be " +
				"known",
		)
	}
	if is, _ := c.Locked(); is {
		t.Fatal("a good answer must clear the lock state")
	}
}

// TestAbsentBehindTheEdgeIsUnreachableNotMissing (CONTRACT.md section 3): a
// 404 that is not flipr's JSON is the edge answering for a flipr that is not
// there, and it is retried and reported unreachable.
func TestAbsentBehindTheEdgeIsUnreachableNotMissing(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			http.Error(w, "404 page not found", http.StatusNotFound)
		}),
	)
	t.Cleanup(srv.Close)
	m := &countingMetrics{}
	c := &Client{
		cfg:     Config{Service: "s", Version: "v1", Log: quietLog()},
		base:    srv.URL,
		metrics: m,
		hc: &http.Client{
			Timeout: time.Second,
		},
		retry: Retry{Attempts: 2}.withDefaults(),
	}
	if _, err := c.ping(); err == nil {
		t.Fatal("want an error")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("an edge 404 must be retried: %d requests", got)
	}
	if m.outcomes["unreachable"] != 1 || m.outcomes["missing"] != 0 {
		t.Fatalf("outcome: %v", m.outcomes)
	}
}

// TestFliprsOwn404IsMissingAndNotRetried: the JSON 404 is the contract
// speaking, once.
func TestFliprsOwn404IsMissingAndNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"no namespace s@v1"}`))
		}),
	)
	t.Cleanup(srv.Close)
	m := &countingMetrics{}
	c := &Client{
		cfg:     Config{Service: "s", Version: "v1", Log: quietLog()},
		base:    srv.URL,
		metrics: m,
		hc: &http.Client{
			Timeout: time.Second,
		},
		retry: Retry{}.withDefaults(),
	}
	if _, err := c.ping(); err == nil {
		t.Fatal("want an error")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("flipr's own 404 was retried: %d requests", got)
	}
	if m.outcomes["missing"] != 1 {
		t.Fatalf("outcome: %v", m.outcomes)
	}
}

// TestListIsATooolsVerb: ListNamespaces returns every namespace held.
func TestListIsAToolsVerb(t *testing.T) {
	f := newFake()
	srv := f.serve(t)
	c, err := New(cfg(srv.URL, Refuse))
	if err != nil {
		t.Fatal(err)
	}
	ns, err := c.List()
	if err != nil || len(ns) != 2 {
		t.Fatalf("List: %v %d", err, len(ns))
	}
}

// TestConstructionRefusals (CONTRACT.md section 10).
func TestConstructionRefusals(t *testing.T) {
	f := newFake()
	srv := f.serve(t)
	for name, mutate := range map[string]func(*Config){
		"commit as version": func(c *Config) { c.Version = "29c44ff" },
		"empty declared":    func(c *Config) { c.Declared = nil },
		"address as url":    func(c *Config) { c.URL = "http://10.0.0.5:9800" },
	} {
		c0 := cfg(srv.URL, Refuse)
		mutate(&c0)
		if _, err := New(c0); err == nil {
			t.Errorf("%s: constructed", name)
		}
	}
	// a name with a port is fine: the throwaway edge is flipr.test:7800
	c0 := cfg(srv.URL, Refuse)
	c0.URL = "http://flipr.test:7800"
	if _, err := New(c0); err != nil &&
		strings.Contains(err.Error(), "address") {
		t.Errorf("a name with a port was refused: %v", err)
	}
}

// countingMetrics is a Metrics that counts, for the tests.
type countingMetrics struct {
	mu       sync.Mutex
	outcomes map[string]int
	retries  int
	gens     int
}

// Request counts each outcome.
func (m *countingMetrics) Request(method, outcome string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.outcomes == nil {
		m.outcomes = map[string]int{}
	}
	m.outcomes[outcome]++
}

// Retry counts retries.
func (m *countingMetrics) Retry(
	string,
) {
	m.mu.Lock()
	m.retries++
	m.mu.Unlock()
}

// Duration is not needed by these tests.
func (m *countingMetrics) Duration(string, time.Duration) {}

// GenerationChange counts generation changes.
func (m *countingMetrics) GenerationChange() { m.mu.Lock(); m.gens++; m.mu.Unlock() }

// TestSetIsTheOperatorsVerb: a flip in another namespace with a reason.
func TestSetIsTheOperatorsVerb(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if strings.HasSuffix(r.URL.Path, "/SetFlag") {
				body, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(body, &got)
				_, _ = w.Write(
					[]byte(
						`{"flag":{"key":"fetch.enabled","value":{"boolValue":false}}}`,
					),
				)
				return
			}
			_, _ = w.Write([]byte(`{"storeGeneration":"g1"}`))
		}),
	)
	t.Cleanup(srv.Close)
	c, err := New(cfg(srv.URL, Refuse))
	if err != nil {
		t.Fatal(err)
	}
	f, err := c.Set(
		"kingfisher",
		"v1",
		"fetch.enabled",
		&fliprv1.Value{
			Kind: &fliprv1.Value_BoolValue{BoolValue: false},
		},
		"operator: pausing fetches",
	)
	if err != nil || f.GetKey() != "fetch.enabled" {
		t.Fatalf("Set: %v %v", err, f)
	}
	if got["service"] != "kingfisher" || got["version"] != "v1" ||
		got["reason"] != "operator: pausing fetches" {
		t.Fatalf(
			"the request did not carry the namespace and reason: "+
				"%v",
			got,
		)
	}
}

// TestALandedSetDropsTheCache: a tool that flips and reads back sees the
// new value at once, not the cached namespace for up to a TTL (found
// 2026-09-06 against a live flipr).
func TestALandedSetDropsTheCache(t *testing.T) {
	f := newFake()
	f.values["thing.enabled"] = true
	srv := f.serve(t)
	c0 := cfg(srv.URL, Refuse)
	c0.CacheTTL = time.Hour // the cache would serve the old value all day
	c, err := New(c0)
	if err != nil {
		t.Fatal(err)
	}
	if on, _, _ := c.Check("thing.enabled"); !on {
		t.Fatal("before the flip: on")
	}
	if _, err := c.Set("testsvc", "v1", "thing.enabled", &fliprv1.Value{Kind: &fliprv1.Value_BoolValue{BoolValue: false}}, "operator"); err != nil {
		t.Fatal(err)
	}
	if on, known, why := c.Check("thing.enabled"); on || !known {
		t.Fatalf(
			"after the flip the cache is dropped and the read is "+
				"fresh: on=%v known=%v %s",
			on,
			known,
			why,
		)
	}
	// a Set that flipr refused leaves the cache as it was: nothing changed
	f.down = true
	if _, err := c.Set("testsvc", "v1", "thing.enabled", &fliprv1.Value{Kind: &fliprv1.Value_BoolValue{BoolValue: true}}, "operator"); err == nil {
		t.Fatal("a down flipr refuses the flip")
	}
}

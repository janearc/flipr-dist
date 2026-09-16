// Package flipr is the Go client for flipr, the flag store the mesh reads
// before it does anything expensive.
//
// It exists because there were two hand-rolled copies of it. gaggle wrote one,
// metricsd was about to copy it, and the second copy is where a shared client
// stops being a convenience and starts being the only way the doctrine stays
// consistent, so the client lives in flipr's own tree, general enough to adapt.
//
// # The doctrine, and the one thing callers must choose
//
// The cache is a performance optimisation and never a fallback: a stale
// flag served quietly is worse than an outage, because an outage is
// something you can see.
//
// So every check pings. A check inside [Config.CacheTTL] of the last
// refresh reads the cached namespace, and the first check after it
// refreshes the whole namespace in one trip.
//
// An unanswered ping is reported as unknown, and what the caller does with
// that is the policy, out loud (clients/CONTRACT.md sections 4 and 5).
//
// What a caller does with unknown is the one thing this package refuses to
// decide, because the two right answers point in opposite directions:
//
//   - A spend gate must refuse. gaggle's flags guard GPU hours, LLM calls and
//     third-party fetches; a stale yes there authorizes money nobody approved.
//     Unknown means STOP. This is [Refuse].
//
//   - An instrument must keep reporting. metricsd's flag decides whether a
//     collector publishes; silencing telemetry because the flag store wobbled
//     removes the instrument in exactly the incident it exists for, and a hole
//     in the series is not a safe direction, it is a blind spot. Unknown means
//     carry on with the last known answer and PUBLISH the uncertainty. This is
//     [Hold].
//
// Picking wrong is quiet in both directions, so [Config.OnUnknown] has no
// default: a zero Config is rejected by [New] rather than guessing.
//
// # Two modes, chosen at startup and never mixed
//
//   - Flipr mode: first contact succeeded. Values are refreshed once per
//     CacheTTL, so an operator flip lands within it.
//   - env mode: URL is empty. Gates read `<PREFIX>_<KEY>_ENABLED`. This
//     pre-onboarding behaviour, chosen by leaving the URL empty and never
//     fallen into: a configured flipr that does not answer at boot keeps the
//     client in flipr mode with the policy applied to every read, because an
//     outage must not make a service more willing to spend.
package flipr

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	fliprv1 "github.com/janearc/flipr-dist/clients/go/flipr/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Policy is what a caller does when nothing authoritative answered.
type Policy int

const (
	// PolicyUnset is the zero value and is not a policy. New rejects it.
	PolicyUnset Policy = iota

	// Refuse: unknown never authorizes. For gates over spend, where a stale
	// yes costs money. Enabled returns false.
	Refuse

	// Hold: unknown keeps the last known answer, or the declared default if
	// nothing has been read yet. For components whose job is to keep
	// reporting, where going quiet IS the failure.
	//
	// Enabled returns what the caller was already doing, and Check's known
	// result is how the caller publishes that it is holding.
	Hold
)

// Flag is one declared boolean: what the service publishes about itself at
// boot so an operator finds it in the store without reading the source.
type Flag struct {
	// Key is the flag name, conventionally "<area>.enabled".
	Key string
	// On is the declared default -- what the flag reads before anyone flips
	// it.
	On bool
	// Desc must say what ON does, what OFF does, and what happens to work
	// already in flight. An operator reading it at 03:20 is deciding
	// whether flipping it will fix the page or cause the next one.
	Desc string
	// Expensive marks a flag that gates real spend, which puffin renders
	// with a $ in its roster. Marking a cheap flag expensive dilutes the
	// marker.
	Expensive bool
}

// Config builds a Client. Service, Version, EnvPrefix, Declared, OnUnknown and
// Log are required.
type Config struct {
	// Service is the namespace's service half, e.g. "metricsd".
	Service string
	// Version is the namespace's version half. PIN IT rather than passing a
	// build commit: per-commit namespaces are born with declared defaults,
	// so every deploy silently resurrects flags an operator had killed.
	Version string
	// URL is flipr's base URL. Empty means env mode, deliberately.
	URL string
	// EnvPrefix is the env-mode variable prefix, e.g. `METRICSD` makes
	// smc.enabled read METRICSD_SMC_ENABLED.
	EnvPrefix string
	// Declared is what this service publishes about itself at boot.
	Declared []Flag
	// OnUnknown is the caller's policy. There is no default; see the
	// package comment on why guessing here is quiet in both directions.
	OnUnknown Policy
	// Log receives the loud lines: dropping to env mode, a refused publish,
	// a store wipe, an unresolved read.
	Log *slog.Logger
	// HTTP is optional; a 5s-timeout client is used when nil. The timeout
	// is per attempt; see Retry.
	HTTP *http.Client
	// Retry is the bounded backoff every call makes. The zero value means
	// the contract's defaults (three attempts, 50ms base, 1s ceiling, full
	// jitter); set Attempts to 1 to make one try only.
	Retry Retry
	// CacheTTL is how long a refreshed namespace answers checks without a
	// trip to flipr. Zero means DefaultCacheTTL; NoCache means every check
	// refreshes, the pre-contract behaviour.
	CacheTTL time.Duration
	// Metrics receives the client's own instruments (CONTRACT.md section
	// 7). Nil is a no-op.
	Metrics Metrics
}

// DefaultCacheTTL is the contract's default: a flip lands within five seconds.
const DefaultCacheTTL = 5 * time.Second

// NoCache as CacheTTL makes every check a refresh.
const NoCache = time.Duration(-1)

// Metrics is what the client emits about itself; the caller supplies the
// series (CONTRACT.md section 7). Outcome is one of value, missing,
// unreachable, refused, locked.
type Metrics interface {
	Request(method, outcome string)
	Retry(method string)
	Duration(method string, d time.Duration)
	GenerationChange()
}

type noMetrics struct{}

// Request counts nothing: no Metrics was given.
func (noMetrics) Request(string, string) {}

// Retry counts nothing.
func (noMetrics) Retry(string) {}

// Duration records nothing.
func (noMetrics) Duration(string, time.Duration) {}

// GenerationChange counts nothing.
func (noMetrics) GenerationChange() {}

// ClientHeader is the value sent as X-Flipr-Client: go/<tag>/<hash>
// (CONTRACT.md sections 1 and 8).
//
// ClientTag and ClientHash live in stamp.go, Generated by clients/bin/stamp.sh
// and committed at the tag, so a consumer that pins the tag announces it with
// nothing passed at build; a build's -ldflags -X on them still wins. A checkout
// between tags announces dev/unknown, which flipr counts as an unknown client.
func ClientHeader() string { return "go/" + ClientTag + "/" + ClientHash }

// Client reads flags for one service namespace.
type Client struct {
	cfg     Config
	base    string // "" => env mode
	hc      *http.Client
	retry   Retry
	ttl     time.Duration
	metrics Metrics

	mu        sync.Mutex
	gen       string
	published bool // the declarations reached flipr at least once
	up        bool // the last ping answered
	last      map[string]bool
	ns        *fliprv1.Namespace // the last refresh, whole
	refreshed time.Time
	lockedAt  time.Time
	// while it stands, every call answers from the lock with no request
	lockUntil time.Time
	lockedWhy string
}

// New makes first contact.
//
// It never fails the process on a flipr that is down -- that lands in env mode
// with a loud log line -- but it DOES fail on a misconfigured Config, because
// a service that publishes under the wrong name or with no unknown-policy is a
// bug an operator cannot flip their way out of.
func New(cfg Config) (*Client, error) {
	switch {
	case cfg.Service == "":
		return nil, errors.New("flipr: Config.Service is required")
	case cfg.Version == "":
		return nil, errors.New(
			"flipr: Config.Version is required; pin it, do not " +
				"pass a build commit",
		)
	case cfg.EnvPrefix == "":
		return nil, errors.New(
			"flipr: Config.EnvPrefix is required for env mode",
		)
	case cfg.OnUnknown == PolicyUnset:
		return nil, errors.New(
			"flipr: Config.OnUnknown must be Refuse (spend " +
				"gates) or Hold (instruments); there is no " +
				"safe default",
		)
	case cfg.Log == nil:
		return nil, errors.New(
			"flipr: Config.Log is required; this client's " +
				"failures are only useful if they are heard",
		)
	case looksLikeCommit(cfg.Version):
		return nil, fmt.Errorf(
			"flipr: Config.Version %q looks like a commit hash; "+
				"the version is the flag contract's, pinned",
			cfg.Version,
		)
	case len(cfg.Declared) == 0:
		return nil, errors.New(
			"flipr: Config.Declared is empty; a service with no " +
				"flags has nothing to read",
		)
	case isAddress(cfg.URL):
		return nil, fmt.Errorf(
			"flipr: Config.URL %q is an address; flipr is "+
				"reached by name",
			cfg.URL,
		)
	}
	hc := cfg.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 5 * time.Second}
	}
	ttl := cfg.CacheTTL
	if ttl == 0 {
		ttl = DefaultCacheTTL
	}
	var m Metrics = cfg.Metrics
	if m == nil {
		m = noMetrics{}
	}
	c := &Client{
		cfg:     cfg,
		base:    strings.TrimRight(cfg.URL, "/"),
		hc:      hc,
		retry:   cfg.Retry.withDefaults(),
		ttl:     ttl,
		metrics: m,
		last:    map[string]bool{},
	}
	if c.base == "" {
		cfg.Log.Warn(
			"flipr: no URL configured; env-mode gates",
			"prefix",
			cfg.EnvPrefix,
		)
		return c, nil
	}
	gen, err := c.ping()
	if err != nil {
		// Not env mode. A flipr that was named and did not answer is an
		// outage, and an outage must not make a service more willing to
		// spend.
		//
		// Env mode reads the declared defaults, and a declared default
		// can be on: a blip at boot once resurrected a gate an operator
		// had killed, and left it on until a restart.
		//
		// The client stays in flipr mode. Every check pings, so the
		// next answer is the promotion; until then the caller's
		// policy applies to every read.
		cfg.Log.Error(
			"flipr: configured but unreachable at boot; the "+
				"policy applies to every read until it "+
				"answers",
			"url",
			c.base,
			"err",
			err.Error(),
		)
		return c, nil
	}
	c.gen = gen
	c.up = true
	if err := c.publish(); err != nil {
		// A refused publish is a contract failure, not an outage:
		// surface it and stay in flipr mode so the refusal stays
		// visible on every read.
		cfg.Log.Error("flipr: publish refused", "err", err.Error())
	} else {
		c.published = true
	}
	cfg.Log.Info(
		"flipr: connected",
		"namespace",
		c.Namespace(),
		"generation",
		gen,
	)
	return c, nil
}

// Check answers one read with its epistemics separated.
//
// on is the value to act on, already resolved through OnUnknown. known says
// whether anything authoritative answered; a caller that publishes its own
// metrics should publish known too, so "the flag is off" and "we could not ask"
// are distinguishable from outside the process.
//
// why is a human sentence for a log line, empty when there is nothing to
// explain.
func (c *Client) Check(key string) (on, known bool, why string) {
	if c.base == "" {
		return c.checkEnv(key)
	}
	// every check pings: the cache is an optimisation on the namespace
	// fetch, never a fallback for flipr's absence
	if err := c.verify(); err != nil {
		return c.unknown(key, err.Error())
	}
	c.mu.Lock()
	fresh := c.ns != nil && c.ttl > 0 && time.Since(c.refreshed) < c.ttl
	c.mu.Unlock()
	if !fresh {
		if err := c.fetch(); err != nil {
			return c.unknown(key, err.Error())
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range c.ns.GetFlags() {
		if f.GetKey() != key {
			continue
		}
		v := f.GetValue().GetBoolValue()
		c.last[key] = v
		if !v {
			return false, true, key + " is off in flipr"
		}
		return true, true, ""
	}
	// missing: flipr answered and this key is not declared in the namespace
	return c.unknownLocked(key, key+" is not declared in "+c.Namespace())
}

// verify pings, and on a changed generation drops the cache and republishes
// the declarations (collaborative self-heal: publish never clobbers an
// operator's value).
func (c *Client) verify() error {
	gen, err := c.ping()
	if err != nil {
		c.mu.Lock()
		c.up = false
		c.mu.Unlock()
		return fmt.Errorf("flipr unreachable (%w)", err)
	}
	c.mu.Lock()
	c.up = true
	changed := c.gen != "" && gen != c.gen
	first := !c.published
	c.gen = gen
	if changed {
		c.ns = nil
	}
	c.mu.Unlock()
	if changed {
		c.metrics.GenerationChange()
		c.cfg.Log.Warn(
			"flipr: store generation changed; republishing "+
				"declared defaults",
			"generation",
			gen,
		)
	}
	if changed || first {
		// the declarations reach flipr on the first answer after a boot
		// that found it down, and again after a wipe; never clobbering
		// a value
		if err := c.publish(); err != nil {
			return fmt.Errorf("flipr republish failed (%w)", err)
		}
		c.mu.Lock()
		c.published = true
		c.mu.Unlock()
	}
	return nil
}

// fetch reads the whole namespace in one trip into the cache.
func (c *Client) fetch() error {
	resp := &fliprv1.GetNamespaceResponse{}
	if err := c.rpc("GetNamespace", &fliprv1.GetNamespaceRequest{
		Service: c.cfg.Service,
		Version: c.cfg.Version,
	}, resp); err != nil {
		return fmt.Errorf(
			"flipr GetNamespace %s failed (%w)",
			c.Namespace(),
			err,
		)
	}
	c.mu.Lock()
	c.ns = resp.GetNamespace()
	c.refreshed = time.Now()
	c.mu.Unlock()
	return nil
}

// Refresh pings and fetches the whole namespace in one trip. Callers on a
// loop call this once per interval; Check between refreshes costs one ping.
func (c *Client) Refresh() error {
	if err := c.verify(); err != nil {
		return err
	}
	return c.fetch()
}

// Snapshot is the namespace as of the last refresh, whole, for a health page
// or a roster; nil before the first successful refresh.
func (c *Client) Snapshot() *fliprv1.Namespace {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ns
}

// List returns every namespace flipr holds. A tool's verb (puffin's audit),
// never a service's.
func (c *Client) List() ([]*fliprv1.Namespace, error) {
	resp := &fliprv1.ListNamespacesResponse{}
	if err := c.rpc(
		"ListNamespaces", &fliprv1.ListNamespacesRequest{}, resp,
	); err != nil {
		return nil, err
	}
	return resp.GetNamespaces(), nil
}

// Set flips one flag in any namespace with a stated reason: the operator's
// verb, for tools (puffin's flip screen, fliprctl). A service never calls
// it; a service's flags are flipped by an operator, not by the service.
//
// flipr refuses a flip without a reason, a flip to a different type, and a
// flip of an undeclared flag, and this returns what flipr said.
//
// A flip that landed drops this client's cached namespace, whichever
// namespace it was, because the client knows the store changed.
//
// A tool that flips and reads back must not see the old value: against a
// live flipr, Set(false) then Check read true for up to five seconds.
func (c *Client) Set(
	service, version, key string,
	value *fliprv1.Value,
	reason string,
) (*fliprv1.Flag, error) {
	resp := &fliprv1.SetFlagResponse{}
	err := c.rpc("SetFlag", &fliprv1.SetFlagRequest{
		Service: service,
		Version: version,
		Key:     key,
		Value:   value,
		Reason:  reason,
	}, resp)
	if err == nil {
		c.mu.Lock()
		c.ns = nil
		c.refreshed = time.Time{}
		c.mu.Unlock()
	}
	return resp.GetFlag(), err
}

// Up reports whether the last ping answered: false before the first answer
// and after any unanswered one, for a health line that distinguishes "flipr
// is authoritative" from "flipr was named and is not answering".
func (c *Client) Up() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.up
}

// Locked reports whether the last answer from flipr was a lock, and why
// (CONTRACT.md section 9), for the caller's own health line.
func (c *Client) Locked() (bool, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.lockedAt.IsZero(), c.lockedWhy
}

// checkEnv resolves a gate with no flipr. Every answer here is known: the
// operator's environment is authoritative, including its refusals.
func (c *Client) checkEnv(key string) (on, known bool, why string) {
	env := c.envVar(key)
	switch v := os.Getenv(env); {
	case v == "":
		return c.declaredDefault(key), true, ""
	case strings.EqualFold(v, "1") || strings.EqualFold(v, "true"):
		return true, true, ""
	case strings.EqualFold(v, "0") || strings.EqualFold(v, "false"):
		return false, true, key + " disabled by " + env
	default:
		// The operator wrote it, so it is a known answer and not an
		// outage -- but it is not a boolean, so fall back to the
		// declaration and say so rather than inventing an
		// interpretation of "yes please".
		return c.declaredDefault(key), true,
			fmt.Sprintf(
				"%s=%q is not a boolean; using the declared "+
					"default",
				env,
				v,
			)
	}
}

// unknown applies OnUnknown to a read that nothing authoritative answered.
func (c *Client) unknown(key, why string) (on, known bool, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unknownLocked(key, why)
}

// unknownLocked is unknown with c.mu held.
func (c *Client) unknownLocked(
	key, why string,
) (on, known bool, reason string) {
	switch c.cfg.OnUnknown {
	case Hold:
		v, seen := c.last[key]
		if !seen {
			v = c.declaredDefault(key)
		}
		return v, false, why + "; holding last known"
	default: // Refuse
		return false, false, why + "; gates fail STOP"
	}
}

// looksLikeCommit is true for a 7 to 40 digit hex string, which is a commit
// and not a flag contract version.
func looksLikeCommit(v string) bool {
	if len(v) < 7 || len(v) > 40 {
		return false
	}
	for _, r := range v {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}

// isAddress is true for a URL whose host is an IP address other than loopback:
// flipr is reached by name (a port on a name is fine; the throwaway edge is
// flipr.test:7800), and an address in a config is one more thing that is true
// today and wrong tomorrow.
//
// Loopback is allowed so a test can stand up a fake on 127.0.0.1.
func isAddress(u string) bool {
	if u == "" {
		return false
	}
	host := u
	if i := strings.Index(host, "://"); i >= 0 {
		host = host[i+3:]
	}
	if i := strings.IndexAny(host, "/?#"); i >= 0 {
		host = host[:i]
	}
	if i := strings.LastIndex(host, ":"); i >= 0 &&
		!strings.Contains(host, "]") {
		host = host[:i]
	}
	if host == "127.0.0.1" || host == "localhost" || host == "::1" ||
		host == "[::1]" {
		return false
	}
	parts := strings.Split(host, ".")
	if len(parts) == 4 {
		for _, p := range parts {
			if _, err := strconv.Atoi(p); err != nil {
				return false
			}
		}
		return true
	}
	return false
}

// Enabled is Check with the policy already applied and the epistemics dropped,
// for callers that only want a yes or no and a reason.
func (c *Client) Enabled(key string) (bool, string) {
	on, _, why := c.Check(key)
	return on, why
}

// Namespace reports the store key the gates read, for a /health handler.
func (c *Client) Namespace() string {
	return c.cfg.Service + "@" + c.cfg.Version
}

// Mode reports which gate regime is live: "flipr" or "env".
func (c *Client) Mode() string {
	if c.base == "" {
		return "env"
	}
	return "flipr"
}

// Declared returns the flags this client publishes, so a caller can initialise
// one metric series per flag at boot rather than having them spring into
// existence on first read.
func (c *Client) Declared() []Flag { return c.cfg.Declared }

// envVar maps smc.enabled to `<PREFIX>_SMC_ENABLED`.
func (c *Client) envVar(key string) string {
	base := strings.TrimSuffix(key, ".enabled")
	return c.cfg.EnvPrefix + "_" + strings.ToUpper(
		strings.ReplaceAll(base, ".", "_"),
	) + "_ENABLED"
}

// declaredDefault answers a key's declared default, so env mode, the publish
// and the Hold policy share ONE source of truth about what a flag starts as.
func (c *Client) declaredDefault(key string) bool {
	for _, d := range c.cfg.Declared {
		if d.Key == key {
			return d.On
		}
	}
	// A key nobody declared is not this service's to authorize.
	return false
}

// ping is the liveness check every read makes; returns the store generation.
func (c *Client) ping() (string, error) {
	resp := &fliprv1.PingResponse{}
	if err := c.rpc("Ping", &fliprv1.PingRequest{}, resp); err != nil {
		return "", err
	}
	return resp.GetStoreGeneration(), nil
}

// publish sends the declared flags; idempotent, never overwrites an operator.
func (c *Client) publish() error {
	ns := &fliprv1.Namespace{Service: c.cfg.Service, Version: c.cfg.Version}
	for _, d := range c.cfg.Declared {
		ns.Flags = append(ns.Flags, &fliprv1.Flag{
			Key: d.Key,
			Value: &fliprv1.Value{
				Kind: &fliprv1.Value_BoolValue{BoolValue: d.On},
			},
			Description: d.Desc,
			Expensive:   d.Expensive,
		})
	}
	return c.rpc(
		"PublishNamespace",
		&fliprv1.PublishNamespaceRequest{
			Namespace: ns,
		},
		&fliprv1.PublishNamespaceResponse{},
	)
}

// rpc posts protojson to one FliprService method and decodes the reply,
// retrying on the Retry policy. A transport failure or a 5xx is tried again
// after a jittered wait; a 4xx is returned at once, because the contract would
// say the same thing twice.
//
// The error after the last attempt says how many were made and how long they
// took, so a log line reads as "two tries over 80ms" rather than as one
// mysterious failure.
//
// protojson over plain HTTP, not gRPC, because
// gRPC would drag HTTP/2 and generated stubs into every consumer for a service
// whose entire job is answering a question that fits in a cache line.
func (c *Client) rpc(method string, req, resp proto.Message) error {
	body, err := protojson.Marshal(req)
	if err != nil {
		return err
	}
	start := time.Now()
	defer func() { c.metrics.Duration(method, time.Since(start)) }()
	// a remembered lock answers at once, with no request, until its
	// deadline (CONTRACT.md section 9): a hot path cannot storm a locked
	// flipr
	if se := c.lockStanding(method); se != nil {
		c.metrics.Request(method, "locked")
		return se
	}
	var last error
	made := 0
	for attempt := 1; attempt <= c.retry.Attempts; attempt++ {
		made = attempt
		status, err := c.once(method, body, resp)
		if err == nil {
			c.noteUnlocked()
			c.metrics.Request(method, "value")
			return nil
		}
		last = err
		if !retryable(err, status) || attempt == c.retry.Attempts {
			break
		}
		wait := c.retry.wait(attempt)
		// a rate-limited flipr says how long to wait; honour it, capped
		var se *statusError
		if errors.As(err, &se) && se.retryAfter > 0 {
			wait = se.retryAfter
			if wait > LockCeiling {
				wait = LockCeiling
			}
		}
		c.metrics.Retry(method)
		c.cfg.Log.Debug(
			"flipr: retrying",
			"method",
			method,
			"attempt",
			attempt,
			"of",
			c.retry.Attempts,
			"wait",
			wait.String(),
			"err",
			err.Error(),
		)
		time.Sleep(wait)
	}
	c.metrics.Request(method, outcomeOf(last))
	if made > 1 {
		return fmt.Errorf(
			"%s: after %d attempts over %s: %w",
			method,
			made,
			time.Since(start).Round(time.Millisecond),
			last,
		)
	}
	return last
}

// lockStanding returns the remembered lock as the answer while its deadline
// stands, else nil.
func (c *Client) lockStanding(method string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lockUntil.IsZero() || time.Now().After(c.lockUntil) {
		return nil
	}
	return &statusError{
		method:      method,
		status:      http.StatusLocked,
		text:        "423 Locked (remembered)",
		body:        `{"error":"` + c.lockedWhy + `"}`,
		contentType: "application/json",
	}
}

// outcomeOf names a failed call's outcome for the instruments (CONTRACT.md
// section 3): missing is flipr's own 404, refused any other 4xx, locked a
// 423, and everything else unreachable.
func outcomeOf(err error) string {
	var se *statusError
	if !errors.As(err, &se) {
		return "unreachable"
	}
	switch {
	case se.status == 423:
		return "locked"
	case se.status == 404 && se.fromFlipr():
		return "missing"
	case se.status >= 500, se.status == 404, se.status == 429:
		return "unreachable"
	default:
		return "refused"
	}
}

// noteLocked remembers a lock and its deadline: Retry-After capped at the
// ceiling, or the default when flipr sent none. The health line reads it
// through Locked.
func (c *Client) noteLocked(why string, retryAfter time.Duration) {
	until := retryAfter
	if until <= 0 {
		until = LockDefault
	}
	if until > LockCeiling {
		until = LockCeiling
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lockedAt.IsZero() {
		c.cfg.Log.Warn(
			"flipr: locked; holding and backing off",
			"reason",
			why,
			"until",
			time.Now().Add(until).Format(time.RFC3339),
		)
	}
	c.lockedAt = time.Now()
	c.lockUntil = time.Now().Add(until)
	c.lockedWhy = why
}

// noteUnlocked clears a lock the client had been told about, once flipr
// answers as unlocked again.
func (c *Client) noteUnlocked() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.lockedAt.IsZero() {
		c.cfg.Log.Info("flipr: unlocked", "was", c.lockedWhy)
	}
	c.lockedAt = time.Time{}
	c.lockUntil = time.Time{}
	c.lockedWhy = ""
}

// once makes one attempt. It returns the HTTP status alongside the error so
// the caller can tell a refused request from an unanswered one.
func (c *Client) once(
	method string,
	body []byte,
	resp proto.Message,
) (int, error) {
	hr, err := http.NewRequest(
		"POST",
		c.base+"/flipr.v1.FliprService/"+method,
		bytes.NewReader(body),
	)
	if err != nil {
		return 0, err
	}
	hr.Header.Set("Content-Type", "application/json")
	// Self-declared attribution for flipr's record and its counters. This
	// client reads and publishes its own declarations; it never calls
	// SetFlag.
	//
	// Saying so on every call means whoever mines the record for who
	// touched a namespace can rule it out at a glance instead of wondering.
	hr.Header.Set("X-Flipr-Caller", c.Namespace())
	// And which client this is, so a copy announces itself as a copy
	// (CONTRACT.md section 8).
	hr.Header.Set("X-Flipr-Client", ClientHeader())
	r, err := c.hc.Do(hr)
	if err != nil {
		return 0, err
	}
	defer r.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<22))
	if err != nil {
		return r.StatusCode, err
	}
	if r.StatusCode != http.StatusOK {
		se := &statusError{
			method:      method,
			status:      r.StatusCode,
			text:        r.Status,
			body:        strings.TrimSpace(string(raw)),
			contentType: r.Header.Get("Content-Type"),
		}
		if ra := r.Header.Get("Retry-After"); ra != "" {
			if n, err := strconv.Atoi(ra); err == nil && n > 0 {
				se.retryAfter = time.Duration(n) * time.Second
			}
		}
		if r.StatusCode == http.StatusLocked {
			c.noteLocked(se.reason(), se.retryAfter)
		}
		return r.StatusCode, se
	}
	return r.StatusCode, protojson.Unmarshal(raw, resp)
}

// statusError is a non-200 answer, kept as a type so the status survives
// wrapping and a caller can errors.As it out.
type statusError struct {
	method, text, body, contentType string
	status                          int
	retryAfter                      time.Duration
}

// Error names the method, what flipr said and the body it sent.
func (e *statusError) Error() string {
	return e.method + ": " + e.text + ": " + e.body
}

// Status is the HTTP status of the answer.
func (e *statusError) Status() int { return e.status }

// fromFlipr says whether the answer came from flipr itself (a JSON body) or
// from the edge answering for an absent flipr (anything else). Two 404s
// arrive here and confusing them is a real incident shape (CONTRACT.md
// section 3).
func (e *statusError) fromFlipr() bool {
	return strings.HasPrefix(e.contentType, "application/json") ||
		strings.HasPrefix(e.body, "{")
}

// reason pulls flipr's own error text out of a JSON body, for a lock's why.
func (e *statusError) reason() string {
	var m map[string]any
	if err := json.Unmarshal([]byte(e.body), &m); err == nil {
		if r, ok := m["error"].(string); ok && r != "" {
			return r
		}
		if r, ok := m["reason"].(string); ok && r != "" {
			return r
		}
	}
	return e.body
}

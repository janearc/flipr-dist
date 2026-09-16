package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// INTEGRATION TESTS. These drive the real mux, the real store on a real bbolt
// file, and the real oplog -- no stubs in the path. The only thing not
// exercised is the listener itself, which is main's job.

// testServer builds a complete flipr backed by a temporary database.
func testServer(t *testing.T) (*httptest.Server, *captureSink) {
	t.Helper()
	reg := NewInstruments(time.Now())
	store, err := OpenStore(
		filepath.Join(t.TempDir(), "flipr.db"),
		reg.Registry(),
		NewLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	sink := &captureSink{}
	oplog := NewOpLog(sink, reg.Registry(), NewLogger())
	srv := httptest.NewServer(
		newServer(store, oplog, reg, NewLogger()).routes(),
	)
	t.Cleanup(func() {
		srv.Close()
		store.Close()
	})
	return srv, sink
}

// post sends a protojson body to an RPC and returns status and body.
func post(
	t *testing.T,
	srv *httptest.Server,
	method, body string,
) (int, string) {
	t.Helper()
	resp, err := http.Post(srv.URL+"/flipr.v1.FliprService/"+method,
		"application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

// TestTheWholeLifecycle walks a service through onboarding, being read, and
// being flipped -- the sequence every onboarded service actually performs.
func TestTheWholeLifecycle(t *testing.T) {
	srv, sink := testServer(t)

	code, body := post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"albatross","version":"a1b2c3d","flags":[
		{"key":"nightly.enabled","value":{"boolValue":true},"description":"on: the nightly aggregation runs and may spend. off: no nightly runs.","expensive":true},
		{"key":"nightly.smoothing","value":{"stringValue":"claude"},"description":"which smoother: surreal or claude. the spend gate is nightly.enabled."},
		{"key":"web.banner","value":{"stringValue":""},"description":"banner text","expensive":false}]}}`,
	)
	if code != 200 || !strings.Contains(body, `"flagsPublished":3`) {
		t.Fatalf("publish: %d %s", code, body)
	}

	code, body = post(
		t,
		srv,
		"GetNamespace",
		`{"service":"albatross","version":"a1b2c3d"}`,
	)
	if code != 200 || !strings.Contains(body, "nightly.smoothing") {
		t.Fatalf("get namespace: %d %s", code, body)
	}

	code, body = post(
		t,
		srv,
		"SetFlag",
		`{"service":"albatross","version":"a1b2c3d","key":"nightly.smoothing","value":{"stringValue":"surreal"},"reason":"claude smoothing is banned"}`,
	)
	if code != 200 || !strings.Contains(body, "surreal") {
		t.Fatalf("set: %d %s", code, body)
	}

	code, body = post(
		t,
		srv,
		"GetFlag",
		`{"service":"albatross","version":"a1b2c3d","key":"nightly.smoothing"}`,
	)
	if code != 200 || !strings.Contains(body, "surreal") {
		t.Fatalf("get after set: %d %s", code, body)
	}
	// The flip must not clear the metadata the emergency page needs --
	// checked on the boolean that owns the spend, since the smoothing
	// string is now deliberately cheap per the killable-boolean rule.
	code, body = post(
		t,
		srv,
		"SetFlag",
		`{"service":"albatross","version":"a1b2c3d","key":"nightly.enabled","value":{"boolValue":false},"reason":"lifecycle test kill"}`,
	)
	if code != 200 || !strings.Contains(body, `"expensive":true`) {
		t.Errorf("expensive lost across a flip: %d %s", code, body)
	}

	code, body = post(t, srv, "ListNamespaces", `{}`)
	if code != 200 || !strings.Contains(body, "albatross") {
		t.Fatalf("list: %d %s", code, body)
	}

	// The record holds what flipr CHANGED: the 3 writes (PublishNamespace,
	// SetFlag, SetFlag) are logged synchronously and are there now; the 3
	// reads are not records.
	if sink.count() != 3 {
		t.Errorf(
			"op log has %d entries, want exactly the 3 writes",
			sink.count(),
		)
	}
}

// TestAFlipWithoutAReasonIsRefused guards the rule that makes an outage
// explainable afterwards.
func TestAFlipWithoutAReasonIsRefused(t *testing.T) {
	srv, _ := testServer(t)
	code, body := post(
		t,
		srv,
		"SetFlag",
		`{"service":"s","version":"v","key":"k","value":{"boolValue":false}}`,
	)
	if code != http.StatusBadRequest ||
		!strings.Contains(body, "reason is required") {
		t.Fatalf("got %d %s", code, body)
	}
}

// TestOffContractRequestsAreRefused checks the boundary rejects unknown fields
// rather than ignoring them. The generated types are lenient by design, so if
// this is not enforced here an off-contract message arrives.
func TestOffContractRequestsAreRefused(t *testing.T) {
	srv, _ := testServer(t)
	// rollout_percentage is exactly the kind of thing flipr refuses to
	// become.
	code, body := post(
		t,
		srv,
		"GetFlag",
		`{"service":"s","version":"v","key":"k","rollout_percentage":50}`,
	)
	if code != http.StatusBadRequest ||
		!strings.Contains(body, "off-contract") {
		t.Fatalf("got %d %s", code, body)
	}
}

// TestMissesReturn404 checks a flag that was never declared is a miss rather
// than a false "off".
func TestMissesReturn404(t *testing.T) {
	srv, _ := testServer(t)
	if code, _ := post(t, srv, "GetFlag", `{"service":"nope","version":"v","key":"k"}`); code != 404 {
		t.Errorf("GetFlag miss returned %d, want 404", code)
	}
	if code, _ := post(t, srv, "GetNamespace", `{"service":"nope","version":"v"}`); code != 404 {
		t.Errorf("GetNamespace miss returned %d, want 404", code)
	}
}

// TestIncompleteNamespaceIsRefused checks a publish must name both the service
// and the version, since the version is what keeps deploys apart.
func TestIncompleteNamespaceIsRefused(t *testing.T) {
	srv, _ := testServer(t)
	for _, body := range []string{
		`{}`,
		`{"namespace":{"service":"a"}}`,
		`{"namespace":{"version":"v"}}`,
	} {
		if code, _ := post(t, srv, "PublishNamespace", body); code != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400", body, code)
		}
	}
}

// TestGetOnAnRPCIsRefused checks the RPC surface is POST-only.
func TestGetOnAnRPCIsRefused(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/flipr.v1.FliprService/GetFlag")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("got %d, want 405", resp.StatusCode)
	}
}

// TestPingDoesNotNeedABody checks the liveness call every client makes is as
// forgiving as possible, since it is on everyone's hot path.
func TestPingDoesNotNeedABody(t *testing.T) {
	srv, _ := testServer(t)
	code, body := post(t, srv, "Ping", ``)
	if code != 200 || !strings.Contains(body, Version) {
		t.Fatalf("got %d %s", code, body)
	}
}

// TestHealthReportsHealthy checks /health answers with the real state.
func TestHealthReportsHealthy(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("got %d", resp.StatusCode)
	}
	var out struct {
		Healthy bool   `json:"healthy"`
		Version string `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if !out.Healthy || out.Version != Version {
		t.Fatalf("got %+v", out)
	}
}

// TestHealthFailsLoudlyWhenTheStoreIsGone checks /health never returns an
// uncritical 200. A monitor that reports OK when it cannot see is worse than
// no monitor.
func TestHealthFailsLoudlyWhenTheStoreIsGone(t *testing.T) {
	inst := NewInstruments(time.Now())
	store, err := OpenStore(
		filepath.Join(t.TempDir(), "f.db"),
		inst.Registry(),
		NewLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	oplog := NewOpLog(&captureSink{}, inst.Registry(), NewLogger())
	srv := httptest.NewServer(
		newServer(store, oplog, inst, NewLogger()).routes(),
	)
	defer srv.Close()

	store.Close() // the store is now unusable underneath a live server

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a dead store reported %d; want 503", resp.StatusCode)
	}
}

// TestAPIServesAParsableDescriptor checks the service answers "what is your
// API" itself, and that the answer is a real FileDescriptorSet rather than
// bytes that merely look like one.
func TestAPIServesAParsableDescriptor(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/api")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var fds descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(raw, &fds); err != nil {
		t.Fatalf("/api did not return a FileDescriptorSet: %v", err)
	}
	var found bool
	for _, f := range fds.File {
		for _, svc := range f.Service {
			if svc.GetName() == "FliprService" {
				found = true
				if len(svc.Method) != 7 {
					t.Errorf(
						"descriptor advertises %d "+
							"methods, want 7",
						len(svc.Method),
					)
				}
			}
		}
	}
	if !found {
		t.Error("descriptor does not describe FliprService")
	}
}

// TestMetricsRecordEveryRPC checks the router-level instrumentation actually
// fires, so "every RPC is measured" is a property rather than a habit.
func TestMetricsRecordEveryRPC(t *testing.T) {
	srv, _ := testServer(t)
	post(t, srv, "Ping", `{}`)
	post(
		t,
		srv,
		"GetFlag",
		`{"service":"nope","version":"v","key":"k"}`,
	) // 404
	post(
		t,
		srv,
		"SetFlag",
		`{"service":"s","version":"v","key":"k"}`,
	) // 400

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	out := string(raw)
	for _, want := range []string{
		`flipr_rpc_requests_total{method="Ping",outcome="ok"} 1`,
		`flipr_rpc_requests_total{method="GetFlag",outcome="client_error"} 1`,
		`flipr_rpc_requests_total{method="SetFlag",outcome="client_error"} 1`,
		"flipr_rpc_duration_seconds_bucket",
		"flipr_store_reload_total",
		"flipr_db_size_bytes",
		"flipr_oplog_entries_total",
		"flipr_namespaces",
		"flipr_expensive_flags",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q from /metrics", want)
		}
	}
}

// TestEmergencySetDiscipline is the ruling mechanized: off stops behaviour,
// uniformly, with no room for whimsy.
// Expensive flags are boolean, positively named, and their descriptions
// state both polarities. Publishes that break the discipline are refused
// with words that teach the fix.
func TestEmergencySetDiscipline(t *testing.T) {
	srv, _ := testServer(t)
	refuse := func(flag, wantWord string) {
		t.Helper()
		code, body := post(
			t,
			srv,
			"PublishNamespace",
			`{"namespace":{"service":"s","version":"v","flags":[`+flag+`]}}`,
		)
		if code != http.StatusBadRequest {
			t.Fatalf(
				"accepted a non-conforming expensive flag "+
					"(%d): %s",
				code,
				flag,
			)
		}
		if !strings.Contains(body, wantWord) {
			t.Errorf(
				"refusal does not teach: want %q in %s",
				wantWord,
				body,
			)
		}
	}
	// an expensive string: the oh-shit page could not kill it
	refuse(
		`{"key":"model.backend","value":{"stringValue":"claude"},"description":"on: x. off: y.","expensive":true}`,
		"must be BOOLEAN",
	)
	// a negated name: "off" becomes a double negative
	refuse(
		`{"key":"fetch.disable","value":{"boolValue":true},"description":"on: x. off: y.","expensive":true}`,
		"double negative",
	)
	// polarity unstated: the subject is named, the behavior is not
	refuse(
		`{"key":"fetch.noaa","value":{"boolValue":false},"description":"NOAA InPort: bathymetry","expensive":true}`,
		"both polarities",
	)

	// the conforming shape is accepted
	code, _ := post(t, srv, "PublishNamespace",
		`{"namespace":{"service":"s","version":"v","flags":[
			{"key":"fetch.noaa","value":{"boolValue":false},"expensive":true,
			 "description":"on: fetches NOAA InPort bathymetry datasets over the network. off: no NOAA requests are made."}]}}`)
	if code != 200 {
		t.Fatalf("refused a conforming flag: %d", code)
	}
	// and cheap flags stay free: the discipline binds the break-glass
	// surface, not every knob
	code, _ = post(t, srv, "PublishNamespace",
		`{"namespace":{"service":"s","version":"v","flags":[
			{"key":"web.banner","value":{"stringValue":"hi"},"description":"whimsy permitted here"}]}}`)
	if code != 200 {
		t.Fatalf("the discipline leaked onto a cheap flag: %d", code)
	}
}

// TestNamespaceBudget: flag 25 is refused, 24 is fine, and the refusal
// carries the reasoning rather than a bare number.
func TestNamespaceBudget(t *testing.T) {
	srv, _ := testServer(t)
	flags := func(n int) string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf(
				`{"key":"knob.k%d","value":{"boolValue":false},"description":"a knob"}`,
				i,
			)
		}
		return strings.Join(out, ",")
	}
	code, _ := post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"s","version":"v","flags":[`+flags(
			24,
		)+`]}}`,
	)
	if code != 200 {
		t.Fatalf("24 flags should publish: %d", code)
	}
	code, body := post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"s","version":"v","flags":[`+flags(
			25,
		)+`]}}`,
	)
	if code != http.StatusBadRequest {
		t.Fatalf("25 flags published: %d", code)
	}
	if !strings.Contains(body, "not an a/b testing system") {
		t.Errorf("refusal does not carry the reasoning: %s", body)
	}
}

// TestDeleteNamespaceOverHTTP: reason required, logged before answered.
func TestDeleteNamespaceOverHTTP(t *testing.T) {
	srv, sink := testServer(t)
	post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"dodo","version":"old1","flags":[{"key":"k","value":{"boolValue":true},"description":"x"}]}}`,
	)

	code, _ := post(
		t,
		srv,
		"DeleteNamespace",
		`{"service":"dodo","version":"old1"}`,
	)
	if code != http.StatusBadRequest {
		t.Fatalf("delete without a reason: %d", code)
	}
	code, body := post(
		t,
		srv,
		"DeleteNamespace",
		`{"service":"dodo","version":"old1","reason":"stale deploy namespace, gc sweep"}`,
	)
	if code != 200 || !strings.Contains(body, `"flagsRemoved":1`) {
		t.Fatalf("delete: %d %s", code, body)
	}
	if code, _ := post(t, srv, "GetNamespace", `{"service":"dodo","version":"old1"}`); code != 404 {
		t.Fatal("retired namespace still answers")
	}
	found := false
	for _, e := range sink.snapshot() {
		if e["op"] == "DeleteNamespace" &&
			e["reason"] == "stale deploy namespace, gc sweep" {
			found = true
		}
	}
	if !found {
		t.Fatal("the delete left no oplog record")
	}
}

// TestTheRevisionIsTheCheckpoint: every write bumps the
// store revision by one inside its own transaction; every answer carries it;
// a GetNamespace with since equal to the current revision answers unchanged
// with no flags; a read never moves it; a restart keeps it.
func TestTheRevisionIsTheCheckpoint(t *testing.T) {
	dir := t.TempDir()
	inst := NewInstruments(time.Now())
	store, err := OpenStore(
		filepath.Join(dir, "f.db"),
		inst.Registry(),
		NewLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	sink := &captureSink{}
	oplog := NewOpLog(sink, inst.Registry(), NewLogger())
	srv := httptest.NewServer(
		newServer(store, oplog, inst, NewLogger()).routes(),
	)
	t.Cleanup(srv.Close)

	rev := func(body string) uint64 {
		var m map[string]any
		_ = json.Unmarshal([]byte(body), &m)
		r, _ := m["revision"].(string) // protojson renders uint64 as a string
		var n uint64
		fmt.Sscan(r, &n)
		return n
	}
	_, body := post(t, srv, "Ping", `{}`)
	if rev(body) != 0 {
		t.Fatalf("a fresh store starts at revision 0, got %s", body)
	}
	_, body = post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"s","version":"v","flags":[{"key":"k","value":{"boolValue":true},"description":"a test flag"}]}}`,
	)
	if rev(body) != 1 {
		t.Fatalf("the first write is revision 1: %s", body)
	}
	_, body = post(
		t,
		srv,
		"SetFlag",
		`{"service":"s","version":"v","key":"k","value":{"boolValue":false},"reason":"testing the checkpoint"}`,
	)
	if rev(body) != 2 {
		t.Fatalf("the second write is revision 2: %s", body)
	}
	// the oplog record of the flip carries its revision
	last := sink.snapshot()[len(sink.snapshot())-1]
	if last["op"] != "SetFlag" || last["revision"] != uint64(2) {
		t.Fatalf("the record does not carry revision 2: %v", last)
	}
	// reads do not move it, and since == current answers unchanged with no
	// flags
	for i := 0; i < 3; i++ {
		post(
			t,
			srv,
			"GetFlag",
			`{"service":"s","version":"v","key":"k"}`,
		)
	}
	_, body = post(
		t,
		srv,
		"GetNamespace",
		`{"service":"s","version":"v","since":"2"}`,
	)
	if rev(body) != 2 || !strings.Contains(body, `"unchanged":true`) ||
		strings.Contains(body, `"flags"`) {
		t.Fatalf(
			"since == current should answer unchanged and "+
				"nothing else: %s",
			body,
		)
	}
	if got := inst.Registry().Counter(mUnchanged).Value(); got != 1 {
		t.Errorf("unchanged counter = %d, want 1", got)
	}
	// since behind the current answers the namespace and the revision
	_, body = post(
		t,
		srv,
		"GetNamespace",
		`{"service":"s","version":"v","since":"1"}`,
	)
	if rev(body) != 2 || strings.Contains(body, `"unchanged":true`) ||
		!strings.Contains(body, `"key":"k"`) {
		t.Fatalf("since behind should answer the namespace: %s", body)
	}
	// and /health says it
	_, doc := health(t, srv.URL)
	if fmt.Sprint(doc["revision"]) != "2" {
		t.Fatalf("health revision = %v", doc["revision"])
	}
	// a restart keeps it
	srv.Close()
	store.Close()
	again, err := OpenStore(
		filepath.Join(dir, "f.db"),
		NewInstruments(time.Now()).Registry(),
		NewLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if again.Revision() != 2 {
		t.Fatalf(
			"after a restart the revision is %d, want 2",
			again.Revision(),
		)
	}
}

// TestTheLockIsAFileAPersonWrites (CONTRACT.md section 9; lock.go): a lock
// file beside the store stops the RPCs it covers with 423 and a Retry-After,
// shows on /health as degraded with the reason, is recorded in the oplog,
// takes effect within the watch's interval, and is released by removing it.
func TestTheLockIsAFileAPersonWrites(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "f.db")
	inst := NewInstruments(time.Now())
	store, err := OpenStore(db, inst.Registry(), NewLogger())
	if err != nil {
		t.Fatal(err)
	}
	sink := &captureSink{}
	oplog := NewOpLog(sink, inst.Registry(), NewLogger())
	locks := &lockWatch{
		path: lockPath(db),
		log:  NewLogger(),
		rec: func(op string, l *Lock) {
			_ = oplog.OpSync(
				op,
				map[string]any{
					"service": strings.Join(l.Scopes, ","),
					"reason":  l.Reason,
				},
			)
		},
	}
	srv := httptest.NewServer(
		newServerLocked(
			store,
			oplog,
			inst,
			NewLogger(),
			locks,
		).routes(),
	)
	t.Cleanup(func() { srv.Close(); store.Close() })
	post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"kingfisher","version":"v1","flags":[{"key":"k","value":{"boolValue":true},"description":"a test flag"}]}}`,
	)
	post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"dodo","version":"v1","flags":[{"key":"k","value":{"boolValue":true},"description":"a test flag"}]}}`,
	)

	// a namespace lock: kingfisher stops, dodo and Ping do not
	if _, err := writeLock(lockPath(db), []string{"kingfisher@v1"}, "restore rehearsal"); err != nil {
		t.Fatal(err)
	}
	locks.checked = time.Time{} // the test does not wait a second
	code, body := post(
		t,
		srv,
		"GetNamespace",
		`{"service":"kingfisher","version":"v1"}`,
	)
	if code != http.StatusLocked ||
		!strings.Contains(body, "restore rehearsal") {
		t.Fatalf("kingfisher under a namespace lock: %d %s", code, body)
	}
	if code, _ := post(t, srv, "GetNamespace", `{"service":"dodo","version":"v1"}`); code != 200 {
		t.Fatalf("dodo under kingfisher's lock: %d", code)
	}
	if code, _ := post(t, srv, "Ping", `{}`); code != 200 {
		t.Fatalf("Ping under a namespace lock: %d", code)
	}
	if code, _ := post(t, srv, "SetFlag", `{"service":"kingfisher","version":"v1","key":"k","value":{"boolValue":false},"reason":"x"}`); code != http.StatusLocked {
		t.Fatalf("a flip under the lock: %d", code)
	}
	code, doc := health(t, srv.URL)
	if code != 200 || doc["status"] != "degraded" {
		t.Fatalf("health under a lock: %d %v", code, doc)
	}
	if rs, _ := doc["reasons"].([]any); len(rs) == 0 ||
		!contains(rs[0].(string), "locked (kingfisher@v1)") {
		t.Fatalf("reasons: %v", doc["reasons"])
	}
	recs := sink.snapshot()
	if last := recs[len(recs)-1]; last["op"] != "Lock" ||
		last["reason"] != "restore rehearsal" {
		t.Fatalf("the lock was not recorded: %v", last)
	}

	// a whole-store lock stops Ping too, so a client learns it on the
	// liveness check
	if _, err := writeLock(lockPath(db), []string{"*"}, "restore to 4127"); err != nil {
		t.Fatal(err)
	}
	locks.checked = time.Time{}
	code, body = post(t, srv, "Ping", `{}`)
	if code != http.StatusLocked ||
		!strings.Contains(body, "restore to 4127") {
		t.Fatalf("Ping under a whole lock: %d %s", code, body)
	}
	if code, _ := post(t, srv, "GetNamespace", `{"service":"dodo","version":"v1"}`); code != http.StatusLocked {
		t.Fatalf("dodo under a whole lock: %d", code)
	}

	// released by removing the file, and recorded
	if err := os.Remove(lockPath(db)); err != nil {
		t.Fatal(err)
	}
	locks.checked = time.Time{}
	if code, _ := post(t, srv, "Ping", `{}`); code != 200 {
		t.Fatalf("after unlock: %d", code)
	}
	if _, doc := health(t, srv.URL); doc["status"] != "healthy" {
		t.Fatalf("health after unlock: %v", doc)
	}
	recs = sink.snapshot()
	if last := recs[len(recs)-1]; last["op"] != "Unlock" {
		t.Fatalf("the unlock was not recorded: %v", last)
	}
	// the revision never moved: a lock is not a store change
	if store.Revision() != 2 {
		t.Fatalf("revision moved on a lock: %d", store.Revision())
	}
}

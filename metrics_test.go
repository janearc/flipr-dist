package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestCounterAndGauge covers the primitives.
func TestCounterAndGauge(t *testing.T) {
	var c Counter
	c.Inc()
	c.Add(4)
	if c.Value() != 5 {
		t.Fatalf("counter = %d, want 5", c.Value())
	}
	var g Gauge
	g.Set(10)
	g.Inc()
	g.Dec()
	g.Dec()
	if g.Value() != 9 {
		t.Fatalf("gauge = %d, want 9", g.Value())
	}
}

// TestHistogramBucketsAreCumulative checks the rendered buckets follow the
// Prometheus rule that each bucket includes everything below it. Getting this
// wrong produces a histogram that looks plausible and quantiles that lie.
func TestHistogramBucketsAreCumulative(t *testing.T) {
	r := NewRegistry()
	r.DeclareHistogram("t_seconds", "help", "method")
	h := r.Histogram("t_seconds", "Ping")
	h.Observe(20 * time.Microsecond) // lands in the 0.00005 bucket
	h.Observe(2 * time.Millisecond)  // lands in the 0.005 bucket
	out := r.Render()

	// The +Inf bucket must equal the total count.
	if !strings.Contains(
		out,
		`t_seconds_bucket{le="+Inf",method="Ping"} 2`,
	) {
		t.Errorf("+Inf bucket wrong:\n%s", out)
	}
	if !strings.Contains(out, "t_seconds_count{method=\"Ping\"} 2") {
		t.Errorf("count wrong:\n%s", out)
	}
	// A bucket below the first observation must be zero, and one above both
	// observations must be 2 -- that is what cumulative means.
	if !strings.Contains(
		out,
		`t_seconds_bucket{le="0.00001",method="Ping"} 0`,
	) {
		t.Errorf("smallest bucket should be empty:\n%s", out)
	}
	if !strings.Contains(
		out,
		`t_seconds_bucket{le="0.05",method="Ping"} 2`,
	) {
		t.Errorf(
			"a bucket above both observations should hold "+
				"both:\n%s",
			out,
		)
	}
}

// TestObserveSince covers the common call shape.
func TestObserveSince(t *testing.T) {
	r := NewRegistry()
	r.DeclareHistogram("d_seconds", "help")
	r.Histogram("d_seconds").ObserveSince(time.Now())
	if !strings.Contains(r.Render(), "d_seconds_count 1") {
		t.Error("ObserveSince did not record")
	}
}

// TestRenderIsStable checks two renders of identical state are byte-identical,
// so a scrape diff means something changed rather than that a map reordered.
func TestRenderIsStable(t *testing.T) {
	r := NewRegistry()
	r.DeclareCounter("a_total", "help", "method")
	r.Counter("a_total", "One").Inc()
	r.Counter("a_total", "Two").Inc()
	r.DeclareGauge("z_gauge", "help", func() int64 { return 3 })
	first, second := r.Render(), r.Render()
	if first != second {
		t.Fatal("render is not deterministic")
	}
}

// TestRenderShapeIsPrometheusText checks HELP and TYPE lines are emitted for
// each family, which is what makes a scrape parse at all.
func TestRenderShapeIsPrometheusText(t *testing.T) {
	r := NewRegistry()
	r.DeclareCounter("c_total", "a counter", "method")
	r.Counter("c_total", "Ping").Add(2)
	r.DeclareGauge("g_thing", "a gauge", func() int64 { return 7 })
	out := r.Render()
	for _, want := range []string{
		"# HELP c_total a counter",
		"# TYPE c_total counter",
		`c_total{method="Ping"} 2`,
		"# HELP g_thing a gauge",
		"# TYPE g_thing gauge",
		"g_thing 7",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// TestCounterHandlesTheUnlabelledCase checks a family with no labels renders
// without an empty brace pair, which some parsers reject.
func TestCounterHandlesTheUnlabelledCase(t *testing.T) {
	r := NewRegistry()
	r.DeclareCounter("plain_total", "help")
	r.Counter("plain_total").Inc()
	out := r.Render()
	if strings.Contains(out, "plain_total{}") {
		t.Errorf("rendered empty labels:\n%s", out)
	}
	if !strings.Contains(out, "plain_total 1") {
		t.Errorf("missing sample:\n%s", out)
	}
}

// TestSameHandleIsReturnedForSameLabels checks the registry does not create a
// fresh counter per call, which would silently reset every increment.
func TestSameHandleIsReturnedForSameLabels(t *testing.T) {
	r := NewRegistry()
	r.DeclareCounter("x_total", "help", "m")
	r.Counter("x_total", "a").Inc()
	r.Counter("x_total", "a").Inc()
	if got := r.Counter("x_total", "a").Value(); got != 2 {
		t.Fatalf(
			"got %d, want 2: a new handle was created per call",
			got,
		)
	}
	once := r.Histogram("h_seconds", "a")
	again := r.Histogram("h_seconds", "a")
	if once != again {
		t.Fatal("histogram handles are not stable")
	}
}

// TestTrimFloat checks bucket bounds render without scientific notation.
func TestTrimFloat(t *testing.T) {
	for in, want := range map[float64]string{
		0.00001: "0.00001",
		1:       "1",
		0.5:     "0.5",
	} {
		if got := trimFloat(in); got != want {
			t.Errorf("trimFloat(%v) = %q, want %q", in, got, want)
		}
	}
}

// TestLabelHelpers covers the small functions the renderer leans on.
func TestLabelHelpers(t *testing.T) {
	if got := renderLabels(map[string]string{}); got != "" {
		t.Errorf("empty labels rendered as %q", got)
	}
	if got := renderLabels(map[string]string{"b": "2", "a": "1"}); got != `{a="1",b="2"}` {
		t.Errorf("labels not sorted: %s", got)
	}
	if got := zip([]string{"a", "b"}, []string{"1"}); len(got) != 1 ||
		got["a"] != "1" {
		t.Errorf("zip mishandled a short value list: %v", got)
	}
	if got := splitKey(""); got != nil {
		t.Errorf("splitKey(\"\") = %v, want nil", got)
	}
	src := map[string]string{"a": "1"}
	cp := copyLabels(src)
	cp["le"] = "x"
	if _, leaked := src["le"]; leaked {
		t.Error(
			"copyLabels aliased its input; le would leak between " +
				"buckets",
		)
	}
}

// TestInstrumentsDeclareEverything checks the process gauges are present and
// readable, since they are the ones nothing else exercises.
func TestInstrumentsDeclareEverything(t *testing.T) {
	i := NewInstruments(time.Now())
	i.RPCDone("Ping", "ok", time.Now())
	i.Rejected("off_contract")
	i.BytesIn("Ping", 10)
	i.BytesOut("Ping", 20)
	out := i.Registry().Render()
	for _, want := range []string{
		"flipr_uptime_seconds", "flipr_goroutines", "flipr_gomaxprocs",
		"flipr_memstats_alloc_bytes", "flipr_memstats_sys_bytes", "flipr_memstats_gc_total",
		`flipr_rpc_requests_total{method="Ping",outcome="ok"} 1`,
		`flipr_rpc_rejected_total{reason="off_contract"} 1`,
		`flipr_rpc_request_bytes_total{method="Ping"} 10`,
		`flipr_rpc_response_bytes_total{method="Ping"} 20`,
		"flipr_rpc_duration_seconds_count",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
}

// TestStartupGaugeIsFixedNotUptime: the gauge reported
// time.Since(started) at scrape time, which is uptime. It is zero until the
// listener opens and constant after.
func TestStartupGaugeIsFixedNotUptime(t *testing.T) {
	i := NewInstruments(time.Now().Add(-2 * time.Second))
	if !strings.Contains(
		i.Registry().Render(),
		"flipr_startup_duration_microseconds 0\n",
	) {
		t.Fatal("before serving the gauge should be zero")
	}
	i.MarkServing()
	first := i.startupMicros.Load()
	if first < 2_000_000 {
		t.Fatalf("startup = %dµs, want about two seconds", first)
	}
	time.Sleep(20 * time.Millisecond)
	i.MarkServing()
	if got := i.startupMicros.Load(); got != first {
		t.Fatalf(
			"the gauge moved after serving began: %d -> %d",
			first,
			got,
		)
	}
}

// TestCountersExistBeforeTheFirstEvent: a fresh registry
// renders every declared counter and label set at zero, so an alert on a
// rate has a series to fire on from the first scrape.
func TestCountersExistBeforeTheFirstEvent(t *testing.T) {
	out := NewInstruments(time.Now()).Registry().Render()
	for _, want := range []string{
		"flipr_oplog_sink_errors_total 0\n",
		"flipr_store_rollbacks_total 0\n",
		`flipr_rpc_requests_total{method="GetFlag",outcome="server_error"} 0` + "\n",
		`flipr_rpc_requests_total{method="DeleteNamespace",outcome="ok"} 0` + "\n",
		`flipr_rpc_rejected_total{reason="type_change"} 0` + "\n",
		`flipr_oplog_entries_total{mode="sync"} 0` + "\n",
		`flipr_rpc_duration_seconds_count{method="Ping"} 0` + "\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf(
				"fresh /metrics lacks %q",
				strings.TrimSpace(want),
			)
		}
	}
	// and every Refusal code the handlers can emit is in the touched list
	for _, code := range []string{"bad_name", "no_value", "type_change", "over_budget", "missing_reason"} {
		found := false
		for _, c := range rejectionCodes {
			found = found || c == code
		}
		if !found {
			t.Errorf(
				"rejection code %q is not touched at startup",
				code,
			)
		}
	}
}

// metricsOf reads the server's /metrics page.
func metricsOf(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// TestTheBurdenIsCountedByNamespaceAndCapped: every RPC that names a
// namespace is counted under it by method, reads add the bytes they
// served, and past the cap a new namespace is other, so a service that
// versions by commit cannot grow the series without end.
func TestTheBurdenIsCountedByNamespaceAndCapped(t *testing.T) {
	srv, _ := testServer(t)
	post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"dodo","version":"v1","flags":[{"key":"k","value":{"boolValue":true},"description":"a test flag"}]}}`,
	)
	post(t, srv, "GetNamespace", `{"service":"dodo","version":"v1"}`)
	post(t, srv, "GetFlag", `{"service":"dodo","version":"v1","key":"k"}`)
	post(t, srv, "Ping", `{}`)
	reg := metricsOf(t, srv)
	for _, want := range []string{
		`flipr_namespace_requests_total{method="PublishNamespace",namespace="dodo@v1"} 1`,
		`flipr_namespace_requests_total{method="GetNamespace",namespace="dodo@v1"} 1`,
		`flipr_namespace_requests_total{method="GetFlag",namespace="dodo@v1"} 1`,
	} {
		if !strings.Contains(reg, want) {
			t.Fatalf("missing %s in\n%s", want, reg)
		}
	}
	if strings.Contains(reg, `method="Ping",namespace="dodo@v1"`) {
		t.Fatal("Ping names no namespace")
	}
	if !regexp.MustCompile(`flipr_namespace_response_bytes_total\{namespace="dodo@v1"\} [1-9]\d+`).
		MatchString(reg) {
		t.Fatalf("reads add their bytes:\n%s", reg)
	}
	// the cap: the label is other past namespaceCap distinct namespaces
	inst := NewInstruments(time.Now())
	for i := 0; i < namespaceCap; i++ {
		if got := inst.namespaceLabel(fmt.Sprintf("svc@dev-%d", i)); got == "other" {
			t.Fatalf("namespace %d is within the cap", i)
		}
	}
	if got := inst.namespaceLabel("svc@dev-more"); got != "other" {
		t.Fatalf("past the cap: %s", got)
	}
	if got := inst.namespaceLabel("svc@dev-3"); got != "svc@dev-3" {
		t.Fatal("a namespace already named stays named")
	}
}

package main

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Every metric flipr publishes, declared in one place so the names, help text
// and label sets are reviewable as a set rather than scattered through the
// handlers.
//
// The naming follows Prometheus convention: `flipr_` prefix, base units
// (seconds, bytes), `_total` on counters. Scrape-time relabelling decides
// which environment a series belongs to, so nothing here knows whether it is
// running in prod or in a dev cluster -- dev-ness must never be a code path.
const (
	mRPCRequests     = "flipr_rpc_requests_total"
	mRPCDuration     = "flipr_rpc_duration_seconds"
	mRPCBytesIn      = "flipr_rpc_request_bytes_total"
	mRPCBytesOut     = "flipr_rpc_response_bytes_total"
	mRPCRejected     = "flipr_rpc_rejected_total"
	mStoreReload     = "flipr_store_reload_total"
	mStoreReloadS    = "flipr_store_reload_duration_seconds"
	mStoreWriteS     = "flipr_store_write_duration_seconds"
	mStoreReadS      = "flipr_store_read_duration_seconds"
	mStoreRollbacks  = "flipr_store_rollbacks_total"
	mOplogEntries    = "flipr_oplog_entries_total"
	mOplogSinkErrs   = "flipr_oplog_sink_errors_total"
	mOplogMirrorErrs = "flipr_oplog_mirror_errors_total"
	mUnchanged       = "flipr_get_namespace_unchanged_total"
	mClientRequests  = "flipr_client_requests_total"
	mNSRequests      = "flipr_namespace_requests_total"
	mNSBytes         = "flipr_namespace_response_bytes_total"
)

// namespaceCap bounds the namespace label: the first this many distinct
// namespaces are named, every one after is "other". Per-build namespaces
// (gaggle's dev-<build>, a service that versions by commit) would otherwise
// grow the series without end.
//
// Coarse on purpose: per namespace is enough to find the oversubscribed ones.
const namespaceCap = 64

// rpcMethods is every RPC the router serves, in one place, so the route
// table, the touched counters and the tests cannot disagree about the set.
var rpcMethods = []string{
	"Ping",
	"GetFlag",
	"GetNamespace",
	"SetFlag",
	"PublishNamespace",
	"ListNamespaces",
	"DeleteNamespace",
}

// rpcOutcomes is the outcome label's whole range; see outcomeFor.
var rpcOutcomes = []string{"ok", "client_error", "server_error"}

// rejectionCodes is every reason a request is refused at the boundary. A new
// Refusal code belongs here too, or its series is absent until first use.
var rejectionCodes = []string{
	"method_not_allowed", "unreadable_body", "off_contract",
	"incomplete_namespace", "over_budget", "emergency_discipline",
	"missing_reason", "bad_name", "no_value", "type_change", "locked",
	"unknown_client",
}

// Instruments holds the registry plus the handful of process-level gauges
// that are read at scrape time rather than pushed.
type Instruments struct {
	reg     *Registry
	started time.Time

	// the namespaces named so far, for the capped label (namespaceLabel)
	nsMu       sync.Mutex
	namespaces map[string]bool

	// startupMicros is the time from process start to the listener opening,
	// fixed once by MarkServing. It used to be read as time.Since(started)
	// at scrape time, which is uptime, and the gauge said so on every
	// dashboard. Zero until serving begins.
	startupMicros atomic.Int64
}

// NewInstruments declares every metric family and wires the process gauges.
func NewInstruments(started time.Time) *Instruments {
	r := NewRegistry()
	i := &Instruments{reg: r, started: started}

	r.DeclareCounter(
		mRPCRequests,
		"RPC calls served, by method and outcome.",
		"method",
		"outcome",
	)
	r.DeclareHistogram(mRPCDuration, "RPC wall time, by method.", "method")
	r.DeclareCounter(
		mRPCBytesIn,
		"Request body bytes read, by method.",
		"method",
	)
	r.DeclareCounter(
		mRPCBytesOut,
		"Response body bytes written, by method.",
		"method",
	)
	r.DeclareCounter(
		mRPCRejected,
		"Requests refused at the boundary, by reason.",
		"reason",
	)

	r.DeclareCounter(mStoreReload, "Snapshot rebuilds since start.")
	r.DeclareHistogram(
		mStoreReloadS,
		"Time to rebuild the in-memory snapshot from disk.",
	)
	r.DeclareHistogram(
		mStoreWriteS,
		"Time for a durable write, bbolt commit included.",
	)
	r.DeclareHistogram(
		mStoreReadS,
		"Time to serve a read from the snapshot.",
	)
	r.DeclareCounter(
		mStoreRollbacks,
		"Writes rolled back because their op log record could not be "+
			"written.",
	)

	r.DeclareCounter(
		mOplogEntries,
		"Op log entries written, by mode.",
		"mode",
	)
	r.DeclareCounter(mOplogSinkErrs, "Op log sink write failures.")
	r.DeclareCounter(
		mUnchanged,
		"GetNamespace answers that said unchanged: the caller held "+
			"the current revision and was given nothing else.",
	)
	r.DeclareCounter(
		mClientRequests,
		"RPCs by the client that made them: a known <lang>/<tag>, "+
			"fliprctl, other (a header the known set does not "+
			"hold) or none (no header). Bounded by the set in "+
			"flipr@clients.",
		"client",
		"method",
	)
	r.DeclareCounter(
		mNSRequests,
		"RPCs that named a namespace, by namespace and method: the "+
			"burden each namespace puts on flipr. The first 64 "+
			"namespaces seen are named; the rest are other.",
		"namespace",
		"method",
	)
	r.DeclareCounter(
		mNSBytes,
		"Bytes served by reads of a namespace (GetFlag, "+
			"GetNamespace): with the request rate, what each "+
			"namespace costs. Same cap.",
		"namespace",
	)
	r.DeclareCounter(
		mOplogMirrorErrs,
		"Writes the kafka mirror refused while the file held the "+
			"record.",
	)

	// Touch every counter and label set now. A counter that does not exist
	// until its first increment has no series to alert on: a fresh pod had
	// no flipr_oplog_blocked_total and no flipr_oplog_sink_errors_total
	// line, and max_over_time over three days returned nothing.
	//
	// Zero is a value; absent is a question.
	for _, m := range rpcMethods {
		for _, o := range rpcOutcomes {
			r.Counter(mRPCRequests, m, o)
		}
		r.Counter(mRPCBytesIn, m)
		r.Counter(mRPCBytesOut, m)
		r.Histogram(mRPCDuration, m)
	}
	for _, c := range rejectionCodes {
		r.Counter(mRPCRejected, c)
	}
	for _, mode := range []string{"async", "sync"} {
		r.Counter(mOplogEntries, mode)
	}
	r.Counter(mOplogSinkErrs)
	r.Counter(mUnchanged)
	r.Counter(mOplogMirrorErrs)
	// other and none exist from the start so the dashboard's panel is never
	// empty; known clients appear as they are seen
	for _, m := range rpcMethods {
		r.Counter(mClientRequests, clientOther, m)
		r.Counter(mClientRequests, clientNone, m)
	}
	r.Counter(mStoreReload)
	r.Counter(mStoreRollbacks)
	r.Histogram(mStoreReloadS)
	r.Histogram(mStoreWriteS)
	r.Histogram(mStoreReadS)

	// Process gauges. Some of these duplicate what metricsd already
	// collects for the host; that overlap is deliberate and deconflicting
	// it is a scrape-time decision, not a reason to leave flipr
	// uninstrumented.
	r.DeclareGauge("flipr_uptime_seconds", "Seconds since start.",
		func() int64 { return int64(time.Since(i.started).Seconds()) })
	r.DeclareGauge(
		"flipr_startup_duration_microseconds",
		"Time from process start to serving, in microseconds. Zero "+
			"until the listener is open.",
		func() int64 { return i.startupMicros.Load() },
	)
	r.DeclareGauge("flipr_goroutines", "Goroutines currently running.",
		func() int64 { return int64(runtime.NumGoroutine()) })
	r.DeclareGauge("flipr_gomaxprocs", "GOMAXPROCS.",
		func() int64 { return int64(runtime.GOMAXPROCS(0)) })
	r.DeclareGauge(
		"flipr_memstats_alloc_bytes",
		"Heap bytes currently allocated.",
		func() int64 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			return int64(m.Alloc)
		},
	)
	r.DeclareGauge(
		"flipr_memstats_sys_bytes",
		"Bytes obtained from the OS.",
		func() int64 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			return int64(m.Sys)
		},
	)
	r.DeclareGauge(
		"flipr_memstats_gc_total",
		"Completed GC cycles.",
		func() int64 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			return int64(m.NumGC)
		},
	)

	return i
}

// MarkServing fixes the startup duration: called once, when the listener is
// about to open. Later calls do not move it.
func (i *Instruments) MarkServing() {
	i.startupMicros.CompareAndSwap(0, time.Since(i.started).Microseconds())
}

// Registry exposes the underlying registry for rendering.
func (i *Instruments) Registry() *Registry { return i.reg }

// NamespaceRequest counts one RPC under the namespace it named, capped.
func (i *Instruments) NamespaceRequest(namespace, method string) {
	i.reg.Counter(mNSRequests, i.namespaceLabel(namespace), method).Inc()
}

// NamespaceBytes adds the bytes a read of a namespace served.
func (i *Instruments) NamespaceBytes(namespace string, n int) {
	i.reg.Counter(mNSBytes, i.namespaceLabel(namespace)).Add(uint64(n))
}

// namespaceLabel names a namespace until the cap, then says other.
func (i *Instruments) namespaceLabel(namespace string) string {
	i.nsMu.Lock()
	defer i.nsMu.Unlock()
	if i.namespaces == nil {
		i.namespaces = map[string]bool{}
	}
	if i.namespaces[namespace] {
		return namespace
	}
	if len(i.namespaces) >= namespaceCap {
		return "other"
	}
	i.namespaces[namespace] = true
	return namespace
}

// ClientRequest counts one RPC under the client that made it (clients.go).
func (i *Instruments) ClientRequest(client, method string) {
	i.reg.Counter(mClientRequests, client, method).Inc()
}

// RPCDone records one completed RPC: its outcome and how long it took.
func (i *Instruments) RPCDone(method, outcome string, start time.Time) {
	i.reg.Counter(mRPCRequests, method, outcome).Inc()
	i.reg.Histogram(mRPCDuration, method).ObserveSince(start)
}

// Rejected records a request refused at the boundary before it reached a
// handler, so a spike in off-contract traffic is visible rather than silent.
func (i *Instruments) Rejected(
	reason string,
) {
	i.reg.Counter(mRPCRejected, reason).Inc()
}

// BytesIn records request body size for a method.
func (i *Instruments) BytesIn(method string, n int) {
	i.reg.Counter(mRPCBytesIn, method).Add(uint64(n))
}

// BytesOut records response body size for a method.
func (i *Instruments) BytesOut(method string, n int) {
	i.reg.Counter(mRPCBytesOut, method).Add(uint64(n))
}

// Unchanged counts a GetNamespace answered from the revision alone.
func (i *Instruments) Unchanged() { i.reg.Counter(mUnchanged).Inc() }

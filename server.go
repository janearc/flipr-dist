package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pb "github.com/janearc/flipr-dist/gen/fliprpb"
)

// The HTTP surface. Split from main so it can be driven by a test without
// binding a port or installing signal handlers: main owns the process,
// server owns the behaviour.

// server holds everything a request needs.
type server struct {
	locks *lockWatch // nil: no lock file configured (tests)
	// the known client set, read from the snapshot by revision (clients.go)
	clients clientSets
	store   *Store
	log     *OpLog
	slog    *Logger
	inst    *Instruments

	// lastHealth is the status /health last answered, so the log line is
	// written when the status changes rather than on every answer.
	//
	// A probe reading /health once a second against a degraded flipr would
	// otherwise write a warn line a second into the collector for as long
	// as it lasts, and a line that repeats every second says nothing the
	// first one did not.
	lastHealth atomic.Value // holds string
}

// newServer wires a store, an oplog and the instruments into a handler set.
func newServer(
	store *Store,
	oplog *OpLog,
	inst *Instruments,
	lg *Logger,
) *server {
	return &server{store: store, log: oplog, slog: lg, inst: inst}
}

// newServerLocked is newServer with the lock file watched (lock.go).
func newServerLocked(
	store *Store,
	oplog *OpLog,
	inst *Instruments,
	lg *Logger,
	locks *lockWatch,
) *server {
	s := newServer(store, oplog, inst, lg)
	s.locks = locks
	return s
}

// routes returns the complete mux. One route per proto method, named for it,
// so the route table and the contract cannot drift apart.
func (s *server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	handlers := map[string]http.HandlerFunc{
		"Ping":             s.handlePing,
		"GetFlag":          s.handleGetFlag,
		"GetNamespace":     s.handleGetNamespace,
		"SetFlag":          s.handleSetFlag,
		"PublishNamespace": s.handlePublishNamespace,
		"ListNamespaces":   s.handleListNamespaces,
		"DeleteNamespace":  s.handleDeleteNamespace,
	}
	// one list, rpcMethods, names the set: a method missing a handler here
	// is a panic at construction, not a 404 in production
	for _, m := range rpcMethods {
		h, ok := handlers[m]
		if !ok {
			panic(
				"rpcMethods names " + m +
					" and routes has no handler for it",
			)
		}
		mux.HandleFunc(
			"/flipr.v1.FliprService/"+m,
			s.instrumented(m, h),
		)
	}
	mux.HandleFunc("/api", s.handleAPI)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)
	return mux
}

// statusWriter remembers the status code and byte count so the middleware can
// record an outcome without each handler reporting its own.
type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

// WriteHeader records the status on its way through.
func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Write records the byte count on its way through.
func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

// instrumented wraps a handler so every RPC is counted and timed, whatever it
// returns. Doing it here rather than in each handler is what makes "every RPC
// is measured" a property of the router instead of a habit.
func (s *server) instrumented(
	method string,
	h http.HandlerFunc,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		h(sw, r)
		s.inst.BytesOut(method, sw.bytes)
		s.inst.RPCDone(method, outcomeFor(sw.status), start)
	}
}

// outcomeFor buckets a status code into ok, client_error or server_error, so
// the outcome label stays low-cardinality.
func outcomeFor(status int) string {
	switch {
	case status == 0 || status < 400:
		return "ok"
	case status < 500:
		return "client_error"
	default:
		return "server_error"
	}
}

// handlePing answers the liveness check every client makes before trusting its
// cache. It deliberately does not touch the store: every service in the mesh
// calls this, so it must never be why flipr is slow.
func (s *server) handlePing(w http.ResponseWriter, r *http.Request) {
	// A whole-store lock stops Ping too, so a client learns the lock on the
	// liveness check it was making anyway and remembers it (lock.go); the
	// check is a cached stat, so the ping stays free.
	if l := s.lockFor("Ping", nil); l != nil {
		s.inst.Rejected("locked")
		w.Header().Set("Retry-After", "5")
		s.fail(w, http.StatusLocked, "locked: "+l.Reason)
		return
	}
	// and the client, on the liveness check too, so a service behind an
	// unknown client learns the refusal on its first request (clients.go)
	if !s.admit(w, r, "Ping") {
		return
	}
	// Version, generation and revision only -- still no store READ, all
	// three are held in memory from open. The liveness check stays free.
	s.reply(
		w,
		"Ping",
		&pb.PingResponse{
			Version:         Version,
			StoreGeneration: s.store.Generation(),
			Revision:        s.store.Revision(),
		},
	)
}

// handleGetFlag returns one flag, or 404 if it is not declared.
func (s *server) handleGetFlag(w http.ResponseWriter, r *http.Request) {
	var req pb.GetFlagRequest
	if !s.decode(w, r, "GetFlag", &req) {
		return
	}
	f, ok := s.store.GetFlag(req.Service, req.Version, req.Key)
	if !ok {
		s.fail(
			w,
			http.StatusNotFound,
			fmt.Sprintf(
				"no flag %q in %s@%s",
				req.Key,
				req.Service,
				req.Version,
			),
		)
		return
	}
	n := s.reply(
		w,
		"GetFlag",
		&pb.GetFlagResponse{Flag: f, Revision: s.store.Revision()},
	)
	s.inst.NamespaceBytes(req.Service+"@"+req.Version, n)
}

// handleGetNamespace returns one service-at-a-version and all of its flags.
// This is the call a client makes at startup to fill its cache in one trip.
func (s *server) handleGetNamespace(w http.ResponseWriter, r *http.Request) {
	var req pb.GetNamespaceRequest
	if !s.decode(w, r, "GetNamespace", &req) {
		return
	}
	// The checkpoint: one integer against one integer before the store is
	// read. A caller that holds the current revision is told so and given
	// nothing else, which is what a consumer polling on its interval costs
	// Zero means everything.
	rev := s.store.Revision()
	if req.Since != 0 && req.Since == rev {
		s.inst.Unchanged()
		s.reply(
			w,
			"GetNamespace",
			&pb.GetNamespaceResponse{
				Revision:  rev,
				Unchanged: true,
			},
		)
		return
	}
	ns, ok := s.store.GetNamespace(req.Service, req.Version)
	if !ok {
		s.fail(
			w,
			http.StatusNotFound,
			fmt.Sprintf(
				"no namespace %s@%s",
				req.Service,
				req.Version,
			),
		)
		return
	}
	n := s.reply(
		w,
		"GetNamespace",
		&pb.GetNamespaceResponse{Namespace: ns, Revision: rev},
	)
	s.inst.NamespaceBytes(req.Service+"@"+req.Version, n)
}

// handleSetFlag is the flip. It refuses without a stated reason and logs
// synchronously, so a flip that was acknowledged always left a trace.
func (s *server) handleSetFlag(w http.ResponseWriter, r *http.Request) {
	var req pb.SetFlagRequest
	if !s.decode(w, r, "SetFlag", &req) {
		return
	}
	if s.refused(
		w,
		r,
		"SetFlag",
		req.Service,
		req.Version,
		req.Key,
		checkNamespaceName(
			req.Service,
			req.Version,
		),
		checkKey(req.Key),
		checkReason(req.Reason),
	) {
		return
	}
	// The record is written inside the store's write: after the commit, and
	// undone with it if the record fails. See Store.transact.
	f, err := s.store.Flip(
		req.Service,
		req.Version,
		req.Key,
		req.Value,
		func(*pb.Flag) error {
			vj, _ := marshal.Marshal(req.Value)
			return s.log.OpSync("SetFlag", map[string]any{
				"revision": s.store.Revision(),
				"service":  req.Service,
				"version":  req.Version,
				"key":      req.Key,
				"value": valueString(
					req.Value,
				),
				"value_json": string(vj),
				"reason":     req.Reason,
				"remote":     r.RemoteAddr,
				"caller":     r.Header.Get("X-Flipr-Caller"),
			})
		},
	)
	if err != nil {
		s.writeFailed(
			w,
			r,
			"SetFlag",
			req.Service,
			req.Version,
			req.Key,
			err,
		)
		return
	}
	s.reply(
		w,
		"SetFlag",
		&pb.SetFlagResponse{Flag: f, Revision: s.store.Revision()},
	)
}

// handlePublishNamespace is build-time onboarding: a service declares which
// flags it has. Idempotent, and it never overwrites an operator's value.
func (s *server) handlePublishNamespace(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req pb.PublishNamespaceRequest
	if !s.decode(w, r, "PublishNamespace", &req) {
		return
	}
	if req.Namespace == nil || req.Namespace.Service == "" ||
		req.Namespace.Version == "" {
		s.slog.Warn(
			"rejected: incomplete namespace",
			map[string]any{"remote": r.RemoteAddr},
		)
		s.inst.Rejected("incomplete_namespace")
		s.fail(
			w,
			http.StatusBadRequest,
			"namespace needs both a service and a version (the "+
				"commit hash at deploy)",
		)
		return
	}
	if s.refused(
		w,
		r,
		"PublishNamespace",
		req.Namespace.Service,
		req.Namespace.Version,
		"",
		checkNamespaceName(
			req.Namespace.Service,
			req.Namespace.Version,
		),
	) {
		return
	}
	for _, f := range req.Namespace.Flags {
		if s.refused(
			w,
			r,
			"PublishNamespace",
			req.Namespace.Service,
			req.Namespace.Version,
			f.GetKey(),
			checkKey(f.GetKey()),
		) {
			return
		}
	}
	// The emergency-set discipline, enforced where it cannot regress. Off
	// stops behaviour, uniformly, with no room for whimsy on the emergency
	// page.
	//
	// Every expensive flag lands on the break-glass surface, so every
	// expensive flag must be killable and legible under the worst
	// conditions. The budget.
	//
	// Nobody needs more than 24 flags in one namespace: flipr is not an a/b
	// testing system, it is a realtime persistent config store. A namespace
	// that wants flag 25 is dumping configuration into the switchboard, and
	// the switchboard being small is what keeps it usable at 03:20.
	if len(req.Namespace.Flags) > 24 {
		s.inst.Rejected("over_budget")
		s.slog.Warn(
			"rejected: namespace over the flag budget",
			map[string]any{
				"service": req.Namespace.Service,
				"flags":   len(req.Namespace.Flags),
				"remote":  r.RemoteAddr},
		)
		s.fail(w, http.StatusBadRequest, fmt.Sprintf(
			"%d flags is over flipr's budget of 24 per "+
				"namespace. flipr is a realtime persistent "+
				"config store, not an a/b testing "+
				"system -- static configuration "+
				"belongs in the "+
				"deploy, and if this service "+
				"genuinely needs more switches, "+
				"that is a conversation "+
				"with the operator, not a bigger publish",
			len(req.Namespace.Flags),
		))
		return
	}
	for _, f := range req.Namespace.Flags {
		if why := lintExpensive(f); why != "" {
			s.inst.Rejected("emergency_discipline")
			s.slog.Warn(
				"rejected: expensive flag fails the "+
					"emergency-set discipline",
				map[string]any{
					"service": req.Namespace.Service,
					"key":     f.Key,
					"why":     why,
					"remote":  r.RemoteAddr},
			)
			s.fail(
				w,
				http.StatusBadRequest,
				fmt.Sprintf("flag %q: %s", f.Key, why),
			)
			return
		}
	}
	n, err := s.store.Publish(req.Namespace, func(n int) error {
		return s.log.OpSync("PublishNamespace", map[string]any{
			"revision": s.store.Revision(),
			"service":  req.Namespace.Service,
			"version":  req.Namespace.Version,
			"flags":    n,
		})
	})
	if err != nil {
		s.writeFailed(
			w,
			r,
			"PublishNamespace",
			req.Namespace.Service,
			req.Namespace.Version,
			"",
			err,
		)
		return
	}
	s.reply(
		w,
		"PublishNamespace",
		&pb.PublishNamespaceResponse{
			FlagsPublished: int32(n),
			Revision:       s.store.Revision(),
		},
	)
}

// handleDeleteNamespace retires one service@version: reason required,
// logged synchronously before the response like every write -- and unlike
// most writes, the oplog record is also what keeps the delete alive through
// a wipe, because replay applies deletes in order.
func (s *server) handleDeleteNamespace(w http.ResponseWriter, r *http.Request) {
	var req pb.DeleteNamespaceRequest
	if !s.decode(w, r, "DeleteNamespace", &req) {
		return
	}
	if s.refused(
		w,
		r,
		"DeleteNamespace",
		req.Service,
		req.Version,
		"",
		checkNamespaceName(
			req.Service,
			req.Version,
		),
		checkReason(req.Reason),
	) {
		return
	}
	n, err := s.store.Retire(req.Service, req.Version, func(n int) error {
		return s.log.OpSync("DeleteNamespace", map[string]any{
			"revision": s.store.Revision(),
			"service":  req.Service,
			"version":  req.Version,
			"reason":   req.Reason,
			"flags":    n,
			"remote":   r.RemoteAddr,
			"caller":   r.Header.Get("X-Flipr-Caller"),
		})
	})
	if err != nil {
		s.writeFailed(
			w,
			r,
			"DeleteNamespace",
			req.Service,
			req.Version,
			"",
			err,
		)
		return
	}
	s.slog.Info("namespace retired", map[string]any{
		"service":       req.Service,
		"version":       req.Version,
		"flags_removed": n,
		"reason":        req.Reason})
	s.reply(
		w,
		"DeleteNamespace",
		&pb.DeleteNamespaceResponse{
			FlagsRemoved: int32(n),
			Revision:     s.store.Revision(),
		},
	)
}

// handleListNamespaces returns every namespace held. The home page's call.
func (s *server) handleListNamespaces(w http.ResponseWriter, r *http.Request) {
	s.reply(
		w,
		"ListNamespaces",
		&pb.ListNamespacesResponse{
			Namespaces: s.store.ListNamespaces(),
			Revision:   s.store.Revision(),
		},
	)
}

// handleAPI publishes flipr's own contract as a FileDescriptorSet, so the
// service answers "what is your API" itself rather than a human answering from
// a document. Built by buf from the same bytes the Go types came from, so it
// cannot drift from the implementation.
func (s *server) handleAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Flipr-Version", Version)
	w.Write(descriptor)
}

// handleHealth says healthy, degraded or down, and names what is wrong.
//
// An uncritical 200 is the failure this estate keeps finding, so this reads
// the store rather than trusting that it opened.
//
// It also looks at what a readable store cannot tell you: whether the oplog
// sink refused a write in the last minute, and whether the last write failed
// or could not be undone.
//
// Each of those is degraded, meaning reads are served and true while flips
// are failing or unrecorded, which the operator must see on the page the
// probes read.
//
// Down is a 503. Degraded is a 200 with the truth in the body, because a
// restart fixes neither.
func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := s.store.Healthy(); err != nil {
		s.healthTransition(
			"down",
			map[string]any{"reason": err.Error()},
		)
		w.WriteHeader(http.StatusServiceUnavailable)
		s.healthBody(
			w,
			"down",
			[]string{"store unreadable: " + err.Error()},
		)
		return
	}
	var reasons []string
	if s.locks != nil {
		if l := s.locks.current(); l != nil {
			reasons = append(
				reasons,
				fmt.Sprintf(
					"locked (%s) by %s at %s: %s",
					strings.Join(l.Scopes, ","),
					l.By,
					l.At.Format(time.RFC3339),
					l.Reason,
				),
			)
		}
	}
	if failed, at := s.log.SinkFailedRecently(); failed {
		reasons = append(
			reasons,
			fmt.Sprintf(
				"op log sink refused a write %s ago; flips "+
					"are failing or unrecorded",
				time.Since(at).Round(time.Second),
			),
		)
	}
	if f, failed := s.store.LastWriteFailure(); failed {
		reasons = append(
			reasons,
			fmt.Sprintf(
				"last store write failed %s ago and no write "+
					"has succeeded since: %s",
				time.Since(f.at).Round(time.Second),
				f.err.Error(),
			),
		)
	}
	if len(reasons) > 0 {
		s.healthTransition(
			"degraded",
			map[string]any{"reasons": reasons},
		)
		s.healthBody(w, "degraded", reasons)
		return
	}
	s.healthTransition("healthy", nil)
	s.healthBody(w, "healthy", nil)
}

// healthTransition logs a health status once, when it becomes that status.
// Down and degraded are errors and warnings; a return to healthy is worth an
// info line so the log shows the episode's end as well as its start.
func (s *server) healthTransition(status string, fields map[string]any) {
	prev, _ := s.lastHealth.Load().(string)
	if prev == status {
		return
	}
	s.lastHealth.Store(status)
	if fields == nil {
		fields = map[string]any{}
	}
	fields["previous"] = prev
	switch status {
	case "down":
		s.slog.Error("/health now answering DOWN", fields)
	case "degraded":
		s.slog.Warn("/health now answering DEGRADED", fields)
	default:
		if prev != "" {
			s.slog.Info("/health answering healthy again", fields)
		}
	}
}

// healthBody writes the health document. "healthy" stays a boolean for the
// probes and clients that read it as one; "status" is the three-valued
// truth; "reasons" is empty when there is nothing to say.
func (s *server) healthBody(
	w http.ResponseWriter,
	status string,
	reasons []string,
) {
	if reasons == nil {
		reasons = []string{}
	}
	out, _ := json.Marshal(map[string]any{
		"healthy":        status != "down",
		"status":         status,
		"reasons":        reasons,
		"version":        Version,
		"generation":     s.store.Generation(),
		"revision":       s.store.Revision(),
		"uptime_seconds": int(time.Since(s.inst.started).Seconds()),
	})
	w.Write(out)
	w.Write([]byte("\n"))
}

// handleMetrics renders the whole registry plus the store's own gauges.
func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	io.WriteString(w, s.inst.Registry().Render())
}

// writeFailed answers a write that did not complete. A Refusal is a 400 and
// nothing was touched.
//
// An Unlogged write is a 500 that says whether the store was rolled back; when
// it was not, the message says so, because a caller who is told "failed" while
// readers see the new value is the one lie this service must never tell.
// Anything else is the store's own error.
func (s *server) writeFailed(
	w http.ResponseWriter,
	r *http.Request,
	method, service, version, key string,
	err error,
) {
	var ref *Refusal
	if errors.As(err, &ref) {
		s.refused(w, r, method, service, version, key, ref)
		return
	}
	fields := map[string]any{
		"method":  method,
		"service": service,
		"version": version,
		"key":     key,
		"err":     err.Error(),
	}
	var un *Unlogged
	if errors.As(err, &un) {
		if un.RolledBack {
			s.slog.Error(
				"write committed, could not be logged, and "+
					"was rolled back; refusing the "+
					"response",
				fields,
			)
		} else {
			s.slog.Error(
				"write committed, could not be logged, and "+
					"could not be rolled back; the store "+
					"serves a value the op log does not "+
					"hold",
				fields,
			)
		}
		s.fail(w, http.StatusInternalServerError, method+" "+un.Error())
		return
	}
	s.slog.Error(method+" failed in the store", fields)
	s.fail(w, http.StatusInternalServerError, err.Error())
}

// refused answers the first non-nil Refusal among checks with a 400, counts
// it under its code, and says so in the log with the identifiers the caller
// sent. Returns true when the handler must stop. Refusals are client errors
// by definition: the store was not touched.
func (s *server) refused(
	w http.ResponseWriter,
	r *http.Request,
	method, service, version, key string,
	checks ...*Refusal,
) bool {
	for _, ref := range checks {
		if ref == nil {
			continue
		}
		s.inst.Rejected(ref.Code)
		s.slog.Warn("rejected: "+ref.Why, map[string]any{
			"method":  method,
			"code":    ref.Code,
			"service": service,
			"version": version,
			"key":     key,
			"remote":  r.RemoteAddr})
		s.fail(w, http.StatusBadRequest, ref.Why)
		return true
	}
	return false
}

// unmarshal refuses unknown fields. The generated types are lenient by design,
// so the boundary has to be strict here or an off-contract message arrives.
var unmarshal = protojson.UnmarshalOptions{DiscardUnknown: false}

// marshal emits zero values so a client never has to guess whether a field was
// absent or false.
var marshal = protojson.MarshalOptions{EmitUnpopulated: true}

// decode reads and validates a request body, answering the client itself if it
// is malformed. Returns false when the caller should stop.
func (s *server) decode(
	w http.ResponseWriter,
	r *http.Request,
	method string,
	m proto.Message,
) bool {
	if r.Method != http.MethodPost {
		s.inst.Rejected("method_not_allowed")
		s.slog.Warn("rejected: method not allowed", map[string]any{
			"method":      method,
			"http_method": r.Method,
			"remote":      r.RemoteAddr})
		s.fail(w, http.StatusMethodNotAllowed, "POST only")
		return false
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		s.inst.Rejected("unreadable_body")
		s.slog.Warn("rejected: unreadable body", map[string]any{
			"method": method,
			"remote": r.RemoteAddr,
			"err":    err.Error()})
		s.fail(w, http.StatusBadRequest, "unreadable body")
		return false
	}
	s.inst.BytesIn(method, len(body))
	if len(strings.TrimSpace(string(body))) == 0 {
		body = []byte("{}")
	}
	if err := unmarshal.Unmarshal(body, m); err != nil {
		s.inst.Rejected("off_contract")
		s.slog.Warn("rejected: off-contract request", map[string]any{
			"method": method,
			"remote": r.RemoteAddr,
			"err":    err.Error()})
		s.fail(
			w,
			http.StatusBadRequest,
			"off-contract request: "+err.Error(),
		)
		return false
	}
	// THE LOCK, after the request is understood and before anything is read
	// or written: a person's word on the host (lock.go).
	//
	// Whole-store locks stop every RPC including Ping, so a client learns
	// the lock on the liveness check it was making anyway; a namespace lock
	// stops the RPCs that name that namespace and nothing else.
	if l := s.lockFor(method, m); l != nil {
		s.inst.Rejected("locked")
		w.Header().Set("Retry-After", "5")
		s.fail(w, http.StatusLocked, "locked: "+l.Reason)
		return false
	}
	// The client, after the lock: counted under the name the known set
	// gives it, refused as unknown_client only when flipr@clients says so
	// (clients.go)
	if !s.admit(w, r, method) {
		return false
	}
	// The burden, by namespace: which namespaces are read and written how
	// often, coarse on purpose (instrument.go, NamespaceRequest)
	if svc, ver := namespaceOf(m); svc != "" {
		s.inst.NamespaceRequest(svc+"@"+ver, method)
	}
	return true
}

// lockFor returns the lock that stops this request, or nil.
func (s *server) lockFor(method string, m proto.Message) *Lock {
	if s.locks == nil {
		return nil
	}
	l := s.locks.current()
	if l == nil {
		return nil
	}
	for _, sc := range l.Scopes {
		if sc == "*" {
			return l
		}
	}
	// a namespace lock: the request names its namespace, or it is not
	// covered
	service, version := namespaceOf(m)
	if service != "" && l.Covers(service, version) {
		return l
	}
	return nil
}

// namespaceOf is the namespace a request names, or empty for the RPCs that
// name none (Ping, ListNamespaces).
func namespaceOf(m proto.Message) (service, version string) {
	switch req := m.(type) {
	case *pb.GetFlagRequest:
		return req.Service, req.Version
	case *pb.GetNamespaceRequest:
		return req.Service, req.Version
	case *pb.SetFlagRequest:
		return req.Service, req.Version
	case *pb.DeleteNamespaceRequest:
		return req.Service, req.Version
	case *pb.PublishNamespaceRequest:
		if req.Namespace != nil {
			return req.Namespace.Service, req.Namespace.Version
		}
	}
	return "", ""
}

// reply marshals a protojson response and reports the bytes it wrote.
func (s *server) reply(
	w http.ResponseWriter,
	method string,
	m proto.Message,
) int {
	out, err := marshal.Marshal(m)
	if err != nil {
		s.fail(w, http.StatusInternalServerError, err.Error())
		return 0
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
	w.Write([]byte("\n"))
	return len(out) + 1
}

// fail writes a JSON error with the given status.
func (s *server) fail(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	fmt.Fprintf(w, `{"error":%q}`+"\n", msg)
}

// negatedName spots keys that would make "off" a double negative on the
// emergency page.
var negatedName = regexp.MustCompile(
	`(?i)(^|[._-])(disable|skip|suppress|block|deny|no|dont|` +
		`inhibit|mute|off)([._-]|$)`,
)

// lintExpensive is the emergency-set discipline: the mechanical, evaluable
// core of "off should stop behavior, uniformly". Returns "" when the flag
// conforms, otherwise the reason a publish is refused. Cheap flags pass
// untouched -- the discipline binds the break-glass surface, not every knob.
//
//   - expensive means boolean: the relays kill booleans, and an expensive
//     string would be the one flag the oh-shit page cannot stop (the
//     killable-boolean rule, mechanized)
//   - true means the behavior happens, false stops it, and the description
//     states BOTH halves explicitly: "on: ... off: ..."
//   - no negated names: "off" must never be a double negative at the exact
//     moment nobody can afford one
func lintExpensive(f *pb.Flag) string {
	if f == nil || !f.Expensive {
		return ""
	}
	if f.Value == nil {
		return "expensive flags carry a value"
	}
	if _, ok := f.Value.Kind.(*pb.Value_BoolValue); !ok {
		return "expensive flags must be BOOLEAN: the emergency " +
			"page kills booleans, and an expensive " +
			"string or int would be the one flag the " +
			"oh-shit page cannot stop. Split it: a " +
			"boolean " +
			"gates the spend, a cheap flag configures beneath it"
	}
	if negatedName.MatchString(f.Key) {
		return "expensive flag names must be positive " +
			"capabilities: a negated name makes \"off\" a " +
			"double negative on the emergency page"
	}
	d := strings.ToLower(f.Description)
	if !strings.Contains(d, "on:") || !strings.Contains(d, "off:") {
		return "expensive flag descriptions must state both " +
			"polarities explicitly, in the form " +
			"\"on: <what happens, cost named>. off: " +
			"<what stops>.\" -- the emergency page is " +
			"read " +
			"at 03:20 and there is no room for inference there"
	}
	return ""
}

// valueString renders a flag value for the oplog, where it is one field in a
// line of JSON rather than a typed thing.
func valueString(v *pb.Value) string {
	switch k := v.GetKind().(type) {
	case *pb.Value_BoolValue:
		if k.BoolValue {
			return "true"
		}
		return "false"
	case *pb.Value_StringValue:
		return k.StringValue
	case *pb.Value_IntValue:
		return fmt.Sprintf("%d", k.IntValue)
	default:
		return ""
	}
}

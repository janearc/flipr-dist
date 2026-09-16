package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	pb "github.com/janearc/flipr-dist/gen/fliprpb"
)

// The client announces itself: X-Flipr-Client: <lang>/<tag>/<hash>, the
// hash over the client's own source at that tag
// (clients/CONTRACT.md sections 1 and 8).
//
// Flipr does not verify the hash. It looks the triple up in a set an
// operator keeps, and counts the request under the name it finds, or
// `other` for a triple the set lacks, or `none` for no header at all.
//
// Attestation is not the point. The point is that a copy announces itself
// as a copy, and the dashboard shows it before anything refuses it.
//
// The set is flags, in a namespace flipr owns, flipr@clients, read from
// the snapshot like any flag.
//
// So a client roll never bounces flipr: `fliprctl clients add
// go/v1.1.0/<hash>` writes one flag through the RPC path an operator flip
// takes, and the next request sees it.
//
// A `known.*` entry holds one triple as its value, and the key is only a
// handle.
//
// `refuse.other` turns refusal on, off by default. Refused means 403 with
// code unknown_client on every RPC, Ping included, so a service behind an
// unknown client goes loud under its own policy rather than quietly serving
// a stale cache.
//
// The tool is known by construction. `flipr ctl` is this binary; it sends
// fliprctl/<Version>/- and is known when its Version is the server's, which
// is the same rule as any other triple (build it from the deployed commit)
// with the hash standing in for itself.

// clientsService and clientsVersion name the namespace flipr owns.
const (
	clientsService = "flipr"
	clientsVersion = "clients"
	keyRefuseOther = "refuse.other"
	knownPrefix    = "known."
	clientHeader   = "X-Flipr-Client"
	// labels for a request that named nothing the set holds
	clientOther = "other"
	clientNone  = "none"
	// the tool's own language on the wire
	clientTool = "fliprctl"
)

// clientSet is the known set as read at one revision: lang/tag to hash, and
// whether `other` and `none` are refused.
type clientSet struct {
	revision uint64
	known    map[string]string
	refuse   bool
}

// clientSets caches the set by revision: a write anywhere moves the
// revision, so a hit is exact and a miss costs one namespace read.
type clientSets struct {
	mu  sync.Mutex
	cur *clientSet
}

// current returns the set as of the store's revision, rebuilding it from the
// snapshot when the revision moved.
func (c *clientSets) current(store *Store) *clientSet {
	rev := store.Revision()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cur != nil && c.cur.revision == rev {
		return c.cur
	}
	set := &clientSet{revision: rev, known: map[string]string{}}
	if ns, ok := store.GetNamespace(clientsService, clientsVersion); ok {
		for _, f := range ns.GetFlags() {
			switch {
			case f.GetKey() == keyRefuseOther:
				set.refuse = f.GetValue().GetBoolValue()
			case strings.HasPrefix(f.GetKey(), knownPrefix):
				// game: allow width
				if lang, tag, hash, ok := splitClient(f.GetValue().GetStringValue()); ok {
					set.known[lang+"/"+tag] = hash
				}
			}
		}
	}
	c.cur = set
	return set
}

// splitClient parses <lang>/<tag>/<hash>; every part must be present.
func splitClient(v string) (lang, tag, hash string, ok bool) {
	parts := strings.Split(strings.TrimSpace(v), "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" ||
		parts[2] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

// clientLabel classifies one request's X-Flipr-Client against the set:
// "<lang>/<tag>" when the triple is known, "fliprctl" for the tool built
// from this server's commit, "other" for a header the set does not hold,
// "none" for no header.
func clientLabel(header string, set *clientSet) string {
	if strings.TrimSpace(header) == "" {
		return clientNone
	}
	lang, tag, hash, ok := splitClient(header)
	if !ok {
		return clientOther
	}
	if lang == clientTool {
		if tag == Version {
			return clientTool
		}
		return clientOther
	}
	if want, ok := set.known[lang+"/"+tag]; ok && want == hash {
		return lang + "/" + tag
	}
	return clientOther
}

// admit counts the request under its client and, when refusal is on, turns
// away a client the set does not hold: 403, code unknown_client, no
// Retry-After (a retry will not change the answer; a person adding the
// client to the set will). Returns false after writing the refusal.
func (s *server) admit(
	w http.ResponseWriter,
	r *http.Request,
	method string,
) bool {
	set := s.clients.current(s.store)
	label := clientLabel(r.Header.Get(clientHeader), set)
	s.inst.ClientRequest(label, method)
	if set.refuse && (label == clientOther || label == clientNone) {
		s.inst.Rejected("unknown_client")
		s.slog.Warn("rejected: unknown client", map[string]any{
			"method": method,
			"client": r.Header.Get(clientHeader),
			"label":  label,
			"caller": r.Header.Get(
				"X-Flipr-Caller",
			), "remote": r.RemoteAddr})
		s.fail(
			w,
			http.StatusForbidden,
			refusalBody(r.Header.Get(clientHeader), label),
		)
		return false
	}
	return true
}

// refusalBody says what was refused and the way through.
//
// A refused fliprctl is the one caller that cannot run `fliprctl clients add`,
// so it is told the rule it met (this binary at the server's commit) and its
// two escapes: `flipr ctl` from inside the pod, or a build from the deployed
// commit; and, with the pod down, the files path.
func refusalBody(header, label string) string {
	if lang, tag, _, ok := splitClient(header); ok && lang == clientTool {
		return fmt.Sprintf(
			"unknown_client: this fliprctl is %s and "+
				"the server is %s; the tool is known only "+
				"when it is the server's own build. "+
				"Run it from the pod (kubectl -n "+
				"flipr exec deploy/flipr -- flipr "+
				"ctl -url http://127.0.0.1:15100 "+
				"...), or build it from the "+
				"deployed commit (bin/install.sh at "+
				"%s); "+
				"with the pod down, the files path works "+
				"without the server (fliprctl "+
				"clients ..., offline)",
			tag,
			Version,
			Version,
		)
	}
	return fmt.Sprintf(
		"unknown_client: %s is not a client flipr knows (%s); the "+
			"known set is flipr@clients, and fliprctl clients "+
			"add <lang>/<tag>/<hash> adds one "+
			"(clients/CONTRACT.md section 8)",
		label,
		header,
	)
}

// clientsDeclaration is what flipr publishes about itself at boot, through the
// same path a service's declaration takes: idempotent, never overwriting a
// value an operator set, recorded in the oplog when it changes anything.
//
// The known.* entries are not declared here; an operator adds them, so a
// rebuilt flipr carries the set the record holds and not whatever tags this
// build happened to know.
func clientsDeclaration() *pb.Namespace {
	return &pb.Namespace{
		Service: clientsService,
		Version: clientsVersion,
		Flags: []*pb.Flag{{
			Key: keyRefuseOther,
			Value: &pb.Value{
				Kind: &pb.Value_BoolValue{BoolValue: false},
			},
			Description: "on: every RPC from a client not in the " +
				"known set (the known.* entries here, " +
				"one <lang>/<tag>/<hash> each) " +
				"or with no X-Flipr-Client header " +
				"is answered 403 unknown_client, " +
				"Ping included, so the service " +
				"behind it " +
				"goes loud under its own policy. " +
				"off: those requests are served and " +
				"counted as other and none on " +
				"flipr_client_requests_total, which is where " +
				"to look before turning this on. In " +
				"flight: nothing; the next request " +
				"decides.",
		}},
	}
}

// declareClients publishes flipr's own namespace at boot with the record
// written inside the write, as a service's publish is.
func declareClients(store *Store, oplog *OpLog) error {
	ns := clientsDeclaration()
	_, err := store.Publish(ns, func(n int) error {
		return oplog.OpSync("PublishNamespace", map[string]any{
			"revision": store.Revision(),
			"service":  ns.Service,
			"version":  ns.Version,
			"flags":    n,
			"caller":   "flipr@" + Version,
		})
	})
	return err
}

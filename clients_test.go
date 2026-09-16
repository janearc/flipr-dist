package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// postAs is post with the two identity headers a published client sends.
func postAs(
	t *testing.T,
	srv *httptest.Server,
	client, method, body string,
) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(
		http.MethodPost,
		srv.URL+"/flipr.v1.FliprService/"+method,
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Flipr-Caller", "test@v1")
	if client != "" {
		req.Header.Set(clientHeader, client)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestTheClientLabelIsTheKnownSetsWord: the pure classification. A triple
// the set holds is its lang/tag; a wrong hash, an unknown tag or a
// malformed header is other; no header is none; the tool at this server's
// Version is fliprctl and at any other is other.
func TestTheClientLabelIsTheKnownSetsWord(t *testing.T) {
	set := &clientSet{
		known: map[string]string{
			"go/v1.1.0": "abc123def456",
			"py/v1.1.0": "0123456789ab",
		},
	}
	cases := map[string]string{
		"go/v1.1.0/abc123def456":          "go/v1.1.0",
		"py/v1.1.0/0123456789ab":          "py/v1.1.0",
		"go/v1.1.0/000000000000":          clientOther, // an edited copy claiming the tag
		"go/v1.0.0/abc123def456":          clientOther, // a tag nobody added
		"go/dev/unknown":                  clientOther, // a build that did not stamp
		"curl":                            clientOther, // not a triple
		"":                                clientNone,
		"   ":                             clientNone,
		clientTool + "/" + Version + "/-": clientTool,
		clientTool + "/stale/-":           clientOther,
	}
	for header, want := range cases {
		if got := clientLabel(header, set); got != want {
			t.Errorf("%q: got %s, want %s", header, got, want)
		}
	}
}

// TestFliprDeclaresItsOwnNamespaceAndReadsTheSetLikeAFlag: at boot
// flipr@clients exists with refuse.other off; a known.* entry written through
// SetFlag is seen on the next request without a restart, and the counter
// names the client.
func TestFliprDeclaresItsOwnNamespaceAndReadsTheSetLikeAFlag(t *testing.T) {
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
	if err := declareClients(store, oplog); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(
		newServer(store, oplog, reg, NewLogger()).routes(),
	)
	t.Cleanup(func() { srv.Close(); store.Close() })

	// the declaration: refuse.other exists, off, described, recorded
	f, ok := store.GetFlag(clientsService, clientsVersion, keyRefuseOther)
	if !ok || f.GetValue().GetBoolValue() ||
		!strings.Contains(f.GetDescription(), "403 unknown_client") {
		t.Fatalf("flipr@clients/refuse.other at boot: %v %v", ok, f)
	}
	if len(sink.entries) != 1 ||
		sink.entries[0]["op"] != "PublishNamespace" ||
		sink.entries[0]["service"] != clientsService {
		t.Fatalf(
			"the declaration is recorded like any service's: %v",
			sink.entries,
		)
	}
	// declaring again changes nothing an operator could have set
	if err := declareClients(store, oplog); err != nil {
		t.Fatal(err)
	}

	// an unknown client is served and counted as other; none likewise
	if code, _ := postAs(t, srv, "go/v1.1.0/abc123def456", "Ping", `{}`); code != http.StatusOK {
		t.Fatalf("refusal is off; the ping is served: %d", code)
	}
	if code, _ := postAs(t, srv, "", "Ping", `{}`); code != http.StatusOK {
		t.Fatalf("no header, refusal off: served, got %d", code)
	}
	if n := reg.Registry().Counter(mClientRequests, clientOther, "Ping").Value(); n != 1 {
		t.Fatalf("other pings counted: %d, want 1", n)
	}
	if n := reg.Registry().Counter(mClientRequests, clientNone, "Ping").Value(); n != 1 {
		t.Fatalf("none pings counted: %d, want 1", n)
	}

	// the operator adds the client: one flag, through SetFlag, no restart
	code, body := postAs(
		t,
		srv,
		"",
		"SetFlag",
		`{"service":"flipr","version":"clients","key":"known.go.v1-1-0","value":{"stringValue":"go/v1.1.0/abc123def456"},"reason":"clients/go/v1.1.0 cut"}`,
	)
	if code != http.StatusOK {
		t.Fatalf("adding the client: %d %s", code, body)
	}
	if code, _ := postAs(t, srv, "go/v1.1.0/abc123def456", "Ping", `{}`); code != http.StatusOK {
		t.Fatal("known client's ping")
	}
	if n := reg.Registry().Counter(mClientRequests, "go/v1.1.0", "Ping").Value(); n != 1 {
		t.Fatalf("the known client is counted by name: %d", n)
	}
}

// TestRefusalIsAFlagAndStopsEveryRPCIncludingPing: refuse.other on turns
// other and none away with 403 unknown_client on Ping and on a read; a
// known client and the tool at this Version go through; off again, the
// next request is served. No restart anywhere.
func TestRefusalIsAFlagAndStopsEveryRPCIncludingPing(t *testing.T) {
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
	if err := declareClients(store, oplog); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(
		newServer(store, oplog, reg, NewLogger()).routes(),
	)
	t.Cleanup(func() { srv.Close(); store.Close() })
	known := "go/v1.1.0/abc123def456"
	postAs(
		t,
		srv,
		"",
		"SetFlag",
		`{"service":"flipr","version":"clients","key":"known.go.v1-1-0","value":{"stringValue":"`+known+`"},"reason":"cut"}`,
	)
	postAs(
		t,
		srv,
		known,
		"PublishNamespace",
		`{"namespace":{"service":"puffin","version":"v1","flags":[{"key":"k","value":{"boolValue":true},"description":"a test flag"}]}}`,
	)

	// on
	if code, body := postAs(t, srv, known, "SetFlag", `{"service":"flipr","version":"clients","key":"refuse.other","value":{"boolValue":true},"reason":"the audit is clean"}`); code != http.StatusOK {
		t.Fatalf("turning refusal on: %d %s", code, body)
	}
	for _, c := range []struct{ client, method, body string }{
		{"", "Ping", `{}`},
		{"go/dev/unknown", "Ping", `{}`},
		{"go/v1.1.0/000000000000", "GetNamespace", `{"service":"puffin","version":"v1"}`},
		{"", "SetFlag", `{"service":"puffin","version":"v1","key":"k","value":{"boolValue":false},"reason":"x"}`},
	} {
		code, body := postAs(t, srv, c.client, c.method, c.body)
		if code != http.StatusForbidden ||
			!strings.Contains(body, "unknown_client") ||
			!strings.Contains(body, "fliprctl clients add") {
			t.Fatalf(
				"%q %s: got %d %s, want 403 unknown_client "+
					"naming the fix",
				c.client,
				c.method,
				code,
				body,
			)
		}
	}
	if code, _ := postAs(t, srv, known, "GetNamespace", `{"service":"puffin","version":"v1"}`); code != http.StatusOK {
		t.Fatal("the known client is served")
	}
	if code, _ := postAs(t, srv, clientTool+"/"+Version+"/-", "Ping", `{}`); code != http.StatusOK {
		t.Fatal("the tool built from this commit is served")
	}
	// a tool built past the deployed commit is refused and told its
	// escapes, not the one command it cannot run
	if code, body := postAs(t, srv, clientTool+"/stale/-", "Ping", `{}`); code != http.StatusForbidden ||
		!strings.Contains(body, "this fliprctl is stale and the server is "+Version) ||
		!strings.Contains(body, "kubectl -n flipr exec") ||
		strings.Contains(body, "fliprctl clients add <lang>") {
		t.Fatalf("a stale tool: %d %s", code, body)
	}
	if f, _ := store.GetFlag("puffin", "v1", "k"); !f.GetValue().
		GetBoolValue() {
		t.Fatal("the refused flip did not land")
	}
	if n := reg.Registry().Counter(mRPCRejected, "unknown_client").Value(); n != 5 {
		t.Fatalf(
			"refusals counted: %d, want 5 (four callers and the "+
				"stale tool)",
			n,
		)
	}
	// off, by the known client (the tool would do this in life)
	if code, _ := postAs(t, srv, known, "SetFlag", `{"service":"flipr","version":"clients","key":"refuse.other","value":{"boolValue":false},"reason":"a curl in the reconciler"}`); code != http.StatusOK {
		t.Fatal("turning refusal off")
	}
	if code, _ := postAs(t, srv, "", "Ping", `{}`); code != http.StatusOK {
		t.Fatal("off again: served")
	}
}

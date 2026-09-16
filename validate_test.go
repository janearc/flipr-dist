package main

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// THE FLIP PATH REFUSES WHAT THE PUBLISH PATH REFUSES, AND MORE. These are the
// review's probe list (and the "@" case), each of which
// answered 200 on 2026-09-03.

// TestAFlipWithoutAValueIsRefused: no value, and an empty value, are both
// refused, because a null served to a client reads as off with known=true.
func TestAFlipWithoutAValueIsRefused(t *testing.T) {
	srv, _ := testServer(t)
	post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"svc","version":"v1","flags":[
		{"key":"gate","value":{"boolValue":true},"description":"x"}]}}`,
	)
	for _, body := range []string{
		`{"service":"svc","version":"v1","key":"gate","reason":"probe: no value"}`,
		`{"service":"svc","version":"v1","key":"gate","value":{},"reason":"probe: empty value"}`,
	} {
		code, out := post(t, srv, "SetFlag", body)
		if code != http.StatusBadRequest ||
			!strings.Contains(out, "a flip carries a value") {
			t.Fatalf("%s -> %d %s", body, code, out)
		}
	}
	code, out := post(
		t,
		srv,
		"GetFlag",
		`{"service":"svc","version":"v1","key":"gate"}`,
	)
	if code != 200 || !strings.Contains(out, `"boolValue":true`) {
		t.Fatalf("the refused flips changed the flag: %d %s", code, out)
	}
}

// TestAFlipCannotRetypeAFlag: an expensive boolean retyped to a string is the
// one flag the emergency page cannot kill. Refused, with the types named.
func TestAFlipCannotRetypeAFlag(t *testing.T) {
	srv, _ := testServer(t)
	post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"svc","version":"v1","flags":[
		{"key":"spend","value":{"boolValue":true},"description":"on: spends. off: stops.","expensive":true}]}}`,
	)
	code, out := post(
		t,
		srv,
		"SetFlag",
		`{"service":"svc","version":"v1","key":"spend","value":{"stringValue":"yes"},"reason":"probe"}`,
	)
	if code != http.StatusBadRequest ||
		!strings.Contains(
			out,
			"is a bool and a flip cannot make it a string",
		) {
		t.Fatalf("retype: %d %s", code, out)
	}
	// the same type is of course fine
	code, out = post(
		t,
		srv,
		"SetFlag",
		`{"service":"svc","version":"v1","key":"spend","value":{"boolValue":false},"reason":"probe"}`,
	)
	if code != 200 {
		t.Fatalf("a same-type flip was refused: %d %s", code, out)
	}
}

// TestTheBudgetBindsTheFlipPath: SetFlag creates keys, so it is where the
// 25th flag would arrive when publish is honest.
func TestTheBudgetBindsTheFlipPath(t *testing.T) {
	srv, _ := testServer(t)
	for i := 0; i < flagBudget; i++ {
		code, out := post(t, srv, "SetFlag", fmt.Sprintf(
			`{"service":"bloat","version":"v1","key":"flag.%02d","value":{"boolValue":true},"reason":"fill"}`,
			i,
		))
		if code != 200 {
			t.Fatalf("flag %d refused: %d %s", i, code, out)
		}
	}
	code, out := post(
		t,
		srv,
		"SetFlag",
		`{"service":"bloat","version":"v1","key":"flag.24","value":{"boolValue":true},"reason":"one too many"}`,
	)
	if code != http.StatusBadRequest || !strings.Contains(out, "budget") {
		t.Fatalf("25th flag: %d %s", code, out)
	}
	// flipping an EXISTING flag in a full namespace is not a 25th flag
	code, out = post(
		t,
		srv,
		"SetFlag",
		`{"service":"bloat","version":"v1","key":"flag.00","value":{"boolValue":false},"reason":"still allowed"}`,
	)
	if code != 200 {
		t.Fatalf(
			"a flip inside a full namespace was refused: %d %s",
			code,
			out,
		)
	}
}

// TestNamesAreValidatedOnBothPaths: spaces, uppercase, an "@" in the version
// and a whitespace reason are refused at the boundary, on flip and publish.
func TestNamesAreValidatedOnBothPaths(t *testing.T) {
	srv, _ := testServer(t)
	cases := map[string]string{
		`{"service":"svc","version":"v1","key":"has space","value":{"boolValue":true},"reason":"r"}`:  "dotted lowercase",
		`{"service":"svc","version":"v1","key":"Upper.Case","value":{"boolValue":true},"reason":"r"}`: "dotted lowercase",
		`{"service":"svc","version":"v1","key":"","value":{"boolValue":true},"reason":"r"}`:           "key is required",
		`{"service":"svc","version":"a@b","key":"k","value":{"boolValue":true},"reason":"r"}`:         `never \"@\"`,
		`{"service":"svc@a","version":"b","key":"k","value":{"boolValue":true},"reason":"r"}`:         `never \"@\"`,
		`{"service":"svc","version":"v1","key":"k","value":{"boolValue":true},"reason":"   "}`:        "reason is required",
	}
	for body, want := range cases {
		code, out := post(t, srv, "SetFlag", body)
		if code != http.StatusBadRequest ||
			!strings.Contains(out, want) {
			t.Errorf(
				"SetFlag %s -> %d %s (want 400 containing %q)",
				body,
				code,
				out,
				want,
			)
		}
	}
	code, out := post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"svc","version":"v1","flags":[{"key":"Bad Key","value":{"boolValue":true},"description":"x"}]}}`,
	)
	if code != http.StatusBadRequest ||
		!strings.Contains(out, "dotted lowercase") {
		t.Fatalf("publish with a bad key: %d %s", code, out)
	}
	code, out = post(
		t,
		srv,
		"PublishNamespace",
		`{"namespace":{"service":"svc","version":"a@b","flags":[{"key":"k","value":{"boolValue":true},"description":"x"}]}}`,
	)
	if code != http.StatusBadRequest ||
		!strings.Contains(out, `never \"@\"`) {
		t.Fatalf("publish with an @ version: %d %s", code, out)
	}
	// the global scope's own names pass
	code, out = post(
		t,
		srv,
		"SetFlag",
		`{"service":"_global","version":"_","key":"log.level","value":{"stringValue":"info"},"reason":"r"}`,
	)
	if code != 200 {
		t.Fatalf(
			"the global scope was refused by its own name rule: "+
				"%d %s",
			code,
			out,
		)
	}
	if code, _ := post(t, srv, "ListNamespaces", `{}`); code != 200 {
		t.Fatal("list")
	}
}

// TestRefusalsAreCountedByCode: every refusal moves the rejected counter
// under a low-cardinality code, so a client sending garbage is visible.
func TestRefusalsAreCountedByCode(t *testing.T) {
	srv, _ := testServer(t)
	post(
		t,
		srv,
		"SetFlag",
		`{"service":"svc","version":"v1","key":"k","reason":"r"}`,
	)
	post(
		t,
		srv,
		"SetFlag",
		`{"service":"svc","version":"v1","key":"Bad","value":{"boolValue":true},"reason":"r"}`,
	)
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b strings.Builder
	buf := make([]byte, 64<<10)
	for {
		n, err := resp.Body.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			break
		}
	}
	for _, want := range []string{`flipr_rpc_rejected_total{reason="no_value"} 1`, `flipr_rpc_rejected_total{reason="bad_name"} 1`} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("metrics lack %q", want)
		}
	}
}

// TestReplayIsNotBoundByTheFlipRules: the oplog is the record of what
// happened. A history holding a 25th key, written before the budget bound the
// flip path, must come back whole on a wipe-recovery rather than refuse to
// start flipr on its own past.
func TestReplayIsNotBoundByTheFlipRules(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	for i := 0; i < flagBudget+3; i++ {
		if err := s.ApplyReplayed("svc", "v", fmt.Sprintf("k%02d", i), `{"boolValue":true}`, true); err != nil {
			t.Fatalf("replayed key %d refused: %v", i, err)
		}
	}
	// and a retype from history is reproduced as it happened
	if err := s.ApplyReplayed("svc", "v", "k00", `{"stringValue":"was retyped"}`, true); err != nil {
		t.Fatalf("replayed retype refused: %v", err)
	}
	if _, n, _ := s.CountFlags(); n != flagBudget+3 {
		t.Fatalf("store holds %d flags, want %d", n, flagBudget+3)
	}
}

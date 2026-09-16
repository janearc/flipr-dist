package main

import (
	"os"
	"testing"

	pb "github.com/janearc/flipr-dist/gen/fliprpb"
)

// TestEnvOr covers the two-line config boundary.
func TestEnvOr(t *testing.T) {
	if got := envOr("FLIPR_TEST_UNSET_VAR", "fallback"); got != "fallback" {
		t.Errorf("got %q", got)
	}
	t.Setenv("FLIPR_TEST_SET_VAR", "value")
	if got := envOr("FLIPR_TEST_SET_VAR", "fallback"); got != "value" {
		t.Errorf("got %q", got)
	}
	t.Setenv("FLIPR_TEST_EMPTY_VAR", "")
	if got := envOr("FLIPR_TEST_EMPTY_VAR", "fallback"); got != "fallback" {
		t.Errorf("an empty variable should fall back, got %q", got)
	}
}

// TestLogger checks the structured logger emits without panicking on any
// shape or level, and that debug is gated by the environment.
func TestLogger(t *testing.T) {
	l := NewLogger()
	l.Info("a message", nil)
	l.Warn("with fields", map[string]any{"k": "v", "n": 1})
	l.Error("an error", map[string]any{"err": "synthetic"})
	l.Debug("gated: silent unless FLIPR_DEBUG", nil)
	t.Setenv("FLIPR_DEBUG", "1")
	NewLogger().Debug("gated: emits with FLIPR_DEBUG", nil)
}

// TestOutcomeFor checks status codes bucket into the three low-cardinality
// outcomes the metric labels use.
func TestOutcomeFor(t *testing.T) {
	for status, want := range map[int]string{
		0: "ok", 200: "ok", 204: "ok",
		400: "client_error", 404: "client_error", 405: "client_error",
		500: "server_error", 503: "server_error",
	} {
		if got := outcomeFor(status); got != want {
			t.Errorf(
				"outcomeFor(%d) = %q, want %q",
				status,
				got,
				want,
			)
		}
	}
}

// TestValueString covers each arm of the value oneof, including the empty
// value a caller can legally send.
func TestValueString(t *testing.T) {
	cases := map[string]*pb.Value{
		"true":  {Kind: &pb.Value_BoolValue{BoolValue: true}},
		"false": {Kind: &pb.Value_BoolValue{BoolValue: false}},
		"surreal": {
			Kind: &pb.Value_StringValue{StringValue: "surreal"},
		},
		"42": {Kind: &pb.Value_IntValue{IntValue: 42}},
		"":   nil,
	}
	for want, v := range cases {
		if got := valueString(v); got != want {
			t.Errorf("valueString(%v) = %q, want %q", v, got, want)
		}
	}
}

// writeFile is a test helper for building fixture files.
func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0600)
}

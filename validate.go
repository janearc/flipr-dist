package main

import (
	"fmt"
	"regexp"
	"strings"

	pb "github.com/janearc/flipr-dist/gen/fliprpb"
)

// What a flip is allowed to say.
//
// SetFlag once checked nothing but the reason: a flip with no value was
// served as null, an expensive boolean could be retyped to a string, and
// the flag budget applied only on publish.
//
// A key with spaces, or a version with an "@" in it, went into the store
// under a name reload could not parse back.
//
// Every rule here is one the estate already lived by: the live keys were all
// lowercase and dotted, so nothing in the store is refused by them.

// flagBudget is the most flags one namespace may hold. Nobody needs more
// than 24: flipr is not an a/b testing system, it is a realtime persistent
// config store. Enforced on publish AND
// on the flip path, because SetFlag creates keys too.
const flagBudget = 24

// maxNameLen bounds every identifier. Keys, services and versions are short
// by nature; a long one is a mistake and would be unreadable on the
// emergency page anyway.
const maxNameLen = 128

// keyFormat is the dotted, lowercase hierarchy USING.md describes:
// nightly.smoothing, log.level, fetch.usgs. Segments start alphanumeric and
// may carry underscores and hyphens; no spaces, no uppercase, no empty
// segments.
var keyFormat = regexp.MustCompile(
	`^[a-z0-9][a-z0-9_-]*(\.[a-z0-9][a-z0-9_-]*)*$`,
)

// serviceFormat allows the leading underscore the global scope uses.
var serviceFormat = regexp.MustCompile(`^[a-z0-9_][a-z0-9_.-]*$`)

// versionFormat allows a commit hash, a branch name, a tag, or the global
// scope's "_". Never "@": the store keys namespaces as service@version and
// parses from the right, so an "@" in either half misfiles the flag.
var versionFormat = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]*$`)

// Refusal is a request flipr will not honour, with the reason a caller is
// told and the low-cardinality code the rejected counter carries. It is a
// client error, never a server one: the store was not touched.
type Refusal struct {
	Code string
	Why  string
}

// Error renders the reason for the caller.
func (r *Refusal) Error() string { return r.Why }

// refuse builds a Refusal.
func refuse(code, format string, args ...any) *Refusal {
	return &Refusal{Code: code, Why: fmt.Sprintf(format, args...)}
}

// checkNamespaceName validates a service and version pair.
func checkNamespaceName(service, version string) *Refusal {
	switch {
	case service == "":
		return refuse("bad_name", "service is required")
	case len(service) > maxNameLen:
		return refuse(
			"bad_name",
			"service name is longer than %d characters",
			maxNameLen,
		)
	case !serviceFormat.MatchString(service):
		return refuse(
			"bad_name",
			"service %q: lowercase letters, digits, dot, "+
				"underscore and hyphen only; never \"@\" or "+
				"whitespace",
			service,
		)
	case version == "":
		return refuse(
			"bad_name",
			"version is required (the commit hash at deploy)",
		)
	case len(version) > maxNameLen:
		return refuse(
			"bad_name",
			"version is longer than %d characters",
			maxNameLen,
		)
	case !versionFormat.MatchString(version):
		return refuse(
			"bad_name",
			"version %q: letters, digits, dot, underscore and "+
				"hyphen only; never \"@\" or whitespace",
			version,
		)
	}
	return nil
}

// checkKey validates one flag key against the dotted hierarchy.
func checkKey(key string) *Refusal {
	switch {
	case key == "":
		return refuse("bad_name", "key is required")
	case len(key) > maxNameLen:
		return refuse(
			"bad_name",
			"key is longer than %d characters",
			maxNameLen,
		)
	case !keyFormat.MatchString(key):
		return refuse(
			"bad_name",
			"key %q: dotted lowercase segments like "+
				"nightly.smoothing; no spaces, no uppercase, "+
				"no empty segment",
			key,
		)
	}
	return nil
}

// checkReason validates a flip's reason: present, and not only whitespace.
func checkReason(reason string) *Refusal {
	if strings.TrimSpace(reason) == "" {
		return refuse(
			"missing_reason",
			"reason is required: say why you are flipping this",
		)
	}
	return nil
}

// kindName names a value's type for a refusal message.
func kindName(v *pb.Value) string {
	switch v.GetKind().(type) {
	case *pb.Value_BoolValue:
		return "bool"
	case *pb.Value_StringValue:
		return "string"
	case *pb.Value_IntValue:
		return "int"
	default:
		return "none"
	}
}

// checkValue validates the value a flip carries against the flag it lands
// on: a flip must carry a typed value, and may not change the type of a flag
// that already has one.
//
// A client reads a flag with the accessor for the type it declared, so a
// retyped flag reads as its zero value with no error, and an expensive
// boolean retyped to a string is the one flag the emergency page cannot
// stop.
func checkValue(existing *pb.Flag, v *pb.Value) *Refusal {
	if v == nil || v.GetKind() == nil {
		return refuse(
			"no_value",
			"a flip carries a value: {\"boolValue\":true}, "+
				"{\"stringValue\":\"...\"} or "+
				"{\"intValue\":\"...\"}",
		)
	}
	if existing == nil || existing.Value == nil ||
		existing.Value.GetKind() == nil {
		return nil
	}
	if have, want := kindName(v), kindName(existing.Value); have != want {
		return refuse(
			"type_change",
			"flag %q is a %s and a flip cannot make it a %s; "+
				"retire and republish it to change its type",
			existing.Key,
			want,
			have,
		)
	}
	return nil
}

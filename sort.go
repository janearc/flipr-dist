package main

import (
	"sort"

	pb "github.com/janearc/flipr-dist/gen/fliprpb"
)

// Deterministic ordering everywhere a collection leaves the process. Map
// iteration order in Go is randomised, and an API whose response reorders
// between identical calls makes every diff and every cached page look changed.

// sorted returns a namespace's flags in key order.
func sorted(flags map[string]*pb.Flag) []*pb.Flag {
	out := make([]*pb.Flag, 0, len(flags))
	for _, f := range flags {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// sortNamespaces orders namespaces by service, then version.
func sortNamespaces(ns []*pb.Namespace) {
	sort.Slice(ns, func(i, j int) bool {
		if ns[i].Service != ns[j].Service {
			return ns[i].Service < ns[j].Service
		}
		return ns[i].Version < ns[j].Version
	})
}

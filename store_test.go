package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/janearc/flipr-dist/gen/fliprpb"
)

// boolFlag builds a boolean flag for a test.
func boolFlag(key string, v bool, expensive bool, desc string) *pb.Flag {
	return &pb.Flag{
		Key:         key,
		Value:       &pb.Value{Kind: &pb.Value_BoolValue{BoolValue: v}},
		Description: desc,
		Expensive:   expensive,
	}
}

// openStore opens a store under dir, failing the test if it cannot.
func openStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := OpenStore(
		filepath.Join(dir, "flipr.db"),
		NewRegistry(),
		NewLogger(),
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return s
}

// TestValuesSurviveReopen is the whole point of the store: a flip is durable.
// bbolt is never on the read path, so this is the test that the read path and
// the disk actually agree.
func TestValuesSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	if _, err := s.PublishNamespace(&pb.Namespace{
		Service: "albatross", Version: "abc",
		Flags: []*pb.Flag{boolFlag("nightly.aggregation", true, true, "the nightly")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetFlag("albatross", "abc", "nightly.aggregation",
		&pb.Value{Kind: &pb.Value_BoolValue{BoolValue: false}}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2 := openStore(t, dir)
	defer s2.Close()
	f, ok := s2.GetFlag("albatross", "abc", "nightly.aggregation")
	if !ok {
		t.Fatal("flag vanished across reopen")
	}
	if f.Value.GetBoolValue() {
		t.Fatal("the flip did not survive: value is true, want false")
	}
}

// TestPublishDoesNotClobberAnOperatorsFlip guards the rule that a deploy
// declares which flags EXIST while an operator decides what they are SET to.
// If publishing reset values, every deploy would silently undo an emergency
// flip -- and a deploy is exactly when that is most likely to happen.
func TestPublishDoesNotClobberAnOperatorsFlip(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()

	ns := &pb.Namespace{Service: "albatross", Version: "abc",
		Flags: []*pb.Flag{
			boolFlag("expensive.thing", true, true, "costs money"),
		}}
	if _, err := s.PublishNamespace(ns); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetFlag("albatross", "abc", "expensive.thing",
		&pb.Value{Kind: &pb.Value_BoolValue{BoolValue: false}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PublishNamespace(ns); err != nil {
		t.Fatal(err)
	}
	f, _ := s.GetFlag("albatross", "abc", "expensive.thing")
	if f.Value.GetBoolValue() {
		t.Fatal(
			"a redeploy re-enabled a flag an operator had turned " +
				"off",
		)
	}
}

// TestFlipKeepsDescriptionAndExpensive guards the emergency page. It is the
// set where expensive is true, so a flip that cleared the field would empty
// the page the first time anyone used it.
func TestFlipKeepsDescriptionAndExpensive(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	if _, err := s.PublishNamespace(&pb.Namespace{
		Service: "haho", Version: "v1",
		Flags: []*pb.Flag{boolFlag("model.enabled", true, true, "hahod may spawn a model")},
	}); err != nil {
		t.Fatal(err)
	}
	f, err := s.SetFlag("haho", "v1", "model.enabled",
		&pb.Value{Kind: &pb.Value_BoolValue{BoolValue: false}})
	if err != nil {
		t.Fatal(err)
	}
	if !f.Expensive {
		t.Error(
			"expensive was cleared by a flip; the emergency page " +
				"would lose this flag",
		)
	}
	if f.Description == "" {
		t.Error("description was cleared by a flip")
	}
	if f.UpdatedAt == "" {
		t.Error("a flip did not stamp updated_at")
	}
}

// TestNamespacesAreKeyedByVersion checks that a deploy cannot write the
// running version's flags: albatross@abc and albatross@xyz are separate.
func TestNamespacesAreKeyedByVersion(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	for _, v := range []string{"abc", "xyz"} {
		if _, err := s.PublishNamespace(&pb.Namespace{
			Service: "albatross", Version: v,
			Flags: []*pb.Flag{boolFlag("k", true, false, "")},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.SetFlag("albatross", "abc", "k",
		&pb.Value{Kind: &pb.Value_BoolValue{BoolValue: false}}); err != nil {
		t.Fatal(err)
	}
	other, _ := s.GetFlag("albatross", "xyz", "k")
	if !other.Value.GetBoolValue() {
		t.Fatal("writing one version's flag changed another version's")
	}
	if got := len(s.ListNamespaces()); got != 2 {
		t.Fatalf("want 2 namespaces, got %d", got)
	}
}

// TestGetNamespaceAndMisses covers the found and not-found paths of both
// single-flag and whole-namespace reads.
func TestGetNamespaceAndMisses(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	if _, err := s.PublishNamespace(&pb.Namespace{
		Service: "svc", Version: "v", Flags: []*pb.Flag{boolFlag("a", true, false, "")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.GetNamespace("svc", "v"); !ok {
		t.Error("published namespace not found")
	}
	if _, ok := s.GetNamespace("svc", "nope"); ok {
		t.Error("found a namespace that was never published")
	}
	if _, ok := s.GetFlag("svc", "nope", "a"); ok {
		t.Error("found a flag in a namespace that does not exist")
	}
	if _, ok := s.GetFlag("svc", "v", "nope"); ok {
		t.Error("found a flag that was never declared")
	}
}

// TestSetFlagCreatesNamespaceOnDemand checks a flip works even when nothing
// published the namespace first, so an operator is never blocked by a service
// that has not onboarded yet.
func TestSetFlagCreatesNamespaceOnDemand(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	f, err := s.SetFlag("brand", "new", "key",
		&pb.Value{Kind: &pb.Value_StringValue{StringValue: "surreal"}})
	if err != nil {
		t.Fatal(err)
	}
	if f.Value.GetStringValue() != "surreal" {
		t.Fatalf("got %q", f.Value.GetStringValue())
	}
}

// TestCountFlags checks the numbers the gauges are built from.
func TestCountFlags(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	if _, err := s.PublishNamespace(&pb.Namespace{
		Service: "svc", Version: "v",
		Flags: []*pb.Flag{
			boolFlag("cheap", true, false, ""),
			boolFlag("costly", true, true, ""),
			boolFlag("costly2", true, true, ""),
		},
	}); err != nil {
		t.Fatal(err)
	}
	ns, flags, expensive := s.CountFlags()
	if ns != 1 || flags != 3 || expensive != 2 {
		t.Fatalf(
			"got ns=%d flags=%d expensive=%d, want 1/3/2",
			ns,
			flags,
			expensive,
		)
	}
}

// TestHealthyOnAFreshStore checks the health path reads the store rather than
// trusting that it opened.
func TestHealthyOnAFreshStore(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	if err := s.Healthy(); err != nil {
		t.Fatalf("healthy on a fresh store: %v", err)
	}
}

// TestOpenStoreRefusesAnUnusablePath checks that a store which cannot be
// opened fails loudly rather than returning something half-built.
func TestOpenStoreRefusesAnUnusablePath(t *testing.T) {
	dir := t.TempDir()
	// A directory where the file should be: bbolt cannot open it.
	p := filepath.Join(dir, "flipr.db")
	if err := os.Mkdir(p, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(p, NewRegistry(), NewLogger()); err == nil {
		t.Fatal("opened a store on a directory; want an error")
	}
}

// TestParseNSKey covers the split, including a service name containing the
// separator, where the rightmost @ must win.
func TestParseNSKey(t *testing.T) {
	k, err := parseNSKey("albatross@abc")
	if err != nil || k.service != "albatross" || k.version != "abc" {
		t.Fatalf("got %+v err=%v", k, err)
	}
	k, err = parseNSKey("we@ird@v1")
	if err != nil || k.service != "we@ird" || k.version != "v1" {
		t.Fatalf("rightmost @ must win, got %+v", k)
	}
	if _, err := parseNSKey("nosplit"); err == nil {
		t.Fatal("want an error on a malformed key")
	}
}

// TestNSKeyString checks the stored form.
func TestNSKeyString(t *testing.T) {
	if got := (nsKey{"a", "b"}).String(); got != "a@b" {
		t.Fatalf("got %q", got)
	}
}

// TestListNamespacesIsOrdered checks output is deterministic, because an API
// that reorders between identical calls makes every diff look changed.
func TestListNamespacesIsOrdered(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	for _, n := range []struct{ svc, ver string }{{"b", "2"}, {"a", "2"}, {"a", "1"}} {
		if _, err := s.PublishNamespace(&pb.Namespace{
			Service: n.svc, Version: n.ver, Flags: []*pb.Flag{boolFlag("k", true, false, "")},
		}); err != nil {
			t.Fatal(err)
		}
	}
	got := s.ListNamespaces()
	want := []string{"a@1", "a@2", "b@2"}
	for i, w := range want {
		if k := (nsKey{got[i].Service, got[i].Version}).String(); k != w {
			t.Fatalf("position %d: got %s want %s", i, k, w)
		}
	}
}

// TestFlagsWithinANamespaceAreOrdered checks the same for flags.
func TestFlagsWithinANamespaceAreOrdered(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	if _, err := s.PublishNamespace(&pb.Namespace{
		Service: "svc", Version: "v",
		Flags: []*pb.Flag{
			boolFlag("zebra", true, false, ""),
			boolFlag("aardvark", true, false, ""),
		},
	}); err != nil {
		t.Fatal(err)
	}
	ns, _ := s.GetNamespace("svc", "v")
	if ns.Flags[0].Key != "aardvark" || ns.Flags[1].Key != "zebra" {
		var keys []string
		for _, f := range ns.Flags {
			keys = append(keys, f.Key)
		}
		t.Fatalf("unordered: %s", strings.Join(keys, ","))
	}
}

// TestGenerationSurvivesReopenAndChangesOnWipe is the wipe report. Same file
// reopened keeps its generation; a recreated file wears a new one, which is
// how every client learns the store was nuked.
func TestGenerationSurvivesReopenAndChangesOnWipe(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	g1 := s.Generation()
	if g1 == "" {
		t.Fatal("no generation minted")
	}
	s.Close()

	s2 := openStore(t, dir)
	if s2.Generation() != g1 {
		t.Fatalf(
			"generation changed across a plain reopen: %s -> %s",
			g1,
			s2.Generation(),
		)
	}
	s2.Close()

	// the wipe
	if err := os.Remove(filepath.Join(dir, "flipr.db")); err != nil {
		t.Fatal(err)
	}
	s3 := openStore(t, dir)
	defer s3.Close()
	if s3.Generation() == g1 {
		t.Fatal(
			"a wiped store kept its generation; the wipe report " +
				"is broken",
		)
	}
}

// stringFlag builds a string-valued flag for a test.
func stringFlag(key, v string) *pb.Flag {
	return &pb.Flag{
		Key:   key,
		Value: &pb.Value{Kind: &pb.Value_StringValue{StringValue: v}},
	}
}

// TestGlobalScopeFallback: a flag set in the global scope
// answers for every service that does not set its own, and a service's own
// flag always wins.
func TestGlobalScopeFallback(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	if _, err := s.PublishNamespace(&pb.Namespace{
		Service: GlobalService, Version: GlobalVersion,
		Flags: []*pb.Flag{stringFlag("log.level", "debug")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PublishNamespace(&pb.Namespace{
		Service: "albatross", Version: "abc",
		Flags: []*pb.Flag{stringFlag("log.level", "info"), boolFlag("own", true, false, "")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PublishNamespace(&pb.Namespace{
		Service: "kingfisher", Version: "xyz",
		Flags: []*pb.Flag{boolFlag("fetch.enabled", false, true, "")},
	}); err != nil {
		t.Fatal(err)
	}

	// kingfisher declares no log.level: the global answers
	f, ok := s.GetFlag("kingfisher", "xyz", "log.level")
	if !ok || f.Value.GetStringValue() != "debug" {
		t.Fatalf("global fallback failed: ok=%v f=%v", ok, f)
	}
	// albatross declares its own: it wins over the global
	f, _ = s.GetFlag("albatross", "abc", "log.level")
	if f.Value.GetStringValue() != "info" {
		t.Fatalf("service flag did not win over global: %v", f)
	}
	// a key nobody declares is still missing, not defaulted
	if _, ok := s.GetFlag("kingfisher", "xyz", "never.set"); ok {
		t.Fatal("an undeclared key resolved via nothing")
	}
	// the global scope answers for itself without recursing
	if _, ok := s.GetFlag(GlobalService, GlobalVersion, "never.set"); ok {
		t.Fatal("global scope resolved a key it does not hold")
	}
}

// TestGetNamespaceMergesGlobal checks the cache-filling call carries the
// merged truth, so a client's check() on a globally-set key works with zero
// client-side precedence code.
func TestGetNamespaceMergesGlobal(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	if _, err := s.PublishNamespace(&pb.Namespace{
		Service: GlobalService, Version: GlobalVersion,
		Flags: []*pb.Flag{stringFlag("log.level", "debug"), stringFlag("only.global", "g")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PublishNamespace(&pb.Namespace{
		Service: "svc", Version: "v",
		Flags: []*pb.Flag{stringFlag("log.level", "info")},
	}); err != nil {
		t.Fatal(err)
	}
	ns, ok := s.GetNamespace("svc", "v")
	if !ok {
		t.Fatal("namespace missing")
	}
	got := map[string]string{}
	for _, f := range ns.Flags {
		got[f.Key] = f.Value.GetStringValue()
	}
	if got["log.level"] != "info" {
		t.Errorf("service flag lost the overlay: %v", got)
	}
	if got["only.global"] != "g" {
		t.Errorf(
			"global-only flag missing from the merged view: %v",
			got,
		)
	}
}

// TestFliprObeysItsOwnLogLevel checks the dogfood path: setting the global
// log.level flips flipr's own debug gate on the very next reload.
func TestFliprObeysItsOwnLogLevel(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	if s.log.debug.Load() {
		t.Fatal("debug gate open before any flag")
	}
	if _, err := s.SetFlag(GlobalService, GlobalVersion, "log.level",
		&pb.Value{Kind: &pb.Value_StringValue{StringValue: "debug"}}); err != nil {
		t.Fatal(err)
	}
	if !s.log.debug.Load() {
		t.Fatal(
			"global log.level=debug did not open flipr's own " +
				"debug gate",
		)
	}
	if _, err := s.SetFlag(GlobalService, GlobalVersion, "log.level",
		&pb.Value{Kind: &pb.Value_StringValue{StringValue: "info"}}); err != nil {
		t.Fatal(err)
	}
	if s.log.debug.Load() {
		t.Fatal("log.level=info did not close the gate")
	}
}

// TestConcurrentWritersLeaveTheSnapshotCurrent: after any
// number of writers have returned, the snapshot readers are served from must
// equal the disk. Without the writer lock a preempted writer could publish a
// snapshot older than the one already published.
func TestConcurrentWritersLeaveTheSnapshotCurrent(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	const writers, rounds = 8, 40
	done := make(chan error, writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			for i := 0; i < rounds; i++ {
				svc := fmt.Sprintf("svc%d", w)
				v := &pb.Value{
					Kind: &pb.Value_IntValue{
						IntValue: int64(i),
					},
				}
				if _, err := s.SetFlag(svc, "v", fmt.Sprintf("k%d", i%flagBudget), v); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}(w)
	}
	for w := 0; w < writers; w++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	served := s.current()
	if err := s.reload(); err != nil {
		t.Fatal(err)
	}
	disk := s.current()
	if len(served) != len(disk) {
		t.Fatalf(
			"served %d namespaces, disk has %d",
			len(served),
			len(disk),
		)
	}
	for ns, flags := range disk {
		for k, f := range flags {
			got, ok := served[ns][k]
			if !ok ||
				got.Value.GetIntValue() != f.Value.GetIntValue() {
				t.Fatalf(
					"%s/%s: served %v, disk %v",
					ns,
					k,
					got.GetValue(),
					f.Value,
				)
			}
		}
	}
}

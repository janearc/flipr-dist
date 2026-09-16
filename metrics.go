package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Instrumentation, and there is a lot of it on purpose.
//
// flipr is the service every other service asks before it does anything
// expensive. If it is slow, everything is slow; if it is lying, everything acts
// on a lie.
//
// So it is measured far past the point of looking silly -- Because it is a
// crucial service, it is instrumented everywhere, even where that seems silly.
//
// Hand-rolled rather than client_golang. Not dogma: haho and delightd both
// hand-roll their /metrics and only metricsd takes the dependency, so this
// matches the estate.
//
// It also keeps flipr's dependency list to bbolt and protobuf, which is easy to
// defend for a service that must never fail to start.
//
// Some of these will overlap host metrics that metricsd already collects.
// That is known and accepted; deconflicting, merging or dropping the
// duplicates is a scrape-time decision and nothing here has to change for it.

// Counter is a monotonically increasing value.
type Counter struct{ v atomic.Uint64 }

// Inc adds one.
func (c *Counter) Inc() { c.v.Add(1) }

// Add adds n.
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// Value reads the current total.
func (c *Counter) Value() uint64 { return c.v.Load() }

// Gauge is a value that can go up and down.
type Gauge struct{ v atomic.Int64 }

// Set replaces the value.
func (g *Gauge) Set(n int64) { g.v.Store(n) }

// Inc adds one.
func (g *Gauge) Inc() { g.v.Add(1) }

// Dec subtracts one.
func (g *Gauge) Dec() { g.v.Add(-1) }

// Value reads the current value.
func (g *Gauge) Value() int64 { return g.v.Load() }

// histogramBuckets are upper bounds in seconds.
//
// Chosen for a service whose answers should be measured in microseconds: the
// interesting question is not "is it under a second" but "has it left the
// microsecond range", so the buckets are dense where flipr should live and
// coarse past the point where something is already wrong.
//
// 200µs and 250µs were added from a measurement: three days of live GetFlag
// had its median in the 50 to 100µs bucket and its p99 in the 100 to 500µs
// one.
//
// So every quantile between p60 and p99 was an interpolation across a
// five-fold range, and a p99 of 496µs was the bucket edge rather than a
// number.
var histogramBuckets = []float64{
	0.000_01, 0.000_05, 0.000_1, 0.000_2, 0.000_25, 0.000_5,
	0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5,
}

// Histogram is a Prometheus histogram with fixed buckets.
type Histogram struct {
	counts []atomic.Uint64 // cumulative per bucket, filled at render time
	sum    atomic.Uint64   // nanoseconds, to stay integral and lock-free
	count  atomic.Uint64
}

// NewHistogram builds a histogram over the standard bucket set.
func NewHistogram() *Histogram {
	return &Histogram{counts: make([]atomic.Uint64, len(histogramBuckets))}
}

// Observe records one duration.
func (h *Histogram) Observe(d time.Duration) {
	s := d.Seconds()
	for i, b := range histogramBuckets {
		if s <= b {
			h.counts[i].Add(1)
			break
		}
	}
	h.sum.Add(uint64(d.Nanoseconds()))
	h.count.Add(1)
}

// ObserveSince records the time elapsed since start. The common call shape.
func (h *Histogram) ObserveSince(
	start time.Time,
) {
	h.Observe(time.Since(start))
}

// metric is one rendered family: name, help, type and its samples.
type metric struct {
	name    string
	help    string
	kind    string
	samples []sample
}

// sample is one line of a metric family: optional labels and a value.
type sample struct {
	labels map[string]string
	value  string
}

// Registry holds every metric flipr publishes and renders them as Prometheus
// text. Small enough to lock around: /metrics is scraped every few seconds,
// not hundreds of times a second, so contention here is not a concern.
type Registry struct {
	mu sync.Mutex
	// family -> labelkey -> counter
	counters map[string]map[string]*Counter
	gauges   map[string]func() int64 // family -> value function
	// family -> labelkey -> histogram
	hists  map[string]map[string]*Histogram
	help   map[string]string
	labels map[string][]string // family -> ordered label names
}

// NewRegistry builds an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		counters: map[string]map[string]*Counter{},
		gauges:   map[string]func() int64{},
		hists:    map[string]map[string]*Histogram{},
		help:     map[string]string{},
		labels:   map[string][]string{},
	}
}

// Counter returns the counter for a family and label values, creating it on
// first use. Label values are positional and must match the names given to
// DeclareCounter.
func (r *Registry) Counter(family string, labelValues ...string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := strings.Join(labelValues, "\x1f")
	fam := r.counters[family]
	if fam == nil {
		fam = map[string]*Counter{}
		r.counters[family] = fam
	}
	c := fam[key]
	if c == nil {
		c = &Counter{}
		fam[key] = c
	}
	return c
}

// Histogram returns the histogram for a family and label values, creating it
// on first use.
func (r *Registry) Histogram(family string, labelValues ...string) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := strings.Join(labelValues, "\x1f")
	fam := r.hists[family]
	if fam == nil {
		fam = map[string]*Histogram{}
		r.hists[family] = fam
	}
	h := fam[key]
	if h == nil {
		h = NewHistogram()
		fam[key] = h
	}
	return h
}

// DeclareCounter registers help text and label names for a counter family.
func (r *Registry) DeclareCounter(family, help string, labelNames ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.help[family] = help
	r.labels[family] = labelNames
}

// DeclareHistogram registers help text and label names for a histogram family.
func (r *Registry) DeclareHistogram(family, help string, labelNames ...string) {
	r.DeclareCounter(family, help, labelNames...)
}

// DeclareGauge registers a gauge backed by a function, so the value is read at
// scrape time rather than being pushed on every change. Cheaper and it cannot
// go stale.
func (r *Registry) DeclareGauge(family, help string, fn func() int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.help[family] = help
	r.gauges[family] = fn
}

// Render writes the whole registry in Prometheus text exposition format.
func (r *Registry) Render() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var families []metric
	for name, byLabel := range r.counters {
		m := metric{name: name, help: r.help[name], kind: "counter"}
		for key, c := range byLabel {
			m.samples = append(m.samples, sample{
				labels: zip(r.labels[name], splitKey(key)),
				value:  fmt.Sprintf("%d", c.Value()),
			})
		}
		families = append(families, m)
	}
	for name, fn := range r.gauges {
		families = append(families, metric{
			name: name, help: r.help[name], kind: "gauge",
			samples: []sample{{value: fmt.Sprintf("%d", fn())}},
		})
	}

	sort.Slice(
		families,
		func(i, j int) bool {
			return families[i].name < families[j].name
		},
	)

	var b strings.Builder
	for _, m := range families {
		fmt.Fprintf(
			&b,
			"# HELP %s %s\n# TYPE %s %s\n",
			m.name,
			m.help,
			m.name,
			m.kind,
		)
		sort.Slice(m.samples, func(i, j int) bool {
			return renderLabels(
				m.samples[i].labels,
			) < renderLabels(
				m.samples[j].labels,
			)
		})
		for _, s := range m.samples {
			fmt.Fprintf(
				&b,
				"%s%s %s\n",
				m.name,
				renderLabels(s.labels),
				s.value,
			)
		}
	}

	// Histograms render as three families each: bucket, sum, count.
	var histNames []string
	for name := range r.hists {
		histNames = append(histNames, name)
	}
	sort.Strings(histNames)
	for _, name := range histNames {
		fmt.Fprintf(
			&b,
			"# HELP %s %s\n# TYPE %s histogram\n",
			name,
			r.help[name],
			name,
		)
		var keys []string
		for k := range r.hists[name] {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			h := r.hists[name][k]
			base := zip(r.labels[name], splitKey(k))
			var cumulative uint64
			for i, bound := range histogramBuckets {
				cumulative += h.counts[i].Load()
				l := copyLabels(base)
				l["le"] = trimFloat(bound)
				fmt.Fprintf(
					&b,
					"%s_bucket%s %d\n",
					name,
					renderLabels(l),
					cumulative,
				)
			}
			l := copyLabels(base)
			l["le"] = "+Inf"
			fmt.Fprintf(
				&b,
				"%s_bucket%s %d\n",
				name,
				renderLabels(l),
				h.count.Load(),
			)
			fmt.Fprintf(
				&b,
				"%s_sum%s %f\n",
				name,
				renderLabels(base),
				float64(h.sum.Load())/1e9,
			)
			fmt.Fprintf(
				&b,
				"%s_count%s %d\n",
				name,
				renderLabels(base),
				h.count.Load(),
			)
		}
	}
	return b.String()
}

// zip pairs ordered label names with their values, ignoring any surplus.
func zip(names, values []string) map[string]string {
	out := map[string]string{}
	for i := range names {
		if i < len(values) && values[i] != "" {
			out[names[i]] = values[i]
		}
	}
	return out
}

// splitKey undoes the label-value join used as a map key.
func splitKey(k string) []string {
	if k == "" {
		return nil
	}
	return strings.Split(k, "\x1f")
}

// copyLabels returns a shallow copy so a rendered "le" does not leak between
// buckets.
func copyLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}

// renderLabels formats a label set as Prometheus expects, sorted so output is
// byte-identical between scrapes of identical state.
func renderLabels(l map[string]string) string {
	if len(l) == 0 {
		return ""
	}
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", k, l[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// trimFloat renders a bucket bound without scientific notation, which some
// scrapers dislike in the le label.
func trimFloat(f float64) string {
	s := fmt.Sprintf("%.6f", f)
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}

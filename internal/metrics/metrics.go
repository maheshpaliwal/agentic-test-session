// Package metrics is a small, dependency-free metrics registry that renders
// the Prometheus text exposition format (version 0.0.4). It covers what the
// tsession services need: counters, labelled counters and gauges computed at
// scrape time.
package metrics

import (
	"cmp"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds a set of metrics. The zero value is not usable; call New.
type Registry struct {
	mu      sync.Mutex
	metrics map[string]collector
}

// collector writes one metric family's samples.
type collector interface {
	kind() string
	help() string
	write(w io.Writer, name string)
}

// New returns an empty Registry.
func New() *Registry { return &Registry{metrics: map[string]collector{}} }

func (r *Registry) register(name string, c collector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !validName(name) {
		panic("metrics: invalid metric name " + strconv.Quote(name))
	}
	if _, dup := r.metrics[name]; dup {
		panic("metrics: duplicate metric " + name)
	}
	r.metrics[name] = c
}

// validName reports whether s is a legal metric or label name.
func validName(s string) bool {
	if s == "" {
		return false
	}
	for i, c := range s {
		letter := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if !letter && (i == 0 || c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// Counter is a monotonically increasing value.
type Counter struct {
	v    atomic.Uint64
	desc string
}

// Inc adds one.
func (c *Counter) Inc() { c.v.Add(1) }

// Add adds n; negative values are ignored (a counter never goes down).
func (c *Counter) Add(n int) {
	if n > 0 {
		c.v.Add(uint64(n))
	}
}

// Value returns the current count.
func (c *Counter) Value() uint64 { return c.v.Load() }

func (c *Counter) kind() string { return "counter" }
func (c *Counter) help() string { return c.desc }
func (c *Counter) write(w io.Writer, name string) {
	fmt.Fprintf(w, "%s %d\n", name, c.v.Load())
}

// Counter registers and returns a counter. It panics on an invalid or
// duplicate name, which is a programming error.
func (r *Registry) Counter(name, help string) *Counter {
	c := &Counter{desc: help}
	r.register(name, c)
	return c
}

// CounterVec is a family of counters distinguished by label values.
type CounterVec struct {
	desc   string
	labels []string

	mu       sync.RWMutex
	children map[string]*Counter // key: label values joined by \xff
}

// CounterVec registers and returns a labelled counter family.
func (r *Registry) CounterVec(name, help string, labels ...string) *CounterVec {
	for _, l := range labels {
		if !validName(l) {
			panic("metrics: invalid label name " + strconv.Quote(l))
		}
	}
	v := &CounterVec{desc: help, labels: labels, children: map[string]*Counter{}}
	r.register(name, v)
	return v
}

// With returns the counter for the given label values, creating it on first
// use. It panics when the number of values differs from the number of labels.
func (v *CounterVec) With(values ...string) *Counter {
	if len(values) != len(v.labels) {
		panic(fmt.Sprintf("metrics: got %d label values, want %d", len(values), len(v.labels)))
	}
	key := strings.Join(values, "\xff")
	v.mu.RLock()
	c := v.children[key] // one map read: cannot panic
	v.mu.RUnlock()
	if c != nil {
		return c
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if c = v.children[key]; c == nil {
		c = &Counter{}
		v.children[key] = c
	}
	return c
}

func (v *CounterVec) kind() string { return "counter" }
func (v *CounterVec) help() string { return v.desc }
func (v *CounterVec) write(w io.Writer, name string) {
	v.mu.RLock()
	defer v.mu.RUnlock() // w is the caller's: a panic of it must not leave With waiting
	for _, k := range slices.Sorted(maps.Keys(v.children)) {
		fmt.Fprintf(w, "%s%s %d\n", name, labelSet(v.labels, strings.Split(k, "\xff")), v.children[k].v.Load())
	}
}

// Sample is one labelled gauge value produced at scrape time.
type Sample struct {
	Labels []string // values, in the order the label names were registered
	Value  float64
}

type gaugeFunc struct {
	desc    string
	labels  []string
	fn      func() []Sample
	counter bool // exposed as a counter: fn never goes down
}

func (g *gaugeFunc) kind() string {
	if g.counter {
		return "counter"
	}
	return "gauge"
}
func (g *gaugeFunc) help() string { return g.desc }
func (g *gaugeFunc) write(w io.Writer, name string) {
	samples := g.fn()
	slices.SortStableFunc(samples, func(a, b Sample) int {
		return cmp.Compare(strings.Join(a.Labels, "\xff"), strings.Join(b.Labels, "\xff"))
	})
	for _, s := range samples {
		if len(s.Labels) != len(g.labels) {
			continue // a malformed sample must not corrupt the exposition
		}
		fmt.Fprintf(w, "%s%s %s\n", name, labelSet(g.labels, s.Labels), formatFloat(s.Value))
	}
}

// GaugeFunc registers a gauge whose value is computed on every scrape. fn
// must be safe for concurrent use and should be cheap.
func (r *Registry) GaugeFunc(name, help string, fn func() float64) {
	r.register(name, &gaugeFunc{desc: help, fn: func() []Sample { return []Sample{{Value: fn()}} }})
}

// CounterFunc registers a counter whose value is read on every scrape, for a
// count that is kept elsewhere. fn must be safe for concurrent use, cheap, and
// must never return less than it did before.
func (r *Registry) CounterFunc(name, help string, fn func() float64) {
	r.register(name, &gaugeFunc{desc: help, counter: true, fn: func() []Sample { return []Sample{{Value: fn()}} }})
}

// GaugeVecFunc registers a labelled gauge family computed on every scrape.
func (r *Registry) GaugeVecFunc(name, help string, labels []string, fn func() []Sample) {
	for _, l := range labels {
		if !validName(l) {
			panic("metrics: invalid label name " + strconv.Quote(l))
		}
	}
	r.register(name, &gaugeFunc{desc: help, labels: labels, fn: fn})
}

func labelSet(names, values []string) string {
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n)
		b.WriteString(`="`)
		b.WriteString(escape(values[i], true))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// escape applies the exposition format's escaping: backslash and newline
// everywhere, and the double quote inside label values.
func escape(s string, quote bool) string {
	if !strings.ContainsAny(s, "\\\n\"") {
		return s
	}
	var b strings.Builder
	for _, c := range s {
		switch {
		case c == '\\':
			b.WriteString(`\\`)
		case c == '\n':
			b.WriteString(`\n`)
		case c == '"' && quote:
			b.WriteString(`\"`)
		default:
			b.WriteRune(c)
		}
	}
	return b.String()
}

func formatFloat(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// sorted lists the metrics by name.
func (r *Registry) sorted() ([]string, []collector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := slices.Sorted(maps.Keys(r.metrics))
	cs := make([]collector, len(names))
	for i, n := range names {
		cs[i] = r.metrics[n]
	}
	return names, cs
}

// WriteTo renders every metric, sorted by name, in the text exposition format.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	names, cs := r.sorted()

	var b strings.Builder
	for i, n := range names {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", n, escape(cs[i].help(), false), n, cs[i].kind())
		cs[i].write(&b, n)
	}
	nw, err := io.WriteString(w, b.String())
	return int64(nw), err
}

// Handler serves the registry in the Prometheus text exposition format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = r.WriteTo(w)
	})
}

// BuildInfo registers tsession_build_info, a constant 1 labelled with the
// service name, its module version and the Go version it was built with.
func (r *Registry) BuildInfo(service, version, goVersion string) {
	r.GaugeVecFunc("tsession_build_info", "Build information; the value is always 1.",
		[]string{"service", "version", "goversion"},
		func() []Sample { return []Sample{{Labels: []string{service, version, goVersion}, Value: 1}} })
}

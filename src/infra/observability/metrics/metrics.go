// Package metrics is a small dependency-free metrics registry that renders the
// Prometheus text exposition format 0.0.4.
package metrics

import (
	"io"
	"math"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// DefBuckets are latency buckets in seconds suited to an HTTP API.
var DefBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

type collector interface {
	name() string
	write(w *strings.Builder)
}

// Registry holds collectors and renders them sorted by name.
type Registry struct {
	mu         sync.RWMutex
	collectors map[string]collector
	funcs      []func(w *strings.Builder)
}

func NewRegistry() *Registry { return &Registry{collectors: map[string]collector{}} }

func (r *Registry) register(c collector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.collectors[c.name()]; dup {
		panic("metrics: duplicate metric " + c.name())
	}
	r.collectors[c.name()] = c
}

// ServeHTTP renders all metrics.
func (r *Registry) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", ContentType)
	_, _ = io.WriteString(w, r.Render())
}

func (r *Registry) Handler() http.Handler { return r }

// Render returns the exposition text.
func (r *Registry) Render() string {
	r.mu.RLock()
	names := make([]string, 0, len(r.collectors))
	for n := range r.collectors {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		r.collectors[n].write(&b)
	}
	r.mu.RUnlock()
	return b.String()
}

// ---- label helpers ----

func escapeLabel(s string) string {
	if !strings.ContainsAny(s, "\\\"\n") {
		return s
	}
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

func escapeHelp(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(s)
}

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// labelString renders {k="v",...} with keys sorted. extra is appended last (e.g. le).
func labelString(keys, vals []string, extraK, extraV string) string {
	if len(keys) == 0 && extraK == "" {
		return ""
	}
	idx := make([]int, len(keys))
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool { return keys[idx[a]] < keys[idx[b]] })
	var b strings.Builder
	b.WriteByte('{')
	for n, i := range idx {
		if n > 0 {
			b.WriteByte(',')
		}
		b.WriteString(keys[i])
		b.WriteString(`="`)
		b.WriteString(escapeLabel(vals[i]))
		b.WriteByte('"')
	}
	if extraK != "" {
		if len(idx) > 0 {
			b.WriteByte(',')
		}
		b.WriteString(extraK)
		b.WriteString(`="`)
		b.WriteString(escapeLabel(extraV))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func header(w *strings.Builder, name, help, typ string) {
	w.WriteString("# HELP " + name + " " + escapeHelp(help) + "\n")
	w.WriteString("# TYPE " + name + " " + typ + "\n")
}

const sep = "\xff"

func key(vals []string) string { return strings.Join(vals, sep) }

// ---- Counter ----

type counterVec struct {
	nm, help string
	keys     []string
	mu       sync.RWMutex
	series   map[string]*counterSeries
}

type counterSeries struct {
	vals []string
	v    atomic.Uint64
}

// CounterVec is a labelled monotonically increasing counter.
type CounterVec struct{ c *counterVec }

func (r *Registry) NewCounterVec(name, help string, labelKeys ...string) *CounterVec {
	c := &counterVec{nm: name, help: help, keys: labelKeys, series: map[string]*counterSeries{}}
	r.register(c)
	return &CounterVec{c}
}

func (c *counterVec) name() string { return c.nm }

// Inc adds one to the series identified by label values (same order as keys).
func (cv *CounterVec) Inc(vals ...string) { cv.Add(1, vals...) }

func (cv *CounterVec) Add(n uint64, vals ...string) {
	c := cv.c
	if len(vals) != len(c.keys) {
		return
	}
	k := key(vals)
	c.mu.RLock()
	s := c.series[k]
	c.mu.RUnlock()
	if s == nil {
		c.mu.Lock()
		if s = c.series[k]; s == nil {
			s = &counterSeries{vals: append([]string(nil), vals...)}
			c.series[k] = s
		}
		c.mu.Unlock()
	}
	s.v.Add(n)
}

func (c *counterVec) write(w *strings.Builder) {
	header(w, c.nm, c.help, "counter")
	c.mu.RLock()
	lines := make([]string, 0, len(c.series))
	for _, s := range c.series {
		lines = append(lines, c.nm+labelString(c.keys, s.vals, "", "")+" "+strconv.FormatUint(s.v.Load(), 10)+"\n")
	}
	c.mu.RUnlock()
	sort.Strings(lines)
	for _, l := range lines {
		w.WriteString(l)
	}
}

// ---- Gauge ----

type gauge struct {
	nm, help string
	bits     atomic.Uint64
	fn       func() float64
	keys     []string
	vals     []string
}

// Gauge is a settable value, optionally computed at scrape time.
type Gauge struct{ g *gauge }

func (g *gauge) name() string { return g.nm }

func (r *Registry) NewGauge(name, help string) *Gauge {
	g := &gauge{nm: name, help: help}
	r.register(g)
	return &Gauge{g}
}

// NewGaugeFunc registers a gauge whose value is computed on every scrape.
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) {
	r.register(&gauge{nm: name, help: help, fn: fn})
}

// NewConstGauge registers a gauge with fixed labels and value (e.g. build info).
func (r *Registry) NewConstGauge(name, help string, value float64, labels map[string]string) {
	g := &gauge{nm: name, help: help}
	for k := range labels {
		g.keys = append(g.keys, k)
	}
	sort.Strings(g.keys)
	for _, k := range g.keys {
		g.vals = append(g.vals, labels[k])
	}
	g.bits.Store(math.Float64bits(value))
	r.register(g)
}

func (g *Gauge) Set(v float64) { g.g.bits.Store(math.Float64bits(v)) }
func (g *Gauge) Add(d float64) {
	for {
		old := g.g.bits.Load()
		if g.g.bits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+d)) {
			return
		}
	}
}
func (g *Gauge) Inc() { g.Add(1) }
func (g *Gauge) Dec() { g.Add(-1) }

func (g *gauge) write(w *strings.Builder) {
	header(w, g.nm, g.help, "gauge")
	v := math.Float64frombits(g.bits.Load())
	if g.fn != nil {
		v = g.fn()
	}
	w.WriteString(g.nm + labelString(g.keys, g.vals, "", "") + " " + formatFloat(v) + "\n")
}

// ---- Histogram ----

type histSeries struct {
	vals    []string
	buckets []atomic.Uint64 // non-cumulative; last slot is +Inf
	count   atomic.Uint64
	sumBits atomic.Uint64
}

type histogramVec struct {
	nm, help string
	keys     []string
	bounds   []float64
	mu       sync.RWMutex
	series   map[string]*histSeries
}

// HistogramVec is a labelled histogram.
type HistogramVec struct{ h *histogramVec }

func (h *histogramVec) name() string { return h.nm }

// NewHistogramVec registers a histogram; buckets must be sorted ascending (nil uses DefBuckets).
func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labelKeys ...string) *HistogramVec {
	if buckets == nil {
		buckets = DefBuckets
	}
	h := &histogramVec{nm: name, help: help, keys: labelKeys, bounds: append([]float64(nil), buckets...), series: map[string]*histSeries{}}
	r.register(h)
	return &HistogramVec{h}
}

func (hv *HistogramVec) Observe(v float64, vals ...string) {
	h := hv.h
	if len(vals) != len(h.keys) {
		return
	}
	k := key(vals)
	h.mu.RLock()
	s := h.series[k]
	h.mu.RUnlock()
	if s == nil {
		h.mu.Lock()
		if s = h.series[k]; s == nil {
			s = &histSeries{vals: append([]string(nil), vals...), buckets: make([]atomic.Uint64, len(h.bounds)+1)}
			h.series[k] = s
		}
		h.mu.Unlock()
	}
	i := sort.SearchFloat64s(h.bounds, v) // first bound >= v
	s.buckets[i].Add(1)
	s.count.Add(1)
	for {
		old := s.sumBits.Load()
		if s.sumBits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			break
		}
	}
}

func (h *histogramVec) write(w *strings.Builder) {
	header(w, h.nm, h.help, "histogram")
	h.mu.RLock()
	list := make([]*histSeries, 0, len(h.series))
	for _, s := range h.series {
		list = append(list, s)
	}
	h.mu.RUnlock()
	sort.Slice(list, func(a, b int) bool { return key(list[a].vals) < key(list[b].vals) })
	for _, s := range list {
		var cum uint64
		for i, b := range h.bounds {
			cum += s.buckets[i].Load()
			w.WriteString(h.nm + "_bucket" + labelString(h.keys, s.vals, "le", formatFloat(b)) + " " + strconv.FormatUint(cum, 10) + "\n")
		}
		cum += s.buckets[len(h.bounds)].Load()
		w.WriteString(h.nm + "_bucket" + labelString(h.keys, s.vals, "le", "+Inf") + " " + strconv.FormatUint(cum, 10) + "\n")
		ls := labelString(h.keys, s.vals, "", "")
		w.WriteString(h.nm + "_sum" + ls + " " + formatFloat(math.Float64frombits(s.sumBits.Load())) + "\n")
		w.WriteString(h.nm + "_count" + ls + " " + strconv.FormatUint(cum, 10) + "\n")
	}
}

// ---- HTTP middleware + default registry ----

// HTTP bundles the request instruments.
type HTTP struct {
	Requests *CounterVec
	Duration *HistogramVec
	InFlight *Gauge
}

// Default builds a registry with HTTP, process, runtime and build_info metrics.
// version is the build identifier; empty falls back to env GIT_SHA, then "unknown".
func Default(version string) (*Registry, *HTTP) {
	r := NewRegistry()
	h := &HTTP{
		Requests: r.NewCounterVec("http_requests_total", "Total HTTP requests by method, route pattern and status.", "method", "route", "status"),
		Duration: r.NewHistogramVec("http_request_duration_seconds", "HTTP request latency in seconds by method, route pattern and status.", nil, "method", "route", "status"),
		InFlight: r.NewGauge("http_requests_in_flight", "HTTP requests currently being served."),
	}
	if version == "" {
		version = os.Getenv("GIT_SHA")
	}
	if version == "" {
		version = "unknown"
	}
	r.NewConstGauge("edp_build_info", "Build information; value is always 1.", 1, map[string]string{"version": version})
	r.NewConstGauge("process_start_time_seconds", "Start time of the process since unix epoch in seconds.", float64(time.Now().UnixNano())/1e9, nil)
	r.NewGaugeFunc("go_goroutines", "Number of goroutines that currently exist.", func() float64 { return float64(runtime.NumGoroutine()) })
	var ms runtime.MemStats
	var msMu sync.Mutex
	read := func(f func(*runtime.MemStats) float64) func() float64 {
		return func() float64 {
			msMu.Lock()
			defer msMu.Unlock()
			runtime.ReadMemStats(&ms)
			return f(&ms)
		}
	}
	r.NewGaugeFunc("go_memstats_heap_alloc_bytes", "Bytes of allocated heap objects.", read(func(m *runtime.MemStats) float64 { return float64(m.HeapAlloc) }))
	r.NewGaugeFunc("go_gc_cycles_total", "Number of completed GC cycles.", read(func(m *runtime.MemStats) float64 { return float64(m.NumGC) }))
	return r, h
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// Middleware records request count, latency and in-flight gauge. The route label
// is r.Pattern as set by ServeMux (bounded cardinality), or "unmatched". It must
// wrap the ServeMux directly so it sees the request the mux annotates; panics are
// counted as 500 and re-raised for the recovery middleware.
func (h *HTTP) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		h.InFlight.Inc()
		sw := &statusWriter{ResponseWriter: w}
		defer func() {
			h.InFlight.Dec()
			rec := recover()
			status := sw.status
			if rec != nil {
				status = http.StatusInternalServerError // outer recovery middleware will answer 500
			} else if status == 0 {
				status = http.StatusOK
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			st := strconv.Itoa(status)
			h.Requests.Inc(r.Method, route, st)
			h.Duration.Observe(time.Since(start).Seconds(), r.Method, route, st)
			if rec != nil {
				panic(rec)
			}
		}()
		next.ServeHTTP(sw, r)
	})
}


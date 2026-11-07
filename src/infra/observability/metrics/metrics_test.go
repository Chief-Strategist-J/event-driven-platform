package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestExpositionFormat(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("t_total", "A counter.\nline2", "b", "a")
	c.Inc("x", `q"u\o`+"\n")
	c.Inc("x", `q"u\o`+"\n")
	h := r.NewHistogramVec("t_seconds", "Hist.", []float64{0.1, 1}, "m")
	h.Observe(0.05, "GET")
	h.Observe(0.5, "GET")
	h.Observe(5, "GET")
	g := r.NewGauge("t_g", "G.")
	g.Set(2)
	g.Dec()
	out := r.Render()
	for _, want := range []string{
		"# HELP t_total A counter.\\nline2\n# TYPE t_total counter\n",
		`t_total{a="q\"u\\o\n",b="x"} 2` + "\n",
		"# TYPE t_seconds histogram\n",
		`t_seconds_bucket{m="GET",le="0.1"} 1` + "\n",
		`t_seconds_bucket{m="GET",le="1"} 2` + "\n",
		`t_seconds_bucket{m="GET",le="+Inf"} 3` + "\n",
		`t_seconds_sum{m="GET"} 5.55` + "\n",
		`t_seconds_count{m="GET"} 3` + "\n",
		"# TYPE t_g gauge\nt_g 1\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Index(out, "t_g") > strings.Index(out, "t_seconds") || strings.Index(out, "t_seconds") > strings.Index(out, "t_total") {
		t.Errorf("metrics not sorted by name:\n%s", out)
	}
}

func TestDefaultMetricsAndMiddleware(t *testing.T) {
	t.Setenv("GIT_SHA", "abc123")
	reg, h := Default("")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /items/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	mux.HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("x") })
	srv := h.Middleware(mux)
	for _, p := range []string{"/items/1", "/items/2", "/nope"} {
		srv.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, p, nil))
	}
	func() {
		defer func() { _ = recover() }()
		srv.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/boom", nil))
	}()
	rec := httptest.NewRecorder()
	reg.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if ct := rec.Header().Get("Content-Type"); ct != ContentType {
		t.Fatalf("content-type %q", ct)
	}
	out := rec.Body.String()
	for _, want := range []string{
		`http_requests_total{method="GET",route="GET /items/{id}",status="418"} 2`,
		`http_requests_total{method="GET",route="unmatched",status="404"} 1`,
		`http_requests_total{method="GET",route="GET /boom",status="500"} 1`,
		`http_request_duration_seconds_bucket{method="GET",route="GET /items/{id}",status="418",le="+Inf"} 2`,
		"http_requests_in_flight 0",
		`edp_build_info{version="abc123"} 1`,
		"process_start_time_seconds ", "go_goroutines ", "go_memstats_heap_alloc_bytes ", "go_gc_cycles_total ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "/items/1") {
		t.Error("raw path leaked into labels")
	}
}

func TestConcurrentUse(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("c_total", "c", "k")
	h := r.NewHistogramVec("h_seconds", "h", nil, "k")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				c.Inc("a")
				h.Observe(0.01, "a")
				_ = r.Render()
			}
		}()
	}
	wg.Wait()
	if !strings.Contains(r.Render(), `c_total{k="a"} 10000`) || !strings.Contains(r.Render(), `h_seconds_count{k="a"} 10000`) {
		t.Fatal("lost updates")
	}
}

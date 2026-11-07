package adapters

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"event-driven-platform/src/features/cdc-integration/contracts"
	apperr "event-driven-platform/src/shared/errors"
)

const testSecret = "s3cr3t-pw"

func newTestAdapter(url string) *KafkaConnectRESTAdapter {
	a := NewKafkaConnectRESTAdapter(url, DebeziumDBConfig{Host: "db", Port: "5432", User: "u", Password: testSecret, DBName: "event_store", RawTableList: "public\\..*"})
	a.backoff = time.Millisecond
	return a
}

func in(mode contracts.CDCMode) contracts.RegisterConnectorInput {
	return contracts.RegisterConnectorInput{ConnectorName: "c1", SlotName: "slot1", PublicationName: "pub1", Mode: mode}
}

func TestCreateRequestShape(t *testing.T) {
	var method, path string
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		w.WriteHeader(201)
	}))
	defer srv.Close()
	if err := newTestAdapter(srv.URL).CreateConnector(context.Background(), in(contracts.ModeOutbox)); err != nil {
		t.Fatal(err)
	}
	if method != "POST" || path != "/connectors" || body["name"] != "c1" {
		t.Fatalf("bad request %s %s %v", method, path, body)
	}
	cfg := body["config"].(map[string]any)
	want := map[string]string{
		"connector.class":    "io.debezium.connector.postgresql.PostgresConnector",
		"plugin.name":        "pgoutput",
		"slot.name":          "slot1",
		"publication.name":   "pub1",
		"topic.prefix":       "edp",
		"table.include.list": "public.outbox",
		"database.hostname":  "db",
		"database.dbname":    "event_store",
		"database.password":  testSecret,
	}
	for k, v := range want {
		if cfg[k] != v {
			t.Errorf("config[%s]=%v want %s", k, cfg[k], v)
		}
	}
}

func TestRawTableMode(t *testing.T) {
	cfg := newTestAdapter("http://x").BuildConnectorConfig(in(contracts.ModeRawTable))
	if cfg["table.include.list"] != "public\\..*" {
		t.Fatal(cfg["table.include.list"])
	}
}

func TestLifecycleShapes(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Method+" "+r.URL.Path)
		w.WriteHeader(202)
	}))
	defer srv.Close()
	a := newTestAdapter(srv.URL)
	ctx := context.Background()
	for _, f := range []func(context.Context, string) error{a.PauseConnector, a.ResumeConnector, a.RestartConnector, a.DeleteConnector} {
		if err := f(ctx, "c1"); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"PUT /connectors/c1/pause", "PUT /connectors/c1/resume", "POST /connectors/c1/restart", "DELETE /connectors/c1"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v", got)
	}
}

func TestErrorMapping(t *testing.T) {
	cases := map[int]int{409: 409, 404: 404, 500: 500, 400: 500}
	for upstream, wantStatus := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(upstream)
			_, _ = w.Write([]byte("internal stacktrace " + testSecret))
		}))
		a := newTestAdapter(srv.URL)
		err := a.CreateConnector(context.Background(), in(contracts.ModeOutbox))
		srv.Close()
		var ae *apperr.ApplicationError
		if !asApp(err, &ae) || ae.StatusCode != wantStatus {
			t.Fatalf("upstream %d: got %v", upstream, err)
		}
		if strings.Contains(err.Error(), testSecret) || strings.Contains(err.Error(), "stacktrace") {
			t.Fatalf("leak in %q", err.Error())
		}
	}
}

func asApp(err error, target **apperr.ApplicationError) bool {
	ae, ok := err.(*apperr.ApplicationError)
	if ok {
		*target = ae
	}
	return ok
}

func TestRetryIdempotentOn503(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(202)
	}))
	defer srv.Close()
	if err := newTestAdapter(srv.URL).PauseConnector(context.Background(), "c1"); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("calls=%d", n)
	}
}

func TestNoRetryOnCreate(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&n, 1)
		w.WriteHeader(503)
	}))
	defer srv.Close()
	if err := newTestAdapter(srv.URL).CreateConnector(context.Background(), in(contracts.ModeOutbox)); err == nil {
		t.Fatal("expected error")
	}
	if n != 1 {
		t.Fatalf("calls=%d", n)
	}
}

func TestRetryBounded(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&n, 1)
		w.WriteHeader(502)
	}))
	defer srv.Close()
	if err := newTestAdapter(srv.URL).DeleteConnector(context.Background(), "c1"); err == nil || n != 3 {
		t.Fatalf("err=%v calls=%d", err, n)
	}
}

func TestConnectionErrorNoLeak(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()
	err := newTestAdapter(url).CreateConnector(context.Background(), in(contracts.ModeOutbox))
	if err == nil || strings.Contains(err.Error(), testSecret) || strings.Contains(err.Error(), url) {
		t.Fatalf("err=%v", err)
	}
}

func TestStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"connector":{"state":"RUNNING"}}`))
	}))
	defer srv.Close()
	s, err := newTestAdapter(srv.URL).ConnectorStatus(context.Background(), "c1")
	if err != nil || s != "RUNNING" {
		t.Fatal(s, err)
	}
}

func TestPasswordFileAndDefaults(t *testing.T) {
	t.Setenv("CDC_DB_PASSWORD", "")
	f := t.TempDir() + "/pw"
	if err := writeFile(f, "filepw\n"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CDC_DB_PASSWORD_FILE", f)
	t.Setenv("CDC_DB_HOST", "")
	cfg := LoadDebeziumDBConfigFromEnv()
	if cfg.Password != "filepw" || cfg.Host != "db" || cfg.Port != "5432" || cfg.User != "postgres" || cfg.DBName != "event_store" {
		t.Fatalf("%+v", cfg)
	}
}

package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"event-driven-platform/src/features/cdc-integration/contracts"
	"event-driven-platform/src/shared/errors"
)

// DebeziumDBConfig holds source database settings for the Debezium Postgres connector.
// Password is never logged, returned in errors or exposed through any accessor.
type DebeziumDBConfig struct {
	Host         string
	Port         string
	User         string
	Password     string
	DBName       string
	RawTableList string
}

// LoadDebeziumDBConfigFromEnv reads CDC_DB_HOST, CDC_DB_PORT, CDC_DB_USER,
// CDC_DB_PASSWORD (or CDC_DB_PASSWORD_FILE), CDC_DB_NAME and
// CDC_RAW_TABLE_INCLUDE_LIST, applying compose-stack defaults.
func LoadDebeziumDBConfigFromEnv() DebeziumDBConfig {
	cfg := DebeziumDBConfig{
		Host:         envOr("CDC_DB_HOST", "db"),
		Port:         envOr("CDC_DB_PORT", "5432"),
		User:         envOr("CDC_DB_USER", "postgres"),
		Password:     os.Getenv("CDC_DB_PASSWORD"),
		DBName:       envOr("CDC_DB_NAME", "event_store"),
		RawTableList: envOr("CDC_RAW_TABLE_INCLUDE_LIST", "public\\..*"),
	}
	if cfg.Password == "" {
		if path := strings.TrimSpace(os.Getenv("CDC_DB_PASSWORD_FILE")); path != "" {
			if b, err := os.ReadFile(path); err != nil {
				slog.Error("CDC: cannot read CDC_DB_PASSWORD_FILE", "error_type", fmt.Sprintf("%T", err))
			} else {
				cfg.Password = strings.TrimRight(string(b), "\r\n")
			}
		}
	}
	return cfg
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// KafkaConnectRESTAdapter talks to the Kafka Connect REST API.
type KafkaConnectRESTAdapter struct {
	baseURL     string
	client      *http.Client
	db          DebeziumDBConfig
	maxAttempts int
	backoff     time.Duration
}

func NewKafkaConnectRESTAdapter(baseURL string, db DebeziumDBConfig) *KafkaConnectRESTAdapter {
	return &KafkaConnectRESTAdapter{
		baseURL:     strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		client:      &http.Client{Timeout: 10 * time.Second},
		db:          db,
		maxAttempts: 3,
		backoff:     200 * time.Millisecond,
	}
}

// BuildConnectorConfig renders the Debezium Postgres connector properties.
func (a *KafkaConnectRESTAdapter) BuildConnectorConfig(input contracts.RegisterConnectorInput) map[string]string {
	tables := "public.outbox"
	if input.Mode == contracts.ModeRawTable {
		tables = a.db.RawTableList
	}
	return map[string]string{
		"connector.class":             "io.debezium.connector.postgresql.PostgresConnector",
		"tasks.max":                   "1",
		"plugin.name":                 "pgoutput",
		"database.hostname":           a.db.Host,
		"database.port":               a.db.Port,
		"database.user":               a.db.User,
		"database.password":           a.db.Password,
		"database.dbname":             a.db.DBName,
		"topic.prefix":                "edp",
		"table.include.list":          tables,
		"snapshot.mode":               "never",
		"slot.name":                   input.SlotName,
		"publication.name":            input.PublicationName,
		"publication.autocreate.mode": "filtered",
		"heartbeat.interval.ms":       "5000",
		"tombstones.on.delete":        "false",
	}
}

func (a *KafkaConnectRESTAdapter) CreateConnector(ctx context.Context, input contracts.RegisterConnectorInput) error {
	body, err := json.Marshal(map[string]any{
		"name":   input.ConnectorName,
		"config": a.BuildConnectorConfig(input),
	})
	if err != nil {
		return errors.NewInternalError("failed to build connector request")
	}
	// POST create is not idempotent: never retried.
	return a.do(ctx, http.MethodPost, "/connectors", body, input.ConnectorName, false)
}

func (a *KafkaConnectRESTAdapter) PauseConnector(ctx context.Context, name string) error {
	return a.do(ctx, http.MethodPut, connPath(name, "pause"), nil, name, true)
}

func (a *KafkaConnectRESTAdapter) ResumeConnector(ctx context.Context, name string) error {
	return a.do(ctx, http.MethodPut, connPath(name, "resume"), nil, name, true)
}

func (a *KafkaConnectRESTAdapter) RestartConnector(ctx context.Context, name string) error {
	return a.do(ctx, http.MethodPost, connPath(name, "restart"), nil, name, true)
}

func (a *KafkaConnectRESTAdapter) DeleteConnector(ctx context.Context, name string) error {
	return a.do(ctx, http.MethodDelete, connPath(name, ""), nil, name, true)
}

// ConnectorStatus returns the raw connector state (e.g. RUNNING, PAUSED, FAILED).
func (a *KafkaConnectRESTAdapter) ConnectorStatus(ctx context.Context, name string) (string, error) {
	var out string
	err := a.doRaw(ctx, http.MethodGet, connPath(name, "status"), nil, name, true, func(b []byte) {
		var s struct {
			Connector struct {
				State string `json:"state"`
			} `json:"connector"`
		}
		if json.Unmarshal(b, &s) == nil {
			out = s.Connector.State
		}
	})
	return out, err
}

func connPath(name, suffix string) string {
	p := "/connectors/" + url.PathEscape(name)
	if suffix != "" {
		p += "/" + suffix
	}
	return p
}

func (a *KafkaConnectRESTAdapter) do(ctx context.Context, method, path string, body []byte, name string, idempotent bool) error {
	return a.doRaw(ctx, method, path, body, name, idempotent, nil)
}

func (a *KafkaConnectRESTAdapter) doRaw(ctx context.Context, method, path string, body []byte, name string, idempotent bool, onOK func([]byte)) error {
	attempts := 1
	if idempotent {
		attempts = a.maxAttempts
	}
	var lastErr error
	for i := 1; i <= attempts; i++ {
		var rdr io.Reader
		if body != nil {
			rdr = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, a.baseURL+path, rdr)
		if err != nil {
			return errors.NewInternalError("failed to build kafka connect request")
		}
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := a.client.Do(req)
		retry := false
		if err != nil {
			if ctx.Err() != nil {
				return errors.NewTimeoutError("kafka connect request cancelled or timed out")
			}
			slog.Error("CDC: kafka connect transport error", "method", method, "path", path, "error_type", fmt.Sprintf("%T", err))
			lastErr = errors.NewInternalError("kafka connect is unavailable")
			retry = true
		} else {
			respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
			resp.Body.Close()
			switch {
			case resp.StatusCode >= 200 && resp.StatusCode < 300:
				if onOK != nil {
					onOK(respBody)
				}
				return nil
			case resp.StatusCode == http.StatusConflict:
				return errors.NewConflictError(fmt.Sprintf("connector conflict in connect cluster: %s", name))
			case resp.StatusCode == http.StatusNotFound:
				return errors.NewNotFoundError(fmt.Sprintf("connector not found: %s", name))
			default:
				slog.Error("CDC: kafka connect upstream error", "method", method, "path", path, "status", resp.StatusCode, "body", truncate(string(respBody), 512))
				lastErr = errors.NewInternalError("kafka connect request failed")
				retry = resp.StatusCode >= 500
			}
		}
		if !retry || i == attempts {
			break
		}
		select {
		case <-ctx.Done():
			return errors.NewTimeoutError("kafka connect request cancelled or timed out")
		case <-time.After(a.backoff * time.Duration(i)):
		}
	}
	return lastErr
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

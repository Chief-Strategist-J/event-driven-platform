package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type ServerConfig struct {
	Name                    string `yaml:"name"`
	BindAddress             string `yaml:"bind_address"`
	AdminBindAddress        string `yaml:"admin_bind_address"`
	IdleTimeoutSeconds      int    `yaml:"idle_timeout_seconds"`
	HeaderTimeoutSeconds    int    `yaml:"header_timeout_seconds"`
	DrainDeadlineSeconds    int    `yaml:"drain_deadline_seconds"`
	PropagationWaitSeconds  int    `yaml:"propagation_wait_seconds"`
	ShutdownDeadlineSeconds int    `yaml:"shutdown_deadline_seconds"`
}

type DatabaseConfig struct {
	URL                    string `yaml:"url"`
	MaxOpenConns           int    `yaml:"max_open_conns"`
	MaxIdleConns           int    `yaml:"max_idle_conns"`
	ConnMaxLifetimeSeconds int    `yaml:"conn_max_lifetime_seconds"`
}

type SchemaRegistryConfig struct {
	URL string `yaml:"url"`
}

type BrokerConfig struct {
	KafkaBrokers string `yaml:"kafka_brokers"`
}

type TelemetryConfig struct {
	OtlpEndpoint string `yaml:"otlp_endpoint"`
	ServiceName  string `yaml:"service_name"`
}

type EndpointsConfig struct {
	BasePath string `yaml:"base_path"`
}

type Config struct {
	Server         ServerConfig         `yaml:"server"`
	Database       DatabaseConfig       `yaml:"database"`
	SchemaRegistry SchemaRegistryConfig `yaml:"schema_registry"`
	Broker         BrokerConfig         `yaml:"broker"`
	Telemetry      TelemetryConfig      `yaml:"telemetry"`
	Endpoints      EndpointsConfig      `yaml:"endpoints"`
}

var envPattern = regexp.MustCompile(`\$\{([A-Za-z_0-9]+)(?::-([^}]*))?\}`)

func LoadConfig(configPath string) (*Config, error) {
	if configPath == "" {
		configPath = "config/default.yaml"
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return loadDefaultFallback()
	}

	expanded := envPattern.ReplaceAllStringFunc(string(data), func(match string) string {
		submatches := envPattern.FindStringSubmatch(match)
		if len(submatches) >= 2 {
			envKey := submatches[1]
			val := os.Getenv(envKey)
			if val != "" {
				return val
			}
			if len(submatches) >= 3 {
				return submatches[2]
			}
		}
		return match
	})

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func loadDefaultFallback() (*Config, error) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@127.0.0.1:5432/event_store?sslmode=disable"
	}
	schemaURL := os.Getenv("SCHEMA_REGISTRY_URL")
	if schemaURL == "" {
		schemaURL = "http://127.0.0.1:8082/apis/registry/v2"
	}
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "127.0.0.1:9092"
	}
	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otlpEndpoint == "" {
		otlpEndpoint = "127.0.0.1:4317"
	}

	port := os.Getenv("PORT")
	bindAddr := "0.0.0.0:8080"
	if port != "" {
		bindAddr = "0.0.0.0:" + port
	}

	adminPort := os.Getenv("ADMIN_PORT")
	adminBindAddr := "127.0.0.1:8081"
	if adminPort != "" {
		adminBindAddr = "127.0.0.1:" + adminPort
	}

	cfg := &Config{
		Server: ServerConfig{
			Name:                    "event-driven-platform",
			BindAddress:             bindAddr,
			AdminBindAddress:        adminBindAddr,
			IdleTimeoutSeconds:      75,
			HeaderTimeoutSeconds:    15,
			DrainDeadlineSeconds:    15,
			PropagationWaitSeconds:  5,
			ShutdownDeadlineSeconds: 30,
		},
		Database: DatabaseConfig{
			URL:                    dbURL,
			MaxOpenConns:           25,
			MaxIdleConns:           5,
			ConnMaxLifetimeSeconds: 300,
		},
		SchemaRegistry: SchemaRegistryConfig{
			URL: schemaURL,
		},
		Broker: BrokerConfig{
			KafkaBrokers: brokers,
		},
		Telemetry: TelemetryConfig{
			OtlpEndpoint: otlpEndpoint,
			ServiceName:  "event-driven-platform",
		},
		Endpoints: EndpointsConfig{
			BasePath: "/api/v1",
		},
	}

	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func validateConfig(cfg *Config) error {
	if strings.TrimSpace(cfg.Server.Name) == "" {
		return fmt.Errorf("config validation error: server.name cannot be empty")
	}
	if strings.TrimSpace(cfg.Server.BindAddress) == "" {
		return fmt.Errorf("config validation error: server.bind_address cannot be empty")
	}
	if strings.TrimSpace(cfg.Server.AdminBindAddress) == "" {
		return fmt.Errorf("config validation error: server.admin_bind_address cannot be empty")
	}
	if cfg.Server.IdleTimeoutSeconds < 1 {
		return fmt.Errorf("config validation error: server.idle_timeout_seconds must be >= 1")
	}
	if cfg.Server.HeaderTimeoutSeconds < 1 {
		return fmt.Errorf("config validation error: server.header_timeout_seconds must be >= 1")
	}
	if cfg.Server.DrainDeadlineSeconds < 1 {
		return fmt.Errorf("config validation error: server.drain_deadline_seconds must be >= 1")
	}
	if cfg.Server.ShutdownDeadlineSeconds < 1 {
		return fmt.Errorf("config validation error: server.shutdown_deadline_seconds must be >= 1")
	}
	if strings.TrimSpace(cfg.Database.URL) == "" {
		return fmt.Errorf("config validation error: database.url cannot be empty")
	}
	return nil
}

func GetBrokerList(brokers string) []string {
	parts := strings.Split(brokers, ",")
	list := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			list = append(list, trimmed)
		}
	}
	return list
}

func ParseIntWithDefault(val string, def int) int {
	if val == "" {
		return def
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return def
	}
	return parsed
}

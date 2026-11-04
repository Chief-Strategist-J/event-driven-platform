package chassis

import (
	"fmt"
	"os"
	"regexp"

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
	OTLPEndpoint string `yaml:"otlp_endpoint"`
	ServiceName  string `yaml:"service_name"`
}

type EndpointsConfig struct {
	BasePath string `yaml:"base_path"`
}

type PlatformConfig struct {
	Server         ServerConfig         `yaml:"server"`
	Database       DatabaseConfig       `yaml:"database"`
	SchemaRegistry SchemaRegistryConfig `yaml:"schema_registry"`
	Broker         BrokerConfig         `yaml:"broker"`
	Telemetry      TelemetryConfig      `yaml:"telemetry"`
	Endpoints      EndpointsConfig      `yaml:"endpoints"`
}

var envPattern = regexp.MustCompile(`\$\{([A-Za-z0-9_]+)(?::-([^}]*))?\}`)

func expandEnv(content string) string {
	return envPattern.ReplaceAllStringFunc(content, func(match string) string {
		submatches := envPattern.FindStringSubmatch(match)
		if len(submatches) >= 2 {
			varName := submatches[1]
			val, exists := os.LookupEnv(varName)
			if exists && val != "" {
				return val
			}
			if len(submatches) >= 3 {
				return submatches[2]
			}
		}
		return match
	})
}

func LoadPlatformConfig(configPath string) (*PlatformConfig, error) {
	if configPath == "" {
		configPath = "config/default.yaml"
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return defaultPlatformConfig(), nil
		}
		return nil, fmt.Errorf("failed reading config file %s: %w", configPath, err)
	}

	expanded := expandEnv(string(data))

	var cfg PlatformConfig
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("failed parsing config yaml: %w", err)
	}

	applyEnvOverrides(&cfg)
	return &cfg, nil
}

func applyEnvOverrides(cfg *PlatformConfig) {
	if port := os.Getenv("PORT"); port != "" {
		cfg.Server.BindAddress = ":" + port
	}
	if adminPort := os.Getenv("ADMIN_PORT"); adminPort != "" {
		host := os.Getenv("ADMIN_HOST")
		if host == "" {
			host = "127.0.0.1"
		}
		cfg.Server.AdminBindAddress = fmt.Sprintf("%s:%s", host, adminPort)
	}
	if dbURL := os.Getenv("DATABASE_URL"); dbURL != "" {
		cfg.Database.URL = dbURL
	}
	if registryURL := os.Getenv("SCHEMA_REGISTRY_URL"); registryURL != "" {
		cfg.SchemaRegistry.URL = registryURL
	}
	if kafkaBrokers := os.Getenv("KAFKA_BROKERS"); kafkaBrokers != "" {
		cfg.Broker.KafkaBrokers = kafkaBrokers
	}
	if otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); otlpEndpoint != "" {
		cfg.Telemetry.OTLPEndpoint = otlpEndpoint
	}
	if serviceName := os.Getenv("OTEL_SERVICE_NAME"); serviceName != "" {
		cfg.Telemetry.ServiceName = serviceName
	}
}

func defaultPlatformConfig() *PlatformConfig {
	return &PlatformConfig{
		Server: ServerConfig{
			Name:                    "event-driven-platform",
			BindAddress:             ":8080",
			AdminBindAddress:        "127.0.0.1:8081",
			ShutdownDeadlineSeconds: 15,
		},
		Database: DatabaseConfig{
			URL: "postgres://postgres:postgres@127.0.0.1:5432/event_store?sslmode=disable",
		},
		SchemaRegistry: SchemaRegistryConfig{
			URL: "http://127.0.0.1:8082/apis/registry/v2",
		},
		Broker: BrokerConfig{
			KafkaBrokers: "127.0.0.1:9092",
		},
		Telemetry: TelemetryConfig{
			OTLPEndpoint: "127.0.0.1:4317",
			ServiceName:  "event-driven-platform",
		},
	}
}

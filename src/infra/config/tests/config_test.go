package tests

import (
	"os"
	"testing"

	"event-driven-platform/src/infra/config"
)

func TestLoadConfig_DefaultFallback(t *testing.T) {
	cfg, err := config.LoadConfig("non-existent-file.yaml")
	if err != nil {
		t.Fatalf("expected fallback config without error, got %v", err)
	}
	if cfg.Server.Name != "event-driven-platform" {
		t.Fatalf("expected server name event-driven-platform, got %s", cfg.Server.Name)
	}
	if cfg.Database.URL == "" {
		t.Fatalf("expected non-empty database URL")
	}
}

func TestLoadConfig_ParsesDefaultYaml(t *testing.T) {
	path := "../../../../config/default.yaml"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		path = "config/default.yaml"
	}
	cfg, err := config.LoadConfig(path)
	if err != nil {
		t.Fatalf("expected to parse default.yaml, got %v", err)
	}
	if cfg.Server.IdleTimeoutSeconds < 1 {
		t.Fatalf("expected idle timeout >= 1, got %d", cfg.Server.IdleTimeoutSeconds)
	}
}

func TestGetBrokerList(t *testing.T) {
	brokers := config.GetBrokerList("localhost:9092, localhost:9093 , ")
	if len(brokers) != 2 {
		t.Fatalf("expected 2 brokers, got %d", len(brokers))
	}
	if brokers[0] != "localhost:9092" || brokers[1] != "localhost:9093" {
		t.Fatalf("unexpected broker list: %v", brokers)
	}
}

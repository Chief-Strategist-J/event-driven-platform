package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestInfrastructureIntegration(t *testing.T) {
	ctx := context.Background()

	// 1. PostgreSQL container (Logical WAL level for Debezium)
	pgContainer, err := postgres.Run(ctx,
		"postgres:16-bookworm",
		postgres.WithDatabase("event_store"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("secret"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %s", err)
	}
	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate postgres container: %s", err)
		}
	}()

	// 2. Kafka KRaft container
	kafkaContainer, err := kafka.Run(ctx,
		"apache/kafka:3.8.0",
		kafka.WithClusterID("test-cluster-id"),
	)
	if err != nil {
		t.Fatalf("failed to start kafka container: %s", err)
	}
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate kafka container: %s", err)
		}
	}()

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil || len(brokers) == 0 {
		t.Fatalf("failed to get kafka brokers: %v", err)
	}

	t.Logf("PostgreSQL and Kafka containers started successfully. Brokers: %v", brokers)
}

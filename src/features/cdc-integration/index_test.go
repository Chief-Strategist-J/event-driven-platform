package cdcintegration

import (
	"testing"

	"event-driven-platform/src/features/cdc-integration/infrastructure/adapters"
)

func TestConnectSelection(t *testing.T) {
	t.Setenv("KAFKA_CONNECT_URL", "")
	if _, ok := newKafkaConnectFromEnv().(*adapters.MockKafkaConnectAdapter); !ok {
		t.Fatal("want mock")
	}
	t.Setenv("KAFKA_CONNECT_URL", "http://connect:8083")
	if _, ok := newKafkaConnectFromEnv().(*adapters.KafkaConnectRESTAdapter); !ok {
		t.Fatal("want rest")
	}
}

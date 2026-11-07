package kafka

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"event-driven-platform/messaging/engine/producers"
)

type KafkaProducerConfig struct {
	Brokers      []string
	BatchSize    int
	BatchTimeout time.Duration
	WriteTimeout time.Duration
	RequiredAcks int
	// AllowAutoTopicCreation asks the broker to create a missing topic on first publish.
	// The broker's own auto.create.topics.enable policy still decides; with it disabled the
	// publish fails with "Unknown Topic Or Partition" and the outbox row is parked after 5 retries.
	AllowAutoTopicCreation bool
}

func DefaultKafkaProducerConfig(brokers []string) KafkaProducerConfig {
	return KafkaProducerConfig{
		Brokers:      brokers,
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		RequiredAcks: -1,
		// Default on; set KAFKA_PRODUCER_AUTO_CREATE_TOPICS=false to disable.
		AllowAutoTopicCreation: os.Getenv("KAFKA_PRODUCER_AUTO_CREATE_TOPICS") != "false",
	}
}

type KafkaProducerAdapter struct {
	writer *kafka.Writer
}

func NewKafkaProducerAdapter(cfg KafkaProducerConfig) producers.ProducerPort {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Balancer:     &kafka.Hash{},
		MaxAttempts:  5,
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		WriteTimeout: cfg.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(cfg.RequiredAcks),
		Async:        false,

		AllowAutoTopicCreation: cfg.AllowAutoTopicCreation,
		Transport: &kafka.Transport{
			Dial: (&net.Dialer{
				Timeout:   3 * time.Second,
				DualStack: true,
			}).DialContext,
		},
	}

	return &KafkaProducerAdapter{writer: writer}
}

func (k *KafkaProducerAdapter) Publish(ctx context.Context, topic string, key []byte, payload []byte) error {
	return k.PublishWithHeaders(ctx, topic, key, payload, nil)
}

func (k *KafkaProducerAdapter) PublishWithHeaders(ctx context.Context, topic string, key []byte, payload []byte, headers map[string]string) error {
	msg := kafka.Message{
		Topic:   topic,
		Key:     key,
		Value:   payload,
		Time:    time.Now().UTC(),
		Headers: toKafkaHeaders(headers),
	}

	if err := k.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka publish to topic %s failed: %w", topic, err)
	}

	return nil
}

// toKafkaHeaders converts headers to Kafka record headers in sorted key order
// so output is deterministic.
func toKafkaHeaders(headers map[string]string) []kafka.Header {
	if len(headers) == 0 {
		return nil
	}
	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]kafka.Header, 0, len(keys))
	for _, k := range keys {
		out = append(out, kafka.Header{Key: k, Value: []byte(headers[k])})
	}
	return out
}

func (k *KafkaProducerAdapter) Close() error {
	if k.writer != nil {
		return k.writer.Close()
	}
	return nil
}

func ResolveKafkaBrokers(envBrokers string) []string {
	if envBrokers == "" {
		return []string{"localhost:9092"}
	}
	var brokers []string
	for _, b := range []string{envBrokers} {
		if host, port, err := net.SplitHostPort(b); err == nil {
			if _, pErr := strconv.Atoi(port); pErr == nil && host != "" {
				brokers = append(brokers, b)
			}
		}
	}
	if len(brokers) == 0 {
		return []string{envBrokers}
	}
	return brokers
}

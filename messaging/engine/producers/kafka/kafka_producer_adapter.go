package kafka

import (
	"context"
	"fmt"
	"net"
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
}

func DefaultKafkaProducerConfig(brokers []string) KafkaProducerConfig {
	return KafkaProducerConfig{
		Brokers:      brokers,
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		RequiredAcks: -1,
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
	msg := kafka.Message{
		Topic: topic,
		Key:   key,
		Value: payload,
		Time:  time.Now().UTC(),
	}

	if err := k.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka publish to topic %s failed: %w", topic, err)
	}

	return nil
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

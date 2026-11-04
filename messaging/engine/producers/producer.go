package producers

import "context"

type ProducerPort interface {
	Publish(ctx context.Context, topic string, key []byte, payload []byte) error
	Close() error
}

type PublishedMessage struct {
	Topic   string
	Key     []byte
	Payload []byte
}

type MemoryProducerAdapter struct {
	Messages []PublishedMessage
}

func NewMemoryProducerAdapter() *MemoryProducerAdapter {
	return &MemoryProducerAdapter{
		Messages: make([]PublishedMessage, 0),
	}
}

func (m *MemoryProducerAdapter) Publish(_ context.Context, topic string, key []byte, payload []byte) error {
	m.Messages = append(m.Messages, PublishedMessage{
		Topic:   topic,
		Key:     key,
		Payload: payload,
	})
	return nil
}

func (m *MemoryProducerAdapter) Close() error {
	return nil
}


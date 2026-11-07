package producers

import "context"

type ProducerPort interface {
	Publish(ctx context.Context, topic string, key []byte, payload []byte) error
	Close() error
}

// HeaderedProducerPort is an optional extension of ProducerPort for producers
// that can attach message headers. Implementations of ProducerPort that do not
// support headers keep working; use PublishRecord to dispatch.
type HeaderedProducerPort interface {
	ProducerPort
	PublishWithHeaders(ctx context.Context, topic string, key []byte, payload []byte, headers map[string]string) error
}

// PublishRecord publishes with headers when the producer supports them and
// falls back to plain Publish otherwise.
func PublishRecord(ctx context.Context, p ProducerPort, topic string, key []byte, payload []byte, headers map[string]string) error {
	if hp, ok := p.(HeaderedProducerPort); ok && len(headers) > 0 {
		return hp.PublishWithHeaders(ctx, topic, key, payload, headers)
	}
	return p.Publish(ctx, topic, key, payload)
}

type PublishedMessage struct {
	Topic   string
	Key     []byte
	Payload []byte
	Headers map[string]string
}

type MemoryProducerAdapter struct {
	Messages []PublishedMessage
}

func NewMemoryProducerAdapter() *MemoryProducerAdapter {
	return &MemoryProducerAdapter{
		Messages: make([]PublishedMessage, 0),
	}
}

func (m *MemoryProducerAdapter) Publish(ctx context.Context, topic string, key []byte, payload []byte) error {
	return m.PublishWithHeaders(ctx, topic, key, payload, nil)
}

func (m *MemoryProducerAdapter) PublishWithHeaders(_ context.Context, topic string, key []byte, payload []byte, headers map[string]string) error {
	m.Messages = append(m.Messages, PublishedMessage{
		Topic:   topic,
		Key:     key,
		Payload: payload,
		Headers: headers,
	})
	return nil
}

func (m *MemoryProducerAdapter) Close() error {
	return nil
}

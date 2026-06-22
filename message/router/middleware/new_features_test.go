package middleware

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestDeadLetterQueue_Middleware(t *testing.T) {
	pub := &mockPublisher{}
	dlq := NewDeadLetterQueue(pub, "dlq-topic", watermill.NopLogger{})

	handlerErr := assert.AnError

	handler := dlq.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		return nil, handlerErr
	})

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set(RetryCountKey, "3")

	produced, err := handler(msg)

	require.NoError(t, err)
	assert.Empty(t, produced)
	assert.Len(t, pub.publishedMessages, 1)
	assert.Equal(t, "dlq-topic", pub.lastTopic)
	assert.Equal(t, handlerErr.Error(), pub.publishedMessages[0].Metadata.Get(DLQReasonKey))
	assert.Equal(t, "3", pub.publishedMessages[0].Metadata.Get(DLQRetryCountKey))
}

func TestDeadLetterQueue_Middleware_NoError(t *testing.T) {
	pub := &mockPublisher{}
	dlq := NewDeadLetterQueue(pub, "dlq-topic", watermill.NopLogger{})

	expectedMsg := message.NewMessage(watermill.NewUUID(), []byte("produced"))
	handler := dlq.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		return []*message.Message{expectedMsg}, nil
	})

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))

	produced, err := handler(msg)

	require.NoError(t, err)
	assert.Len(t, produced, 1)
	assert.Equal(t, expectedMsg.UUID, produced[0].UUID)
	assert.Empty(t, pub.publishedMessages)
}

func TestRetryWithDLQ_Middleware(t *testing.T) {
	pub := &mockPublisher{}
	dlq := NewDeadLetterQueue(pub, "dlq-topic", watermill.NopLogger{})

	retryWithDLQ := RetryWithDLQ{
		Retry: Retry{
			MaxRetries:      1,
			InitialInterval: 1 * time.Millisecond,
			MaxInterval:     10 * time.Millisecond,
			Multiplier:      1,
			Logger:          watermill.NopLogger{},
		},
		DeadLetterQueue: dlq,
	}

	attempts := 0
	handler := retryWithDLQ.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		attempts++
		return nil, assert.AnError
	})

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))

	produced, err := handler(msg)

	require.NoError(t, err)
	assert.Empty(t, produced)
	assert.Equal(t, 2, attempts)
	assert.Len(t, pub.publishedMessages, 1)
}

func TestHeaderPropagation_Middleware(t *testing.T) {
	hp := NewHeaderPropagation([]string{"X-Trace-ID", "X-Request-ID"}, watermill.NopLogger{})

	handler := hp.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		produced := message.NewMessage(watermill.NewUUID(), []byte("produced"))
		produced.Metadata.Set("X-Other", "other-value")
		return []*message.Message{produced}, nil
	})

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("X-Trace-ID", "trace-123")
	msg.Metadata.Set("X-Request-ID", "req-456")
	msg.Metadata.Set("X-Not-Propagated", "should-not-appear")

	produced, err := handler(msg)

	require.NoError(t, err)
	require.Len(t, produced, 1)
	assert.Equal(t, "trace-123", produced[0].Metadata.Get("X-Trace-ID"))
	assert.Equal(t, "req-456", produced[0].Metadata.Get("X-Request-ID"))
	assert.Equal(t, "", produced[0].Metadata.Get("X-Not-Propagated"))
	assert.Equal(t, "other-value", produced[0].Metadata.Get("X-Other"))
}

func TestHeaderPropagation_OverrideExisting(t *testing.T) {
	hp := NewHeaderPropagation([]string{"X-Trace-ID"}, watermill.NopLogger{})

	handler := hp.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		produced := message.NewMessage(watermill.NewUUID(), []byte("produced"))
		produced.Metadata.Set("X-Trace-ID", "already-set")
		return []*message.Message{produced}, nil
	})

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("X-Trace-ID", "trace-123")

	produced, err := handler(msg)

	require.NoError(t, err)
	require.Len(t, produced, 1)
	assert.Equal(t, "already-set", produced[0].Metadata.Get("X-Trace-ID"))
}

func TestPropagateAllHeaders_Middleware(t *testing.T) {
	p := NewPropagateAllHeaders([]string{"X-Secret"}, watermill.NopLogger{})

	handler := p.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		produced := message.NewMessage(watermill.NewUUID(), []byte("produced"))
		return []*message.Message{produced}, nil
	})

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("X-Trace-ID", "trace-123")
	msg.Metadata.Set("X-Request-ID", "req-456")
	msg.Metadata.Set("X-Secret", "secret-value")

	produced, err := handler(msg)

	require.NoError(t, err)
	require.Len(t, produced, 1)
	assert.Equal(t, "trace-123", produced[0].Metadata.Get("X-Trace-ID"))
	assert.Equal(t, "req-456", produced[0].Metadata.Get("X-Request-ID"))
	assert.Equal(t, "", produced[0].Metadata.Get("X-Secret"))
}

type mockPublisher struct {
	publishedMessages []*message.Message
	lastTopic         string
}

func (m *mockPublisher) Publish(topic string, messages ...*message.Message) error {
	m.lastTopic = topic
	for _, msg := range messages {
		m.publishedMessages = append(m.publishedMessages, msg.Copy())
	}
	return nil
}

func (m *mockPublisher) Close() error {
	return nil
}

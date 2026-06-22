package message_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

func TestRouter_AddHandlerWithFallback(t *testing.T) {
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, watermill.NopLogger{})

	router, err := message.NewRouter(message.RouterConfig{}, watermill.NopLogger{})
	require.NoError(t, err)

	handlerCalled := false
	fallbackCalled := false
	handlerErr := assert.AnError

	producedPubSub := gochannel.NewGoChannel(gochannel.Config{}, watermill.NopLogger{})

	router.AddHandlerWithFallback(
		"test_handler",
		"input_topic",
		pubSub,
		"output_topic",
		producedPubSub,
		func(msg *message.Message) ([]*message.Message, error) {
			handlerCalled = true
			return nil, handlerErr
		},
		func(msg *message.Message) ([]*message.Message, error) {
			fallbackCalled = true
			produced := message.NewMessage(watermill.NewUUID(), []byte("fallback-result"))
			return []*message.Message{produced}, nil
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		_ = router.Run(ctx)
	}()

	<-router.Running()

	msg := message.NewMessage(watermill.NewUUID(), []byte("test-payload"))
	require.NoError(t, pubSub.Publish("input_topic", msg))

	time.Sleep(100 * time.Millisecond)

	assert.True(t, handlerCalled)
	assert.True(t, fallbackCalled)
}

func TestRouter_FallbackHandlerFailure(t *testing.T) {
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, watermill.NopLogger{})

	router, err := message.NewRouter(message.RouterConfig{}, watermill.NopLogger{})
	require.NoError(t, err)

	handlerCalled := false
	fallbackCalled := false

	router.AddHandlerWithFallback(
		"test_handler_fallback_fail",
		"input_topic_2",
		pubSub,
		"output_topic_2",
		pubSub,
		func(msg *message.Message) ([]*message.Message, error) {
			handlerCalled = true
			return nil, assert.AnError
		},
		func(msg *message.Message) ([]*message.Message, error) {
			fallbackCalled = true
			return nil, assert.AnError
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		_ = router.Run(ctx)
	}()

	<-router.Running()

	msg := message.NewMessage(watermill.NewUUID(), []byte("test-payload"))
	require.NoError(t, pubSub.Publish("input_topic_2", msg))

	time.Sleep(100 * time.Millisecond)

	assert.True(t, handlerCalled)
	assert.True(t, fallbackCalled)
}

func TestRouter_FallbackHandlerViaSetMethod(t *testing.T) {
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, watermill.NopLogger{})

	router, err := message.NewRouter(message.RouterConfig{}, watermill.NopLogger{})
	require.NoError(t, err)

	handlerCalled := false
	fallbackCalled := false

	h := router.AddConsumerHandler(
		"test_handler_set_fallback",
		"input_topic_3",
		pubSub,
		func(msg *message.Message) error {
			handlerCalled = true
			return assert.AnError
		},
	)

	h.SetFallbackConsumerHandler(func(msg *message.Message) error {
		fallbackCalled = true
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		_ = router.Run(ctx)
	}()

	<-router.Running()

	msg := message.NewMessage(watermill.NewUUID(), []byte("test-payload"))
	require.NoError(t, pubSub.Publish("input_topic_3", msg))

	time.Sleep(100 * time.Millisecond)

	assert.True(t, handlerCalled)
	assert.True(t, fallbackCalled)
}

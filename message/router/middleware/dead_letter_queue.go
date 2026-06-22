package middleware

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

const (
	DLQTopicKey           = "dlq_topic"
	DLQReasonKey          = "dlq_reason"
	DLQRetryCountKey      = "dlq_retry_count"
	DLQOriginalTopicKey   = "dlq_original_topic"
	DLQOriginalHandlerKey = "dlq_original_handler"
)

type DLQPublishFailedFunc func(msg *message.Message, handlerErr error, dlqPublishErr error) ([]*message.Message, error)

func LogOnDLQPublishFailed(msg *message.Message, handlerErr error, dlqPublishErr error) ([]*message.Message, error) {
	return nil, errors.Wrap(handlerErr, "DLQ publish also failed: "+dlqPublishErr.Error())
}

func NackOnDLQPublishFailed(msg *message.Message, handlerErr error, dlqPublishErr error) ([]*message.Message, error) {
	return nil, errors.Wrap(handlerErr, "DLQ publish also failed: "+dlqPublishErr.Error())
}

type DeadLetterQueue struct {
	publisher           message.Publisher
	topic               string
	logger              watermill.LoggerAdapter
	OnDLQPublishFailed  DLQPublishFailedFunc
}

func NewDeadLetterQueue(publisher message.Publisher, topic string, logger watermill.LoggerAdapter) *DeadLetterQueue {
	if logger == nil {
		logger = watermill.NopLogger{}
	}
	return &DeadLetterQueue{
		publisher:          publisher,
		topic:              topic,
		logger:             logger,
		OnDLQPublishFailed: LogOnDLQPublishFailed,
	}
}

func (dlq *DeadLetterQueue) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		producedMessages, err := h(msg)
		if err != nil {
			dlq.logger.Error("Handling failed, sending to DLQ", err, watermill.LogFields{
				"message_uuid": msg.UUID,
				"dlq_topic":    dlq.topic,
			})

			dlqMsg := msg.Copy()
			dlqMsg.Metadata.Set(DLQTopicKey, dlq.topic)
			dlqMsg.Metadata.Set(DLQReasonKey, err.Error())
			dlqMsg.Metadata.Set(DLQRetryCountKey, msg.Metadata.Get(RetryCountKey))
			dlqMsg.Metadata.Set(DLQOriginalTopicKey, message.SubscribeTopicFromCtx(msg.Context()))
			dlqMsg.Metadata.Set(DLQOriginalHandlerKey, message.HandlerNameFromCtx(msg.Context()))

			if publishErr := dlq.publisher.Publish(dlq.topic, dlqMsg); publishErr != nil {
				dlq.logger.Error("Failed to publish message to DLQ", publishErr, watermill.LogFields{
					"message_uuid": msg.UUID,
					"dlq_topic":    dlq.topic,
				})

				if dlq.OnDLQPublishFailed != nil {
					return dlq.OnDLQPublishFailed(msg, err, publishErr)
				}
				return producedMessages, errors.Wrap(err, "failed to send to DLQ: "+publishErr.Error())
			}

			return producedMessages, nil
		}

		return producedMessages, nil
	}
}

type RetryWithDLQ struct {
	Retry
	DeadLetterQueue *DeadLetterQueue
}

func (r RetryWithDLQ) Middleware(h message.HandlerFunc) message.HandlerFunc {
	retryMiddleware := r.Retry.Middleware

	if r.DeadLetterQueue != nil {
		dlqMiddleware := r.DeadLetterQueue.Middleware
		return func(msg *message.Message) ([]*message.Message, error) {
			return dlqMiddleware(retryMiddleware(h))(msg)
		}
	}

	return retryMiddleware(h)
}

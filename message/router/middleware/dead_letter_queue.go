package middleware

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

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
	DLQLocalFilePathKey   = "dlq_local_file_path"
)

type DLQPublishFailedFunc func(msg *message.Message, handlerErr error, dlqPublishErr error) ([]*message.Message, error)

func LogOnDLQPublishFailed(msg *message.Message, handlerErr error, dlqPublishErr error) ([]*message.Message, error) {
	return nil, errors.Wrap(handlerErr, "DLQ publish also failed: "+dlqPublishErr.Error())
}

func NackOnDLQPublishFailed(msg *message.Message, handlerErr error, dlqPublishErr error) ([]*message.Message, error) {
	return nil, errors.Wrap(handlerErr, "DLQ publish also failed: "+dlqPublishErr.Error())
}

func LocalFileOnDLQPublishFailed(dir string, logger watermill.LoggerAdapter) DLQPublishFailedFunc {
	if logger == nil {
		logger = watermill.NopLogger{}
	}
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "watermill-dlq-fallback")
	}

	return func(msg *message.Message, handlerErr error, dlqPublishErr error) ([]*message.Message, error) {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			logger.Error("Cannot create DLQ fallback directory", err, watermill.LogFields{"dir": dir})
			return nil, errors.Wrap(handlerErr, "DLQ publish and local dir creation both failed: "+dlqPublishErr.Error()+", "+err.Error())
		}

		record := struct {
			UUID            string            `json:"uuid"`
			Metadata        message.Metadata  `json:"metadata"`
			Payload         string            `json:"payload"`
			HandlerError    string            `json:"handler_error"`
			DLQPublishError string            `json:"dlq_publish_error"`
			SavedAt         time.Time         `json:"saved_at"`
		}{
			UUID:            msg.UUID,
			Metadata:        msg.Metadata,
			Payload:         string(msg.Payload),
			HandlerError:    handlerErr.Error(),
			DLQPublishError: dlqPublishErr.Error(),
			SavedAt:         time.Now().UTC(),
		}

		data, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			logger.Error("Cannot marshal DLQ fallback record", err, watermill.LogFields{"message_uuid": msg.UUID})
			return nil, errors.Wrap(handlerErr, "DLQ publish and JSON marshal both failed: "+dlqPublishErr.Error()+", "+err.Error())
		}

		fileName := msg.UUID + "-" + time.Now().UTC().Format("20060102-150405") + ".json"
		filePath := filepath.Join(dir, fileName)

		if err := os.WriteFile(filePath, data, 0o644); err != nil {
			logger.Error("Cannot write DLQ fallback file", err, watermill.LogFields{"path": filePath})
			return nil, errors.Wrap(handlerErr, "DLQ publish and file write both failed: "+dlqPublishErr.Error()+", "+err.Error())
		}

		logger.Info("DLQ publish failed, saved message to local file", watermill.LogFields{
			"message_uuid": msg.UUID,
			"file_path":    filePath,
		})

		localMsg := msg.Copy()
		localMsg.Metadata.Set(DLQLocalFilePathKey, filePath)
		return []*message.Message{localMsg}, nil
	}
}

type DeadLetterQueue struct {
	publisher          message.Publisher
	topic              string
	logger             watermill.LoggerAdapter
	OnDLQPublishFailed DLQPublishFailedFunc
	LocalFallbackDir   string
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
		LocalFallbackDir:   filepath.Join(os.TempDir(), "watermill-dlq-fallback"),
	}
}

func NewDeadLetterQueueWithLocalFallback(publisher message.Publisher, topic string, localFallbackDir string, logger watermill.LoggerAdapter) *DeadLetterQueue {
	dlq := NewDeadLetterQueue(publisher, topic, logger)
	dlq.LocalFallbackDir = localFallbackDir
	dlq.OnDLQPublishFailed = LocalFileOnDLQPublishFailed(localFallbackDir, logger)
	return dlq
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

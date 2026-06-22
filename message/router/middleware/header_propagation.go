package middleware

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type HeaderPropagation struct {
	headersToPropagate map[string]struct{}
	logger             watermill.LoggerAdapter
}

func NewHeaderPropagation(headers []string, logger watermill.LoggerAdapter) *HeaderPropagation {
	headerSet := make(map[string]struct{}, len(headers))
	for _, h := range headers {
		headerSet[h] = struct{}{}
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &HeaderPropagation{
		headersToPropagate: headerSet,
		logger:             logger,
	}
}

func (hp *HeaderPropagation) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		producedMessages, err := h(msg)
		if err != nil {
			return producedMessages, err
		}

		for _, producedMsg := range producedMessages {
			for header := range hp.headersToPropagate {
				if value := msg.Metadata.Get(header); value != "" {
					if producedMsg.Metadata.Get(header) == "" {
						producedMsg.Metadata.Set(header, value)
						hp.logger.Trace("Propagated header to produced message", watermill.LogFields{
							"header":           header,
							"value":            value,
							"source_message":   msg.UUID,
							"produced_message": producedMsg.UUID,
						})
					}
				}
			}
		}

		return producedMessages, nil
	}
}

func (hp *HeaderPropagation) AddHeader(header string) {
	hp.headersToPropagate[header] = struct{}{}
}

func (hp *HeaderPropagation) RemoveHeader(header string) {
	delete(hp.headersToPropagate, header)
}

type PropagateAllHeaders struct {
	except map[string]struct{}
	logger watermill.LoggerAdapter
}

func NewPropagateAllHeaders(except []string, logger watermill.LoggerAdapter) *PropagateAllHeaders {
	exceptSet := make(map[string]struct{}, len(except))
	for _, h := range except {
		exceptSet[h] = struct{}{}
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &PropagateAllHeaders{
		except: exceptSet,
		logger: logger,
	}
}

func (p *PropagateAllHeaders) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		producedMessages, err := h(msg)
		if err != nil {
			return producedMessages, err
		}

		for _, producedMsg := range producedMessages {
			for key, value := range msg.Metadata {
				if _, shouldExcept := p.except[key]; !shouldExcept {
					if producedMsg.Metadata.Get(key) == "" {
						producedMsg.Metadata.Set(key, value)
						p.logger.Trace("Propagated metadata to produced message", watermill.LogFields{
							"key":              key,
							"value":            value,
							"source_message":   msg.UUID,
							"produced_message": producedMsg.UUID,
						})
					}
				}
			}
		}

		return producedMessages, nil
	}
}

func PropagateHeadersMiddleware(headers ...string) message.HandlerMiddleware {
	hp := NewHeaderPropagation(headers, nil)
	return hp.Middleware
}

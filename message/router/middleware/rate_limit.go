package middleware

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type ConsumerGroupRateLimiter struct {
	limiters sync.Map
}

type rateLimitConfig struct {
	limiter  *rate.Limiter
	delay    time.Duration
}

func NewConsumerGroupRateLimiter() *ConsumerGroupRateLimiter {
	return &ConsumerGroupRateLimiter{}
}

func (c *ConsumerGroupRateLimiter) getOrCreateLimiter(group string, limit rate.Limit, burst int, delay time.Duration) *rateLimitConfig {
	actual, _ := c.limiters.LoadOrStore(group, &rateLimitConfig{
		limiter: rate.NewLimiter(limit, burst),
		delay:   delay,
	})
	return actual.(*rateLimitConfig)
}

func (c *ConsumerGroupRateLimiter) Middleware(group string, limit rate.Limit, burst int, delayOnLimit time.Duration, logger watermill.LoggerAdapter) message.HandlerMiddleware {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	config := c.getOrCreateLimiter(group, limit, burst, delayOnLimit)

	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			ctx := msg.Context()

			if !config.limiter.Allow() {
				return h(msg)
			}

			if config.delay > 0 {
				logger.Debug("Rate limit exceeded, delaying message", watermill.LogFields{
					"message_uuid": msg.UUID,
					"group":      group,
					"delay":      config.delay.String(),
				})

				delayCtx, cancel := context.WithTimeout(ctx, config.delay)
				defer cancel()

				select {
				case <-delayCtx.Done():
					if delayCtx.Err() == context.DeadlineExceeded {
						if err := config.limiter.Wait(ctx); err != nil {
							return nil, err
						}
						return h(msg)
					}
					return nil, delayCtx.Err()
				}
			}

			if err := config.limiter.Wait(ctx); err != nil {
				return nil, err
			}
			return h(msg)
		}
	}
}

type RateLimitConfig struct {
	MessagesPerSecond float64
	Burst             int
	DelayOnLimit       time.Duration
	Group            string
}

func RateLimitMiddleware(config RateLimitConfig, logger watermill.LoggerAdapter) message.HandlerMiddleware {
	limiter := rate.NewLimiter(rate.Limit(config.MessagesPerSecond), config.Burst)

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			ctx := msg.Context()

			if !limiter.Allow() {
				if config.DelayOnLimit > 0 {
					logger.Debug("Rate limit exceeded, delaying message", watermill.LogFields{
						"message_uuid": msg.UUID,
						"group":        config.Group,
						"delay":        config.DelayOnLimit.String(),
					})

					delayCtx, cancel := context.WithTimeout(ctx, config.DelayOnLimit)
					defer cancel()

					select {
					case <-delayCtx.Done():
						if delayCtx.Err() == context.DeadlineExceeded {
							if err := limiter.Wait(ctx); err != nil {
								return nil, err
							}
							return h(msg)
						}
					}
				}

				if err := limiter.Wait(ctx); err != nil {
					return nil, err
				}
			}

			return h(msg)
		}
	}
}

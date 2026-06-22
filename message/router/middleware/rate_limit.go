package middleware

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type SlidingWindowLimiter struct {
	windowSize time.Duration
	maxCount   int
	mu         sync.Mutex
	timestamps []time.Time
}

func NewSlidingWindowLimiter(windowSize time.Duration, maxCount int) *SlidingWindowLimiter {
	return &SlidingWindowLimiter{
		windowSize: windowSize,
		maxCount:   maxCount,
		timestamps: make([]time.Time, 0, maxCount),
	}
}

func (s *SlidingWindowLimiter) Allow() bool {
	return s.Reserve(time.Now())
}

func (s *SlidingWindowLimiter) Reserve(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := now.Add(-s.windowSize)
	i := 0
	for i < len(s.timestamps) && s.timestamps[i].Before(cutoff) {
		i++
	}
	if i > 0 {
		s.timestamps = s.timestamps[i:]
	}

	if len(s.timestamps) < s.maxCount {
		s.timestamps = append(s.timestamps, now)
		return true
	}
	return false
}

func (s *SlidingWindowLimiter) Wait(ctx context.Context) error {
	for {
		now := time.Now()
		s.mu.Lock()

		cutoff := now.Add(-s.windowSize)
		i := 0
		for i < len(s.timestamps) && s.timestamps[i].Before(cutoff) {
			i++
		}
		if i > 0 {
			s.timestamps = s.timestamps[i:]
		}

		if len(s.timestamps) < s.maxCount {
			s.timestamps = append(s.timestamps, now)
			s.mu.Unlock()
			return nil
		}

		oldest := s.timestamps[0]
		waitUntil := oldest.Add(s.windowSize)
		waitDur := waitUntil.Sub(now)
		s.mu.Unlock()

		if waitDur <= 0 {
			continue
		}

		timer := time.NewTimer(waitDur)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *SlidingWindowLimiter) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-s.windowSize)
	i := 0
	for i < len(s.timestamps) && s.timestamps[i].Before(cutoff) {
		i++
	}
	return len(s.timestamps) - i
}

type ConsumerGroupRateLimiter struct {
	limiters sync.Map
}

type rateLimitConfig struct {
	limiter *rate.Limiter
	sliding *SlidingWindowLimiter
	delay   time.Duration
	useSliding bool
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

func (c *ConsumerGroupRateLimiter) getOrCreateSlidingLimiter(group string, windowSize time.Duration, maxCount int, delay time.Duration) *rateLimitConfig {
	actual, _ := c.limiters.LoadOrStore(group, &rateLimitConfig{
		sliding:    NewSlidingWindowLimiter(windowSize, maxCount),
		delay:      delay,
		useSliding: true,
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

			if config.limiter.Allow() {
				return h(msg)
			}

			if config.delay > 0 {
				logger.Debug("Rate limit exceeded, delaying message", watermill.LogFields{
					"message_uuid": msg.UUID,
					"group":        group,
					"delay":        config.delay.String(),
				})

				delayCtx, cancel := context.WithTimeout(ctx, config.delay)
				defer cancel()

				<-delayCtx.Done()
				if delayCtx.Err() == context.DeadlineExceeded {
					if err := config.limiter.Wait(ctx); err != nil {
						return nil, err
					}
					return h(msg)
				}
				return nil, delayCtx.Err()
			}

			if err := config.limiter.Wait(ctx); err != nil {
				return nil, err
			}
			return h(msg)
		}
	}
}

func (c *ConsumerGroupRateLimiter) SlidingWindowMiddleware(group string, windowSize time.Duration, maxCount int, delayOnLimit time.Duration, logger watermill.LoggerAdapter) message.HandlerMiddleware {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	config := c.getOrCreateSlidingLimiter(group, windowSize, maxCount, delayOnLimit)

	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			ctx := msg.Context()

			if config.sliding.Allow() {
				return h(msg)
			}

			if config.delay > 0 {
				logger.Debug("Sliding window rate limit exceeded, delaying message", watermill.LogFields{
					"message_uuid": msg.UUID,
					"group":        group,
					"window_size":  windowSize.String(),
					"max_count":    maxCount,
					"delay":        config.delay.String(),
				})

				delayCtx, cancel := context.WithTimeout(ctx, config.delay)
				defer cancel()

				<-delayCtx.Done()
				if delayCtx.Err() == context.DeadlineExceeded {
					if err := config.sliding.Wait(ctx); err != nil {
						return nil, err
					}
					return h(msg)
				}
				return nil, delayCtx.Err()
			}

			if err := config.sliding.Wait(ctx); err != nil {
				return nil, err
			}
			return h(msg)
		}
	}
}

type RateLimitConfig struct {
	MessagesPerSecond float64
	Burst             int
	DelayOnLimit      time.Duration
	Group             string

	WindowSize      time.Duration
	MaxPerWindow    int
	UseSlidingWindow bool
}

func RateLimitMiddleware(config RateLimitConfig, logger watermill.LoggerAdapter) message.HandlerMiddleware {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	if config.UseSlidingWindow || config.WindowSize > 0 {
		windowSize := config.WindowSize
		if windowSize == 0 {
			windowSize = time.Second
		}
		maxCount := config.MaxPerWindow
		if maxCount <= 0 && config.MessagesPerSecond > 0 {
			scalar := float64(windowSize) / float64(time.Second)
			maxCount = int(config.MessagesPerSecond * scalar)
			if maxCount <= 0 {
				maxCount = 1
			}
		}
		if maxCount <= 0 {
			maxCount = 1
		}
		limiter := NewSlidingWindowLimiter(windowSize, maxCount)

		return func(h message.HandlerFunc) message.HandlerFunc {
			return func(msg *message.Message) ([]*message.Message, error) {
				ctx := msg.Context()

				if limiter.Allow() {
					return h(msg)
				}

				if config.DelayOnLimit > 0 {
					logger.Debug("Sliding window rate limit exceeded, delaying message", watermill.LogFields{
						"message_uuid": msg.UUID,
						"group":        config.Group,
						"window_size":  windowSize.String(),
						"max_count":    maxCount,
						"delay":        config.DelayOnLimit.String(),
					})

					delayCtx, cancel := context.WithTimeout(ctx, config.DelayOnLimit)
					defer cancel()

					<-delayCtx.Done()
					if delayCtx.Err() == context.DeadlineExceeded {
						if err := limiter.Wait(ctx); err != nil {
							return nil, err
						}
						return h(msg)
					}
					return nil, delayCtx.Err()
				}

				if err := limiter.Wait(ctx); err != nil {
					return nil, err
				}
				return h(msg)
			}
		}
	}

	burst := config.Burst
	if burst <= 0 {
		burst = 1
	}
	limiter := rate.NewLimiter(rate.Limit(config.MessagesPerSecond), burst)

	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			ctx := msg.Context()

			if limiter.Allow() {
				return h(msg)
			}

			if config.DelayOnLimit > 0 {
				logger.Debug("Rate limit exceeded, delaying message", watermill.LogFields{
					"message_uuid": msg.UUID,
					"group":        config.Group,
					"delay":        config.DelayOnLimit.String(),
				})

				delayCtx, cancel := context.WithTimeout(ctx, config.DelayOnLimit)
				defer cancel()

				<-delayCtx.Done()
				if delayCtx.Err() == context.DeadlineExceeded {
					if err := limiter.Wait(ctx); err != nil {
						return nil, err
					}
					return h(msg)
				}
				return nil, delayCtx.Err()
			}

			if err := limiter.Wait(ctx); err != nil {
				return nil, err
			}
			return h(msg)
		}
	}
}

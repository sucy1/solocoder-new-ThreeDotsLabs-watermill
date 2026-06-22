package natsjetstream

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type JetStreamConfig struct {
	Disabled       bool
	AutoProvision  bool
	ConnectOptions []nats.JSOpt
	SubscribeOptions []nats.SubOpt
	PublishOptions   []nats.PubOpt
	TrackMsgId       bool
	AckAsync         bool
	DurablePrefix    string
	DeliverSubjectPrefix string
}

type SubscriberConfig struct {
	URL            string
	CloseTimeout   time.Duration
	AckWaitTimeout time.Duration
	NatsOptions    []nats.Option
	Unmarshaler    Unmarshaler
	JetStream      JetStreamConfig
	QueueGroup     string
	RateLimit      uint64
}

type PublisherConfig struct {
	URL         string
	NatsOptions []nats.Option
	Marshaler   Marshaler
	JetStream   JetStreamConfig
}

func (c *SubscriberConfig) setDefaults() {
	if c.CloseTimeout == 0 {
		c.CloseTimeout = 30 * time.Second
	}
	if c.AckWaitTimeout == 0 {
		c.AckWaitTimeout = 30 * time.Second
	}
	if c.Unmarshaler == nil {
		c.Unmarshaler = &GobMarshaler{}
	}
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = &GobMarshaler{}
	}
}

func stableDeliverSubject(prefix, topic, queueGroup string) string {
	p := prefix
	if p == "" {
		p = "$JS.watermill.deliver"
	}
	if queueGroup != "" {
		return fmt.Sprintf("%s.%s.%s", p, sanitizeSubjectPart(queueGroup), sanitizeSubjectPart(topic))
	}
	return fmt.Sprintf("%s.%s", p, sanitizeSubjectPart(topic))
}

func sanitizeSubjectPart(s string) string {
	s = strings.ReplaceAll(s, ">", "_")
	s = strings.ReplaceAll(s, "*", "_")
	s = strings.ReplaceAll(s, " ", "_")
	return s
}

type Subscriber struct {
	config   SubscriberConfig
	logger   watermill.LoggerAdapter

	natsConn *nats.Conn
	js       nats.JetStreamContext

	subsWg   sync.WaitGroup
	closed   bool
	closedWg sync.WaitGroup
	closedMu sync.Mutex
	closing  chan struct{}

	subscribers     map[string]*subscriber
	subscribersLock sync.Mutex
}

type subscriber struct {
	ctx      context.Context
	cancel   context.CancelFunc
	natsSub  *nats.Subscription
	messages chan *message.Message
	closed   bool
	closedMu sync.Mutex
	key      string
}

func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	config.setDefaults()
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	conn, err := nats.Connect(config.URL, config.NatsOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to NATS")
	}

	var js nats.JetStreamContext
	if !config.JetStream.Disabled {
		js, err = conn.JetStream(config.JetStream.ConnectOptions...)
		if err != nil {
			return nil, errors.Wrap(err, "cannot create JetStream context")
		}
	}

	return &Subscriber{
		config:      config,
		logger:      logger,
		natsConn:    conn,
		js:          js,
		closing:     make(chan struct{}),
		subscribers: make(map[string]*subscriber),
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	subscriberKey := s.subscriberKey(topic)
	s.subscribersLock.Lock()
	if _, exists := s.subscribers[subscriberKey]; exists {
		s.subscribersLock.Unlock()
		return nil, errors.Errorf(
			"subscriber for topic %q (queue %q) already exists; cannot overwrite an active subscription",
			topic, s.config.QueueGroup,
		)
	}

	ctx, cancel := context.WithCancel(ctx)

	output := make(chan *message.Message)

	sub := &subscriber{
		ctx:      ctx,
		cancel:   cancel,
		messages: output,
		key:      subscriberKey,
	}

	s.subscribers[subscriberKey] = sub
	s.subscribersLock.Unlock()

	s.subsWg.Add(1)

	go func() {
		defer s.subsWg.Done()
		s.consumeMessages(ctx, topic, sub, output)

		s.subscribersLock.Lock()
		delete(s.subscribers, subscriberKey)
		s.subscribersLock.Unlock()
	}()

	return output, nil
}

func (s *Subscriber) subscriberKey(topic string) string {
	if s.config.QueueGroup != "" {
		return fmt.Sprintf("%s:%s", s.config.QueueGroup, topic)
	}
	return topic
}

func (s *Subscriber) consumeMessages(ctx context.Context, topic string, sub *subscriber, output chan *message.Message) {
	defer close(output)

	var natsSub *nats.Subscription
	var err error

	subscribeOpts := append([]nats.SubOpt{}, s.config.JetStream.SubscribeOptions...)

	deliverSubject := stableDeliverSubject(s.config.JetStream.DeliverSubjectPrefix, topic, s.config.QueueGroup)
	subscribeOpts = append(subscribeOpts, nats.DeliverSubject(deliverSubject))

	if s.config.QueueGroup != "" {
		if s.config.JetStream.DurablePrefix != "" {
			subscribeOpts = append(subscribeOpts, nats.Durable(s.config.JetStream.DurablePrefix+s.config.QueueGroup))
		}
		if s.config.RateLimit > 0 {
			subscribeOpts = append(subscribeOpts, nats.RateLimit(s.config.RateLimit))
		}
		natsSub, err = s.js.QueueSubscribe(topic, s.config.QueueGroup, func(msg *nats.Msg) {
			s.handleNatsMsg(ctx, msg, sub, output)
		}, subscribeOpts...)
	} else {
		if s.config.JetStream.DurablePrefix != "" {
			subscribeOpts = append(subscribeOpts, nats.Durable(s.config.JetStream.DurablePrefix+topic))
		}
		if s.config.RateLimit > 0 {
			subscribeOpts = append(subscribeOpts, nats.RateLimit(s.config.RateLimit))
		}
		natsSub, err = s.js.Subscribe(topic, func(msg *nats.Msg) {
			s.handleNatsMsg(ctx, msg, sub, output)
		}, subscribeOpts...)
	}

	if err != nil {
		s.logger.Error("Cannot subscribe to topic", err, watermill.LogFields{"topic": topic})
		return
	}

	sub.natsSub = natsSub

	<-ctx.Done()

	sub.closedMu.Lock()
	sub.closed = true
	sub.closedMu.Unlock()

	if natsSub != nil {
		if err := natsSub.Unsubscribe(); err != nil {
			s.logger.Error("Cannot unsubscribe", err, nil)
		}
	}
}

func (s *Subscriber) handleNatsMsg(ctx context.Context, natsMsg *nats.Msg, sub *subscriber, output chan *message.Message) {
	sub.closedMu.Lock()
	isClosed := sub.closed
	sub.closedMu.Unlock()

	if isClosed {
		return
	}

	msg, err := s.config.Unmarshaler.Unmarshal(natsMsg)
	if err != nil {
		s.logger.Error("Cannot unmarshal message", err, nil)
		return
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	select {
	case <-ctx.Done():
		return
	case output <- msg:
	}

	select {
	case <-ctx.Done():
		return
	case <-msg.Acked():
		if s.config.JetStream.AckAsync {
			if err := natsMsg.Ack(); err != nil {
				s.logger.Error("Cannot ack message", err, watermill.LogFields{"message_uuid": msg.UUID})
			}
		} else {
			if err := natsMsg.AckSync(); err != nil {
				s.logger.Error("Cannot ack message", err, watermill.LogFields{"message_uuid": msg.UUID})
			}
		}
	case <-msg.Nacked():
		if err := natsMsg.Nak(); err != nil {
			s.logger.Error("Cannot nack message", err, watermill.LogFields{"message_uuid": msg.UUID})
		}
	}
}

func (s *Subscriber) Close() error {
	s.closedMu.Lock()
	defer s.closedMu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	close(s.closing)

	s.subscribersLock.Lock()
	for key, sub := range s.subscribers {
		s.logger.Debug("Canceling subscriber", watermill.LogFields{"subscriber_key": key})
		if sub.cancel != nil {
			sub.cancel()
		}
	}
	s.subscribersLock.Unlock()

	waitGroupDone := make(chan struct{})
	go func() {
		s.subsWg.Wait()
		close(waitGroupDone)
	}()

	select {
	case <-waitGroupDone:
	case <-time.After(s.config.CloseTimeout):
		s.logger.Error("Subscriber close timeout", errors.New("timeout"), nil)
	}

	if s.natsConn != nil {
		s.natsConn.Close()
	}

	return nil
}

type Publisher struct {
	config PublisherConfig
	logger watermill.LoggerAdapter

	natsConn *nats.Conn
	js       nats.JetStreamContext

	closed   bool
	closedMu sync.Mutex
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	conn, err := nats.Connect(config.URL, config.NatsOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to NATS")
	}

	var js nats.JetStreamContext
	if !config.JetStream.Disabled {
		js, err = conn.JetStream(config.JetStream.ConnectOptions...)
		if err != nil {
			return nil, errors.Wrap(err, "cannot create JetStream context")
		}
	}

	return &Publisher{
		config:   config,
		logger:   logger,
		natsConn: conn,
		js:       js,
	}, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	for _, msg := range messages {
		natsMsg, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrap(err, "cannot marshal message")
		}

		publishOpts := append([]nats.PubOpt{}, p.config.JetStream.PublishOptions...)
		if p.config.JetStream.TrackMsgId && msg.UUID != "" {
			publishOpts = append(publishOpts, nats.MsgId(msg.UUID))
		}

		if _, err := p.js.PublishMsg(natsMsg, publishOpts...); err != nil {
			return errors.Wrap(err, fmt.Sprintf("cannot publish message to topic %s", topic))
		}
	}

	return nil
}

func (p *Publisher) Close() error {
	p.closedMu.Lock()
	defer p.closedMu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	if p.natsConn != nil {
		p.natsConn.Close()
	}

	return nil
}

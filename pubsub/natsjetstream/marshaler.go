package natsjetstream

import (
	"bytes"
	"encoding/gob"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*nats.Msg, error)
}

type Unmarshaler interface {
	Unmarshal(natsMsg *nats.Msg) (*message.Message, error)
}

type GobMarshaler struct{}

func (GobMarshaler) Marshal(topic string, msg *message.Message) (*nats.Msg, error) {
	if msg.UUID == "" {
		return nil, errors.New("message UUID is empty")
	}

	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)

	wrapper := messageWrapper{
		UUID:     msg.UUID,
		Metadata: msg.Metadata,
		Payload:  msg.Payload,
	}

	if err := encoder.Encode(wrapper); err != nil {
		return nil, errors.Wrap(err, "cannot encode message")
	}

	natsMsg := nats.NewMsg(topic)
	natsMsg.Data = buf.Bytes()
	natsMsg.Header = make(nats.Header)

	for k, v := range msg.Metadata {
		natsMsg.Header.Set(k, v)
	}

	return natsMsg, nil
}

func (GobMarshaler) Unmarshal(natsMsg *nats.Msg) (*message.Message, error) {
	buf := new(bytes.Buffer)
	buf.Write(natsMsg.Data)
	decoder := gob.NewDecoder(buf)

	var wrapper messageWrapper
	if err := decoder.Decode(&wrapper); err != nil {
		return nil, errors.Wrap(err, "cannot decode message")
	}

	msg := message.NewMessage(wrapper.UUID, wrapper.Payload)
	msg.Metadata = wrapper.Metadata

	for k, v := range natsMsg.Header {
		if len(v) > 0 {
			msg.Metadata.Set(k, v[0])
		}
	}

	return msg, nil
}

type messageWrapper struct {
	UUID     string
	Metadata message.Metadata
	Payload  message.Payload
}

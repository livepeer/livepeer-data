package event

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	streamAmqp "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type (
	BindingArgs struct {
		Key      string
		Exchange string
		Args     amqp.Table
	}

	StreamOptions struct {
		stream.StreamOptions
		Bindings []BindingArgs
	}

	ConsumeOptions struct {
		Stream string
		*StreamOptions
		*stream.ConsumerOptions
		MemorizeOffset bool
	}

	StreamMessage struct {
		stream.ConsumerContext
		*streamAmqp.Message
	}

	Handler interface {
		HandleMessage(msg StreamMessage)
	}

	StreamConsumer interface {
		ConsumeChan(ctx context.Context, opts ConsumeOptions) (<-chan StreamMessage, error)
		Consume(ctx context.Context, opts ConsumeOptions, handler Handler) error
		CheckConnection() error
		Close() error
	}

	Producer interface {
		Publish(ctx context.Context, key string, body interface{}, persistent bool) error
	}
)

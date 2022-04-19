package event

import (
	"context"
	"time"

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
		// Whether to memorize the message offset in the stream and use it on
		// re-connections to continue from the last read message.
		MemorizeOffset bool
		// ReconnectThreshold is the time that the client will wait without
		// receiving any messages before it restarts the stream consumer out of
		// caution. The default is 10 minutes.
		ReconnectThreshold time.Duration
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

	SimpleProducer interface {
		Publish(ctx context.Context, key string, body interface{}, persistent bool) error
	}

	AMQPProducer interface {
		Publish(ctx context.Context, msg AMQPMessage) error
		Shutdown(context.Context) error
	}

	AMQPConsumer interface {
		Consume(queue string, concurrency int, handler AMQPMessageHandler) error
		Shutdown(context.Context) error
	}

	AMQPClient interface {
		AMQPProducer
		AMQPConsumer
	}
)

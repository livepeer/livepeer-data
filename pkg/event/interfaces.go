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
		// Whether to memorize the message offset in the stream and use it on
		// re-connections to continue from the last read message.
		MemorizeOffset bool
	}

	StreamMessage struct {
		stream.ConsumerContext
		*streamAmqp.Message
	}

	AMQPMessage struct {
		// Exchange and Key of message in the AMQP protocol.
		Exchange, Key string
		// Body is the payload of the message.
		Body interface{}
		// Persistent means whether this message should be persisted in durable
		// storage not to be lost on broker restarts.
		Persistent bool
		// ResultChan receives the result message from the publish operation. Used
		// to guarantee delivery of messages to the broker through confirmation.
		ResultChan chan<- PublishResult
		// WaitResult simplifies waiting for the result of a publish operation. If
		// true, `Publish` will only return after confirmation has been received for
		// the specific message. Cannot be specified together with a `ResultChan`.
		WaitResult bool
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

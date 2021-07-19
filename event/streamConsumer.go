package event

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	streamAmqp "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type (
	BindingOptions struct {
		Key      string
		Exchange string
		Args     amqp.Table
	}

	StreamOptions struct {
		stream.StreamOptions
		*BindingOptions
	}

	ConsumeOptions struct {
		stream.ConsumerOptions
		*StreamOptions
	}

	StreamMessage struct {
		stream.ConsumerContext
		*streamAmqp.Message
	}

	StreamConsumer interface {
		Consume(ctx context.Context, streamName string, opts ConsumeOptions) (<-chan StreamMessage, error)
		Stop() error
	}

	strmConsumer struct {
		uri string
		env *stream.Environment
	}
)

func NewStreamConsumer(uri string) (StreamConsumer, error) {
	opts := stream.NewEnvironmentOptions().
		SetMaxConsumersPerClient(5).
		SetUri(uri)
	env, err := stream.NewEnvironment(opts)
	if err != nil {
		return nil, err
	}
	return &strmConsumer{uri, env}, nil
}

func (c *strmConsumer) Consume(ctx context.Context, streamName string, opts ConsumeOptions) (<-chan StreamMessage, error) {
	exists, err := c.env.StreamExists(streamName)
	if err != nil {
		return nil, err
	}
	if !exists {
		if opts.StreamOptions == nil {
			return nil, fmt.Errorf("stream not found: %s", streamName)
		}
		strmOpts := opts.StreamOptions
		err = c.env.DeclareStream(streamName, &strmOpts.StreamOptions)
		if err != nil {
			return nil, err
		}

		if strmOpts.BindingOptions != nil {
			err = bindQueue(c.uri, streamName, *strmOpts.BindingOptions)
			if err != nil {
				return nil, err
			}
		}
	}

	msgChan := make(chan StreamMessage, 100)
	handleMessages := func(consumerContext stream.ConsumerContext, message *streamAmqp.Message) {
		msgChan <- StreamMessage{consumerContext, message}
	}
	consumer, err := c.env.NewConsumer(streamName, handleMessages, &opts.ConsumerOptions)
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(msgChan)
		<-ctx.Done()
		consumer.Close()
	}()
	// TODO: implement recovery on that close notification
	go notifyConsumerClose(ctx, consumer.NotifyClose())

	return msgChan, nil
}

func (c *strmConsumer) Stop() error {
	return c.env.Close()
}

func notifyConsumerClose(ctx context.Context, channelClose stream.ChannelClose) {
	select {
	case event := <-channelClose:
		panic(fmt.Sprintf("Recovery not implemented. Consumer: %s closed on the stream: %s, reason: %s \n", event.Name, event.StreamName, event.Reason))
	case <-ctx.Done():
	}
}

func bindQueue(uri, queue string, opts BindingOptions) error {
	conn, err := amqp.Dial(uri)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer channel.Close()

	err = channel.QueueBind(queue, opts.Key, opts.Exchange, false, opts.Args)
	if err != nil {
		return fmt.Errorf("queue bind: %w", err)
	}
	return nil
}

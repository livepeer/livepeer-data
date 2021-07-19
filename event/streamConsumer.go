package event

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
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
		*stream.ConsumerOptions
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
		uri  string
		env  *stream.Environment
		done chan struct{}
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
	return &strmConsumer{uri, env, make(chan struct{})}, nil
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
	connect := func() (*stream.Consumer, error) {
		return c.env.NewConsumer(streamName, handleMessages, opts.ConsumerOptions)
	}

	ctx = whileAll(ctx.Done(), c.done)
	done, err := newReconnectingConsumer(ctx, streamName, connect)
	if err != nil {
		return nil, err
	}

	go func() {
		<-done
		close(msgChan)
	}()
	return msgChan, nil
}

func (c *strmConsumer) Stop() error {
	close(c.done)
	return c.env.Close()
}

func newReconnectingConsumer(ctx context.Context, streamName string, connect func() (*stream.Consumer, error)) (<-chan struct{}, error) {
	consumer, err := connect()
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				if err := consumer.Close(); err != nil {
					glog.Errorf("Error closing stream consumer. consumer=%q, stream=%q, err=%q", consumer.GetName(), streamName, err)
				}
				return
			case ev := <-consumer.NotifyClose():
				glog.Errorf("Stream consumer closed, reconnecting. consumer=%q, stream=%q, reason=%q", ev.Name, ev.StreamName, ev.Reason)
				consumer = ensureConnect(ctx, streamName, connect)
				if consumer == nil {
					return
				}
			}
		}
	}()
	return done, nil
}

func ensureConnect(ctx context.Context, streamName string, connect func() (*stream.Consumer, error)) *stream.Consumer {
	for {
		consumer, err := connect()
		if err == nil {
			return consumer
		}
		glog.Errorf("Stream consumer reconnection failure. stream=%q, error=%q", streamName, err)
		select {
		case <-time.After(3 * time.Second):
			glog.Infof("Retrying stream consumer reconnection. stream=%q", streamName)
		case <-ctx.Done():
			return nil
		}
	}
}

func whileAll(done1, done2 <-chan struct{}) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		select {
		case <-done1:
		case <-done2:
		}
	}()
	return ctx
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

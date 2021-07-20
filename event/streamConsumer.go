package event

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/golang/glog"
	amqp "github.com/rabbitmq/amqp091-go"
	streamAmqp "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

var ByteCapacity = stream.ByteCapacity{}
var OffsetSpec = stream.OffsetSpecification{}

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
		*stream.ConsumerOptions
		*StreamOptions
		MemorizeOffset bool
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
		streamUri, amqpUri string

		env  *stream.Environment
		done chan struct{}
	}
)

func NewStreamConsumer(streamUri, amqpUri string) (StreamConsumer, error) {
	var err error
	if amqpUri == "" {
		amqpUri, err = toAmqpScheme(streamUri)
		if err != nil {
			return nil, fmt.Errorf("error converting stream uri %q to amqp: %w", streamUri, err)
		}
	}
	opts := stream.NewEnvironmentOptions().
		SetMaxConsumersPerClient(5).
		SetUri(streamUri)
	env, err := stream.NewEnvironment(opts)
	if err != nil {
		return nil, err
	}
	return &strmConsumer{streamUri, amqpUri, env, make(chan struct{})}, nil
}

func (c *strmConsumer) Stop() error {
	close(c.done)
	return c.env.Close()
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
		err = c.createStream(streamName, *opts.StreamOptions)
		if err != nil {
			return nil, fmt.Errorf("error creating stream: %w", err)
		}
	}

	msgChan := make(chan StreamMessage, 100)
	handleMessages := func(consumerCtx stream.ConsumerContext, message *streamAmqp.Message) {
		msgChan <- StreamMessage{consumerCtx, message}
	}
	connect := func(prevConsumer stream.ConsumerContext) (*stream.Consumer, error) {
		connectOpts := *opts.ConsumerOptions
		if prevCons := prevConsumer.Consumer; prevCons != nil {
			if offset := prevCons.GetOffset(); opts.MemorizeOffset && offset > 0 {
				connectOpts.SetOffset(OffsetSpec.Offset(offset))
			}
		}
		return c.env.NewConsumer(streamName, handleMessages, &connectOpts)
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

func (c *strmConsumer) createStream(streamName string, opts StreamOptions) error {
	err := c.env.DeclareStream(streamName, &opts.StreamOptions)
	if err != nil {
		return err
	}
	if len(opts.Bindings) > 0 {
		err = bindQueue(c.amqpUri, streamName, opts.Bindings)
		if err != nil {
			// stream creation is not idempotent, so delete it when binding fails
			delErr := c.env.DeleteStream(streamName)
			if delErr != nil {
				err = fmt.Errorf("delete error: %q; after bind error: %w", delErr, err)
			}
			return err
		}
	}
	return nil
}

type connectFunc = func(prevConsumer stream.ConsumerContext) (*stream.Consumer, error)

func newReconnectingConsumer(ctx context.Context, streamName string, connect connectFunc) (<-chan struct{}, error) {
	consumer, err := connect(stream.ConsumerContext{})
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
				consumer = ensureConnect(ctx, streamName, connect, stream.ConsumerContext{Consumer: consumer})
				if consumer == nil {
					return
				}
			}
		}
	}()
	return done, nil
}

func ensureConnect(ctx context.Context, streamName string, connect connectFunc, prevConsumer stream.ConsumerContext) *stream.Consumer {
	for {
		consumer, err := connect(prevConsumer)
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

func bindQueue(uri, queue string, bindings []BindingArgs) error {
	conn, err := amqp.Dial(uri)
	if err != nil {
		return fmt.Errorf("dial %q: %w", uri, err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer channel.Close()

	for _, bind := range bindings {
		err = channel.QueueBind(queue, bind.Key, bind.Exchange, false, bind.Args)
		if err != nil {
			return fmt.Errorf("queue bind to %q at %q: %w", bind.Exchange, bind.Key, err)
		}
	}
	return nil
}

func toAmqpScheme(uri string) (string, error) {
	url, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	switch url.Scheme {
	case "rabbitmq-stream+tls":
		url.Scheme = "amqps"
	case "rabbitmq-stream":
		url.Scheme = "amqp"
	default:
		return "", fmt.Errorf("unknown scheme: %s", url.Scheme)
	}
	if port := url.Port(); port != "5552" {
		return "", fmt.Errorf("cannot convert non-default port: %s", port)
	}
	url.Host = url.Hostname() + ":5672"
	return url.String(), nil
}

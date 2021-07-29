package event

import (
	"context"
	"fmt"
	"net/url"
	"runtime/debug"
	"time"

	"github.com/golang/glog"
	amqp "github.com/rabbitmq/amqp091-go"
	streamAmqp "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type (
	strmConsumer struct {
		streamUri, amqpUri string

		env  *stream.Environment
		done chan struct{}
	}
)

func init() {
	if glog.V(10) {
		stream.SetLevelInfo(logs.DEBUG)
	}
}

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

func (c *strmConsumer) Close() error {
	close(c.done)
	return c.env.Close()
}

func (c *strmConsumer) ConsumeChan(ctx context.Context, opts ConsumeOptions) (<-chan StreamMessage, error) {
	exists, err := c.env.StreamExists(opts.Stream)
	if err != nil {
		return nil, err
	}
	if !exists {
		if opts.StreamOptions == nil {
			return nil, fmt.Errorf("stream not found: %s", opts.Stream)
		}
		err = c.createStream(opts.Stream, *opts.StreamOptions)
		if err != nil {
			return nil, fmt.Errorf("error creating stream: %w", err)
		}
	}

	msgChan := make(chan StreamMessage, 100)
	handleMessages := func(consumerCtx stream.ConsumerContext, message *streamAmqp.Message) {
		if glog.V(10) {
			cons := consumerCtx.Consumer
			glog.Infof("Read message from stream. consumer=%q, stream=%q, offset=%v, data=%q",
				cons.GetName(), cons.GetStreamName(), cons.GetOffset(), string(message.GetData()))
		}
		msgChan <- StreamMessage{consumerCtx, message}
	}
	connect := func(prevConsumer stream.ConsumerContext) (*stream.Consumer, error) {
		connectOpts := *opts.ConsumerOptions
		if prevCons := prevConsumer.Consumer; prevCons != nil {
			if offset := prevCons.GetOffset(); opts.MemorizeOffset && offset > 0 {
				connectOpts.SetOffset(OffsetSpec.Offset(offset))
			}
		}
		return c.env.NewConsumer(opts.Stream, handleMessages, &connectOpts)
	}

	ctx = whileAll(ctx.Done(), c.done)
	done, err := newReconnectingConsumer(ctx, opts.Stream, connect)
	if err != nil {
		return nil, err
	}

	go func() {
		<-done
		close(msgChan)
	}()
	return msgChan, nil
}

func (c *strmConsumer) Consume(ctx context.Context, opts ConsumeOptions, handler Handler) error {
	ctx, cancel := context.WithCancel(ctx)
	msgs, err := c.ConsumeChan(ctx, opts)
	if err != nil {
		cancel()
		return err
	}

	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				glog.Fatalf("Panic in stream message handler. panicValue=%v, stack=%q", rec, debug.Stack())
			}
		}()
		defer cancel()

		for msg := range msgs {
			handler.HandleMessage(msg)
		}
	}()
	return nil
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

type connectConsumerFunc = func(prevConsumer stream.ConsumerContext) (*stream.Consumer, error)

func newReconnectingConsumer(ctx context.Context, streamName string, connect connectConsumerFunc) (<-chan struct{}, error) {
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

func ensureConnect(ctx context.Context, streamName string, connect connectConsumerFunc, prevConsumer stream.ConsumerContext) *stream.Consumer {
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

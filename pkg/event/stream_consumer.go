package event

import (
	"context"
	"crypto/tls"
	"errors"
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
		streamUri, amqpUri *url.URL

		env  *stream.Environment
		done chan struct{}
	}
)

func init() {
	if glog.V(10) {
		stream.SetLevelInfo(logs.DEBUG)
	}
}

func NewStreamConsumer(streamUriStr, amqpUriStr string) (StreamConsumer, error) {
	streamUri, amqpUri, err := parseUris(streamUriStr, amqpUriStr)
	if err != nil {
		return nil, err
	}
	glog.Infof("Connecting to RabbitMQ. streamUri=%q, amqpUri=%q", streamUri.Redacted(), amqpUri.Redacted())
	opts := stream.NewEnvironmentOptions().
		SetMaxConsumersPerClient(5).
		SetUri(streamUri.String()).SetTLSConfig(
		&tls.Config{
			ServerName: streamUri.Host,
		})
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

func (c *strmConsumer) CheckConnection() error {
	// create separate env for test to avoid infinite connect retry from lib
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().SetUri(c.streamUri.String()))
	if err != nil {
		return err
	}
	_, err = env.StreamExists("__dummy__")
	if err != nil {
		env.Close()
		return err
	}
	return env.Close()
}

func (c *strmConsumer) ConsumeChan(ctx context.Context, opts ConsumeOptions) (<-chan StreamMessage, error) {
	err := c.ensureStream(opts.Stream, opts.StreamOptions)
	if err != nil {
		return nil, err
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

func (c *strmConsumer) ensureStream(streamName string, opts *StreamOptions) error {
	exists, err := c.env.StreamExists(streamName)
	if err != nil {
		return err
	}
	if !exists {
		if opts == nil {
			return fmt.Errorf("stream not found: %s", streamName)
		}
		err := c.env.DeclareStream(streamName, &opts.StreamOptions)
		if err != nil {
			return fmt.Errorf("error creating stream: %w", err)
		}
	}
	if opts != nil && len(opts.Bindings) > 0 {
		err = bindQueue(c.amqpUri, streamName, opts.Bindings)
		if err != nil {
			return fmt.Errorf("error binding stream: %w", err)
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

func bindQueue(uri *url.URL, queue string, bindings []BindingArgs) error {
	conn, err := amqp.Dial(uri.String())
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

var (
	protoAmqp, protoAmqps       = protocol{"amqp", "5672"}, protocol{"amqps", "5671"}
	protoStream, protoStreamTls = protocol{"rabbitmq-stream", "5552"}, protocol{"rabbitmq-stream+tls", "5551"}
	amqpDefaultUser             = url.UserPassword("guest", "guest")
)

func parseUris(streamUriStr, amqpUriStr string) (*url.URL, *url.URL, error) {
	if streamUriStr == "" && amqpUriStr == "" {
		return nil, nil, errors.New("must provide either stream or amqp uri")
	}
	var streamUri, amqpUri *url.URL
	var err error
	if streamUriStr != "" {
		streamUri, err = url.ParseRequestURI(streamUriStr)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing stream uri: %w", err)
		}
	} else if amqpUriStr != "" {
		amqpUri, err = url.ParseRequestURI(amqpUriStr)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing amqp uri: %w", err)
		}
	}

	streamFallback := withProtocol(amqpUri, protoStream, protoAmqps.scheme, protoStreamTls)
	amqpFallback := withProtocol(streamUri, protoAmqp, protoStreamTls.scheme, protoAmqps)
	return coalesceUri(streamUri, streamFallback), coalesceUri(amqpUri, amqpFallback), nil
}

func coalesceUri(value *url.URL, fallback url.URL) *url.URL {
	result := fallback
	if value != nil {
		result = *value
	}
	if result.Scheme == "" {
		result.Scheme = fallback.Scheme
	}
	if result.Port() == "" && fallback.Port() != "" {
		result.Host += ":" + fallback.Port()
	}
	if u := result.User; u == nil || u.String() == "" {
		result.User = fallback.User
		if u := result.User; u == nil || u.String() == "" {
			result.User = amqpDefaultUser
		}
	}
	return &result
}

type protocol struct{ scheme, port string }

func withProtocol(src *url.URL, defaultProto protocol, srcTlsScheme string, tlsProto protocol) url.URL {
	var result url.URL
	if src != nil {
		result = *src
	}
	proto := defaultProto
	if result.Scheme == srcTlsScheme {
		proto = tlsProto
	}
	result.Scheme = proto.scheme
	result.Host = result.Hostname() + ":" + proto.port
	return result
}

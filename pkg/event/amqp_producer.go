package event

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/golang/glog"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	PublishChannelSize   = 1024
	RetryMinDelay        = 5 * time.Second
	PublishLogSampleRate = 0.1
	MaxRetries           = 3
)

type AMQPProducer struct {
	ctx       context.Context
	amqpURI   string
	publishQ  chan *publishMessage
	connectFn AMQPConnectFunc
}

type AMQPChanPublisher interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

type AMQPConnectFunc func(ctx context.Context, uri string, confirms chan amqp.Confirmation, closed chan *amqp.Error) (AMQPChanPublisher, error)

func NewAMQPProducer(ctx context.Context, uri string, connectFn AMQPConnectFunc) (*AMQPProducer, error) {
	testCtx, cancel := context.WithCancel(ctx)
	_, err := connectFn(testCtx, uri, nil, nil)
	cancel()
	if err != nil {
		return nil, err
	}
	amqp := &AMQPProducer{
		ctx:       ctx,
		amqpURI:   uri,
		publishQ:  make(chan *publishMessage, PublishChannelSize),
		connectFn: connectFn,
	}
	go amqp.mainLoop()
	return amqp, nil
}

type AMQPMessage struct {
	Exchange, Key string
	Body          interface{}
	Persistent    bool
}

func (p *AMQPProducer) Publish(ctx context.Context, msg AMQPMessage) error {
	bodyRaw, err := json.Marshal(msg.Body)
	if err != nil {
		return fmt.Errorf("failed to marshal body to json: %w", err)
	}
	select {
	case p.publishQ <- p.newPublishMessage(msg, bodyRaw):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return fmt.Errorf("producer context done: %w", p.ctx.Err())
	}
}

type publishMessage struct {
	amqp.Publishing
	AMQPMessage
	// internal loop state
	retries int
}

func (p *AMQPProducer) newPublishMessage(msg AMQPMessage, bodyRaw []byte) *publishMessage {
	deliveryMode := amqp.Transient
	if msg.Persistent {
		deliveryMode = amqp.Persistent
	}
	return &publishMessage{
		AMQPMessage: msg,
		Publishing: amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            bodyRaw,
			DeliveryMode:    deliveryMode,
			Priority:        1,
		},
	}
}

func (p *AMQPProducer) mainLoop() {
	defer func() {
		if rec := recover(); rec != nil {
			glog.Fatalf("Panic in background AMQP publisher: value=%v", rec)
		}
	}()

	for {
		retryAfter := time.After(RetryMinDelay)
		err := p.connectAndLoopPublish()
		if p.ctx.Err() != nil {
			return
		}
		<-retryAfter
		glog.Errorf("Recovering AMQP connection: error=%q", err)
	}
}

func (p *AMQPProducer) connectAndLoopPublish() error {
	var (
		ctx, cancel = context.WithCancel(p.ctx)
		confirms    = make(chan amqp.Confirmation, PublishChannelSize)
		closed      = make(chan *amqp.Error, 1)
	)
	defer cancel()
	channel, err := p.connectFn(ctx, p.amqpURI, confirms, closed)
	if err != nil {
		return fmt.Errorf("error setting up AMQP connection: %w", err)
	}

	nextMsgTag := uint64(1)
	outstandingMsgs := map[uint64]*publishMessage{}
	defer func() {
		// we only return on connection errors, so retry all outstanding messages
		for _, msg := range outstandingMsgs {
			p.retryMsg(msg)
		}
	}()

	for {
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case err := <-closed:
			return fmt.Errorf("channel or connection closed: %w", err)
		case msg := <-p.publishQ:
			mandatory, immediate := false, false
			err := channel.Publish(msg.Exchange, msg.Key, mandatory, immediate, msg.Publishing)
			if err != nil {
				p.retryMsg(msg)
				glog.Errorf("Error publishing message: exchange=%q, key=%q, error=%q, body=%q", msg.Exchange, msg.Key, err, msg.Publishing.Body)
				return err
			}

			outstandingMsgs[nextMsgTag] = msg
			nextMsgTag++

			if glog.V(4) && rand.Float32() < PublishLogSampleRate {
				glog.Infof("Sampled: Message published: exchange=%q, key=%q, body=%q", msg.Exchange, msg.Key, msg.Publishing.Body)
			}
		case conf, ok := <-confirms:
			if !ok {
				return errors.New("channel or connection closed")
			}
			tag, success := conf.DeliveryTag, conf.Ack
			msg, ok := outstandingMsgs[tag]
			if !ok {
				glog.Errorf("Received confirmation for unknown message: tag=%v, success=%v", tag, success)
				break
			}
			delete(outstandingMsgs, tag)
			if !success {
				p.retryMsg(msg)
			}
		}
	}
}

func (p *AMQPProducer) retryMsg(msg *publishMessage) {
	msg.retries++
	if msg.retries >= MaxRetries {
		glog.Errorf("Dropping message reaching max retries: exchange=%q, key=%q, body=%q", msg.Exchange, msg.Key, msg.Publishing.Body)
		return
	}

	select {
	case p.publishQ <- msg:
	default:
		glog.Errorf("Failed to re-enqueue message: exchange=%q, key=%q, body=%q", msg.Exchange, msg.Key, msg.Publishing.Body)
	}
}

func NewAMQPConnectFunc(setup func(c *amqp.Channel) error) AMQPConnectFunc {
	return func(ctx context.Context, uri string, confirms chan amqp.Confirmation, closed chan *amqp.Error) (AMQPChanPublisher, error) {
		conn, err := amqp.Dial(uri)
		if err != nil {
			return nil, fmt.Errorf("dial: %w", err)
		}
		go func() {
			<-ctx.Done()
			conn.Close()
		}()

		channel, err := conn.Channel()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("open channel: %w", err)
		}
		if err := channel.Confirm(false); err != nil {
			conn.Close()
			return nil, fmt.Errorf("request confirms: %w", err)
		}
		if setup != nil {
			if err := setup(channel); err != nil {
				conn.Close()
				return nil, fmt.Errorf("channel setup: %w", err)
			}
		}
		if confirms != nil {
			channel.NotifyPublish(confirms)
		}
		if closed != nil {
			channel.NotifyClose(closed)
		}
		return channel, nil
	}
}

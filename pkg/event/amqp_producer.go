package event

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"runtime/debug"
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

var (
	ErrProducerShuttingDown = errors.New("amqp: producer shutting down")
	ErrProducerClosed       = errors.New("amqp: producer closed")
	ErrMaxRetriesReached    = errors.New("amqp: publish max retries reached")
	ErrRetryQueueFull       = errors.New("amqp: retry queue full")
)

type amqpProducer struct {
	amqpURI   string
	publishQ  chan *publishMessage
	connectFn AMQPConnectFunc

	shutdownStart chan struct{}
	shutdownCtx   context.Context
	shutdownDone  context.CancelFunc
}

func NewAMQPProducer(uri string, connectFn AMQPConnectFunc) (AMQPProducer, error) {
	testCtx, cancel := context.WithCancel(context.Background())
	_, err := connectFn(testCtx, uri, nil, nil)
	cancel()
	if err != nil {
		return nil, err
	}
	shutCtx, shutDone := context.WithCancel(context.Background())
	amqp := &amqpProducer{
		amqpURI:       uri,
		publishQ:      make(chan *publishMessage, PublishChannelSize),
		connectFn:     connectFn,
		shutdownStart: make(chan struct{}),
		shutdownCtx:   shutCtx,
		shutdownDone:  shutDone,
	}
	go amqp.mainLoop()
	return amqp, nil
}

// Shutdown will try to gracefully stop the background event publishing process,
// by waiting until all the publish buffer is flushed to the remote broker and
// all message confirmations have been received. This function must be called
// only once in a producer or it will panic.
//
// The Publish function must not be called concurrently with Shutdown or the
// events sent concurrently may be lost (concurrent Publish and Shutdown
// functions may succeed but the event never really gets sent).
func (p *amqpProducer) Shutdown(ctx context.Context) error {
	close(p.shutdownStart)

	select {
	case <-p.shutdownCtx.Done():
		return nil
	case <-ctx.Done():
		p.shutdownDone() // force background main loop to end
		return ctx.Err()
	}
}

type PublishResult struct {
	Message AMQPMessage
	Error   error
}

type AMQPMessage struct {
	Exchange, Key string
	Body          interface{}
	Persistent    bool
	ResultChan    chan<- PublishResult
}

func (p *amqpProducer) Publish(ctx context.Context, msg AMQPMessage) error {
	if p.isShutdownDone() {
		return ErrProducerClosed
	} else if p.isShuttingDown() {
		return ErrProducerShuttingDown
	}
	bodyRaw, err := json.Marshal(msg.Body)
	if err != nil {
		return fmt.Errorf("failed to marshal body to json: %w", err)
	}
	select {
	case p.publishQ <- p.newPublishMessage(msg, bodyRaw):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.shutdownStart:
		return ErrProducerShuttingDown
	}
}

type publishMessage struct {
	amqp.Publishing
	AMQPMessage
	// internal loop state
	retries int
}

func (m *publishMessage) sendResult(err error) {
	if m.ResultChan != nil {
		m.ResultChan <- PublishResult{m.AMQPMessage, err}
	}
}

func (p *amqpProducer) newPublishMessage(msg AMQPMessage, bodyRaw []byte) *publishMessage {
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

func (p *amqpProducer) mainLoop() {
	for {
		retryAfter := time.After(RetryMinDelay)
		err := p.connectAndLoopPublish()
		if err == nil {
			p.shutdownDone()
			return
		}
		<-retryAfter
		glog.Errorf("Recovering AMQP connection: error=%q", err)
	}
}

func (p *amqpProducer) connectAndLoopPublish() error {
	defer func() {
		if rec := recover(); rec != nil {
			glog.Errorf("Panic in background AMQP publisher: value=%q stack:\n%s", rec, string(debug.Stack()))
		}
	}()
	var (
		ctx, cancel = context.WithCancel(context.Background())
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

	shutdown, isShuttingDown := p.shutdownStart, false
	isFlushed := func() bool { return len(outstandingMsgs) == 0 && len(p.publishQ) == 0 }

	for {
		select {
		case err := <-closed:
			return fmt.Errorf("channel or connection closed: %w", err)
		case msg := <-p.publishQ:
			mandatory, immediate := false, false
			err := channel.Publish(msg.Exchange, msg.Key, mandatory, immediate, msg.Publishing)
			if err != nil {
				glog.Errorf("Error publishing message: exchange=%q, key=%q, error=%q, body=%q", msg.Exchange, msg.Key, err, msg.Publishing.Body)
				p.retryMsg(msg)
				if isShuttingDown && isFlushed() {
					glog.Warningf("Shutting down after dropping last pending message: exchange=%q, key=%q", msg.Exchange, msg.Key)
					return nil
				}
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
			if success {
				msg.sendResult(nil)
			} else {
				p.retryMsg(msg)
			}
			if isShuttingDown && isFlushed() {
				glog.Infof("Shutting down after last confirmation received: tag=%d, exchange=%q, key=%q", tag, msg.Exchange, msg.Key)
				return nil
			}
		case <-shutdown:
			if isFlushed() {
				glog.Infof("Shutting down immediately with no pending events")
				return nil
			}
			glog.Infof("Waiting for %d publishes and %d confirmations before shutdown", len(p.publishQ), len(outstandingMsgs))
			shutdown, isShuttingDown = nil, true
		case <-p.shutdownCtx.Done():
			glog.Warningf("Forcing shutdown with %d pending publishes and %d pending confirmations", len(p.publishQ), len(outstandingMsgs))
			for _, msg := range outstandingMsgs {
				msg.sendResult(ErrProducerShuttingDown)
			}
			return nil
		}
	}
}

func (p *amqpProducer) retryMsg(msg *publishMessage) {
	msg.retries++
	if msg.retries >= MaxRetries {
		glog.Errorf("Dropping message reaching max retries: exchange=%q, key=%q, body=%q", msg.Exchange, msg.Key, msg.Publishing.Body)
		msg.sendResult(ErrMaxRetriesReached)
		return
	}

	select {
	case p.publishQ <- msg:
	default:
		glog.Errorf("Failed to re-enqueue message: exchange=%q, key=%q, body=%q", msg.Exchange, msg.Key, msg.Publishing.Body)
		msg.sendResult(ErrRetryQueueFull)
	}
}

func (p *amqpProducer) isShuttingDown() bool {
	select {
	case <-p.shutdownStart:
		return true
	default:
		return false
	}
}

func (p *amqpProducer) isShutdownDone() bool {
	return p.shutdownCtx.Err() != nil
}

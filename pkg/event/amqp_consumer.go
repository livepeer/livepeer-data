package event

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/golang/glog"
	amqp "github.com/rabbitmq/amqp091-go"
)

var ErrConsumerClosed = errors.New("amqp: consumer closed")

// This is a special error wrappper to indicate to the consumer that a message
// should not be re-queued. This is useful for messages that are malformed or
// otherwise invalid and should not be retried. If a dead letter exchange is
// configured, the broker will route the message to it.
type unprocessableMessageErr struct{ error }

// If error is not nil, wraps it in an unprocessable message error so the
// consumer does not requeue the message.
func UnprocessableIfErr(err error) error {
	if err == nil {
		return nil
	}
	return unprocessableMessageErr{err}
}

func IsUnprocessableMessageErr(err error) bool {
	return errors.As(err, &unprocessableMessageErr{})
}

// AMQPMessageHandler is a function that will be called for each message received.
type AMQPMessageHandler func(amqp.Delivery) error

type subscription struct {
	queue       string
	handler     AMQPMessageHandler
	concurrency int
}

type amqpConsumer struct {
	amqpURI   string
	connectFn AMQPConnectFunc

	lock          sync.Mutex
	subscriptions []*subscription
	currChannel   AMQPChanOps
	currCtx       context.Context

	shutdownCtx    context.Context
	shutdown       context.CancelFunc
	consumersGroup sync.WaitGroup
}

func NewAMQPConsumer(uri string, connectFn AMQPConnectFunc) (AMQPConsumer, error) {
	shutCtx, shutdown := context.WithCancel(context.Background())
	amqp := &amqpConsumer{
		amqpURI:     uri,
		connectFn:   connectFn,
		shutdownCtx: shutCtx,
		shutdown:    shutdown,
	}
	if err := amqp.connect(); err != nil {
		return nil, err
	}
	go amqp.reconnectLoop()
	return amqp, nil
}

// Shutdown will try to gracefully stop the background event consuming process.
func (c *amqpConsumer) Shutdown(ctx context.Context) error {
	if c.shutdownCtx.Err() != nil {
		return ErrConsumerClosed
	}
	c.shutdown()
	select {
	case <-c.consumersDone():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *amqpConsumer) consumersDone() chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.consumersGroup.Wait()
	}()
	return done
}

// Consume starts consuming messages from the given queue and calls the provided
// function for each message. The function will be called concurrently at most
// the given number of times. Returning a `nil` error will automatically
// acknowledge the message, whilst an error will cause it to be re-queued
// (nacked).
//
// There is currently no way to cancel a consumption after it has started.
// Shutdown the whole consumer if you need that.
func (c *amqpConsumer) Consume(queue string, concurrency int, handler AMQPMessageHandler) error {
	if c.shutdownCtx.Err() != nil {
		return ErrConsumerClosed
	} else if concurrency < 1 {
		return fmt.Errorf("concurrency must be at least 1, got %d", concurrency)
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	sub := &subscription{queue, handler, concurrency}
	err := doConsume(c.currCtx, &c.consumersGroup, c.currChannel, sub)
	if err != nil {
		return err
	}
	c.subscriptions = append(c.subscriptions, sub)
	return nil
}

func (c *amqpConsumer) reconnectLoop() {
	// initial state is already connected
	for {
		<-time.After(RetryMinDelay)
		<-c.currCtx.Done()
		if c.shutdownCtx.Err() != nil {
			glog.Info("Finishing AMQP consumer reconnect loop due to shutdown")
			return
		}

		glog.Info("Recovering AMQP consumer connection")
		if err := c.connect(); err != nil {
			glog.Errorf("Error connecting AMQP consumer err=%q", err)
		}
	}
}

func (c *amqpConsumer) connect() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	var (
		ctx, cancel = context.WithCancel(context.Background())
		closed      = make(chan *amqp.Error, 1)
	)
	channel, err := c.connectFn(ctx, c.amqpURI, nil, closed)
	if err != nil {
		cancel()
		return err
	}
	for _, sub := range c.subscriptions {
		if err := doConsume(ctx, &c.consumersGroup, channel, sub); err != nil {
			cancel()
			return fmt.Errorf("error consuming queue %q: %v", sub.queue, err)
		}
	}
	go func() {
		defer cancel()
		select {
		case err := <-closed:
			glog.Infof("Channel or connection closed: %s", err)
		case <-c.shutdownCtx.Done():
			glog.Infof("Shutting down consumer as requested")
		}
	}()
	c.currCtx, c.currChannel = ctx, channel
	return nil
}

func doConsume(ctx context.Context, wg *sync.WaitGroup, amqpch AMQPChanOps, sub *subscription) error {
	// TODO: Create custom consumer names to be able to cancel them.
	subs, err := amqpch.Consume(sub.queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	wg.Add(sub.concurrency)
	for i := 0; i < sub.concurrency; i++ {
		go func() {
			defer wg.Done()
			defer func() {
				if rec := recover(); rec != nil {
					glog.Errorf("Panic in background AMQP consumer: value=%q stack:\n%s", rec, string(debug.Stack()))
				}
			}()
			defer drain(subs)

			for {
				select {
				case msg, ok := <-subs:
					if !ok {
						return
					}
					err := runHandlerRecovered(sub, msg)
					if err == nil {
						err = msg.Ack(false)
						if err == nil {
							continue
						}
						// the error likely means the msg was already requeued (e.g. conn
						// reset), but let it fallthrough below and try a nack just in case.
						err = fmt.Errorf("error acking message: %w", err)
					}
					glog.Errorf("Nacking message due to error exchange=%q queue=%q routingKey=%q err=%q", msg.Exchange, sub.queue, msg.RoutingKey, err)
					requeue := !IsUnprocessableMessageErr(err)
					if err := msg.Nack(false, requeue); err != nil {
						glog.Errorf("Error nacking message exchange=%q queue=%q routingKey=%q err=%q", msg.Exchange, sub.queue, msg.RoutingKey, err)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	return nil
}

func runHandlerRecovered(sub *subscription, msg amqp.Delivery) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			glog.Errorf("Panic in AMQP handler: value=%q stack:\n%s", rec, string(debug.Stack()))
			err = UnprocessableIfErr(fmt.Errorf("panic: %v", rec))
		}
	}()

	// Set acknowledger to nil to prevent handler from manually acking/nacking. We
	// have a copy of the msg so no need to undo the patch, but it's future-proof
	// to do so anyway in case amqp.Delivery type becomes a reference in the lib.
	prevAcker := msg.Acknowledger
	defer func() { msg.Acknowledger = prevAcker }()
	msg.Acknowledger = nil

	return sub.handler(msg)
}

func drain(ch <-chan amqp.Delivery) {
	for msg := range ch {
		msg.Nack(false, true)
	}
}

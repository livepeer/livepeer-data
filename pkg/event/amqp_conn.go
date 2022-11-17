package event

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AMQPChanOps interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
}

type AMQPChanSetup interface {
	ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeDelete(name string, ifUnused, noWait bool) error
	ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error)
	QueueInspect(name string) (amqp.Queue, error)
	QueueUnbind(name, key, exchange string, args amqp.Table) error
	Qos(prefetchCount, prefetchSize int, global bool)
}

type AMQPConnectFunc func(ctx context.Context, uri string, confirms chan amqp.Confirmation, closed chan *amqp.Error) (AMQPChanOps, error)

func NewAMQPConnectFunc(setup func(c AMQPChanSetup) error) AMQPConnectFunc {
	return func(ctx context.Context, uri string, confirms chan amqp.Confirmation, closed chan *amqp.Error) (AMQPChanOps, error) {
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

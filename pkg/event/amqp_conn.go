package event

import (
	"context"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
	amqp "github.com/rabbitmq/amqp091-go"
)

type AMQPChanOps interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Ack(uint64, bool) error
	Nack(uint64, bool, bool) error
}

type AMQPChanSetup interface {
	ExchangeBind(string, string, string, bool, amqp091.Table) error
	ExchangeDeclare(string, string, bool, bool, bool, bool, amqp091.Table) error
	ExchangeDeclarePassive(string, string, bool, bool, bool, bool, amqp091.Table) error
	ExchangeDelete(string, bool, bool) error
	ExchangeUnbind(string, string, string, bool, amqp091.Table) error
	QueueBind(string, string, string, bool, amqp091.Table) error
	QueueDeclare(string, bool, bool, bool, bool, amqp091.Table) (amqp091.Queue, error)
	QueueDeclarePassive(string, bool, bool, bool, bool, amqp091.Table) (amqp091.Queue, error)
	QueueDelete(string, bool, bool, bool) (int, error)
	QueueInspect(string) (amqp091.Queue, error)
	QueuePurge(string, bool) (int, error)
	QueueUnbind(string, string, string, amqp091.Table) error
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

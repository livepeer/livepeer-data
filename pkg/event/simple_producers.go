package event

import (
	"context"
	"errors"
	"fmt"
)

func NewAMQPExchangeProducer(ctx context.Context, uri, exchange, keyNs string) (SimpleProducer, error) {
	connectFunc := NewAMQPConnectFunc(func(channel AMQPChanSetup) error {
		err := channel.ExchangeDeclare(exchange, "topic", true, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("exchange declare: %w", err)
		}
		return nil
	})
	producer, err := NewAMQPProducer(uri, connectFunc)
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		producer.Shutdown(context.Background())
	}()
	return producerFunc(func(ctx context.Context, key string, body interface{}, persistent bool) error {
		if keyNs != "" {
			key = keyNs + "." + key
		}
		return producer.Publish(ctx, AMQPMessage{exchange, key, body, persistent, nil, true})
	}), nil
}

func NewAMQPQueueProducer(ctx context.Context, uri, queue string) (SimpleProducer, error) {
	connectFunc := NewAMQPConnectFunc(func(channel AMQPChanSetup) error {
		_, err := channel.QueueDeclare(queue, true, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("queue declare: %w", err)
		}
		return nil
	})
	producer, err := NewAMQPProducer(uri, connectFunc)
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		producer.Shutdown(context.Background())
	}()
	return producerFunc(func(ctx context.Context, key string, body interface{}, persistent bool) error {
		if key != "" {
			return errors.New("when sending directly to a queue, key must always be empty")
		}
		return producer.Publish(ctx, AMQPMessage{"", queue, body, persistent, nil, true})
	}), nil
}

type producerFunc func(ctx context.Context, key string, body interface{}, persistent bool) error

func (f producerFunc) Publish(ctx context.Context, key string, body interface{}, persistent bool) error {
	return f(ctx, key, body, persistent)
}

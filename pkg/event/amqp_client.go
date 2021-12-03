package event

import "context"

type amqpClient struct {
	AMQPConsumer
	AMQPProducer
}

func NewAMQPClient(uri string, connectFn AMQPConnectFunc) (AMQPClient, error) {
	consumer, err := NewAMQPConsumer(uri, connectFn)
	if err != nil {
		return nil, err
	}
	producer, err := NewAMQPProducer(uri, connectFn)
	if err != nil {
		consumer.Shutdown(context.Background())
		return nil, err
	}
	return &amqpClient{consumer, producer}, nil
}

func (c *amqpClient) Shutdown(ctx context.Context) error {
	if err := c.AMQPConsumer.Shutdown(ctx); err != nil {
		return err
	}
	return c.AMQPProducer.Shutdown(ctx)
}

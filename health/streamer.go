package health

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/healthy-streams/event"
)

var (
	bindingKey = "#.stream_health.transcode.#"

	StreamConsumer event.StreamConsumer
)

type StreamFlags struct {
	Stream, Exchange string
	ConsumerName     string

	event.RawStreamOptions
}

func Stream(ctx context.Context, healthcore *Core, flags StreamFlags) error {
	if StreamConsumer == nil {
		return errors.New("StreamConsumer must be initialized")
	}

	streamOpts, err := event.ParseStreamOptions(flags.RawStreamOptions)
	if err != nil {
		return fmt.Errorf("invalid stream max age: %w", err)
	}
	startTime := time.Now().Add(-maxStatsWindow)
	msgs, err := StreamConsumer.Consume(ctx, event.ConsumeOptions{
		Stream: flags.Stream,
		StreamOptions: &event.StreamOptions{
			StreamOptions: *streamOpts,
			Bindings: []event.BindingArgs{
				{Key: bindingKey, Exchange: flags.Exchange},
			},
		},
		ConsumerOptions: event.NewConsumerOptions(flags.ConsumerName, event.TimestampOffset(startTime)),
		MemorizeOffset:  true,
	})
	if err != nil {
		return err
	}

	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				glog.Fatalf("Panic in stream message handler. panicValue=%v", rec)
			}
		}()

		for msg := range msgs {
			if glog.V(10) {
				cons := msg.Consumer
				glog.Infof("Read message from stream. consumer=%q, stream=%q, offset=%v, data=%q",
					cons.GetName(), cons.GetStreamName(), cons.GetOffset(), string(msg.GetData()))
			}

			healthcore.HandleMessage(msg)
		}
	}()
	return nil
}

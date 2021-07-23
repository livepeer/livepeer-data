package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/healthy-streams/event"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

var (
	bindingKey = "#.stream_health.transcode.#"

	StreamConsumer event.StreamConsumer
)

type RawStreamOptions struct {
	Stream, Exchange string
	ConsumerName     string

	MaxLengthBytes      string
	MaxSegmentSizeBytes string
	MaxAge              string
}

func Stream(ctx context.Context, healthcore *Core, opts RawStreamOptions) error {
	if StreamConsumer == nil {
		return errors.New("StreamConsumer must be initialized")
	}

	maxAge, err := time.ParseDuration(opts.MaxAge)
	if err != nil {
		return fmt.Errorf("invalid stream max age: %w", err)
	}
	msgs, err := StreamConsumer.Consume(ctx, opts.Stream, event.ConsumeOptions{
		StreamOptions: &event.StreamOptions{
			StreamOptions: stream.StreamOptions{
				MaxLengthBytes:      event.ByteCapacity.From(opts.MaxLengthBytes),
				MaxSegmentSizeBytes: event.ByteCapacity.From(opts.MaxSegmentSizeBytes),
				MaxAge:              maxAge,
			},
			Bindings: []event.BindingArgs{
				{Key: bindingKey, Exchange: opts.Exchange},
			},
		},
		ConsumerOptions: stream.NewConsumerOptions().
			SetConsumerName(opts.ConsumerName).
			SetOffset(timeOffsetBy(maxStatsWindow)),
		MemorizeOffset: true,
	})
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			for _, data := range msg.Data {
				handleMessageData(data, msg.Consumer, healthcore)
			}
		}
	}()
	return nil
}

func handleMessageData(data []byte, consumer *stream.Consumer, healthcore *Core) {
	defer func() {
		if rec := recover(); rec != nil {
			glog.Fatalf("Panic in stream message handler. panicValue=%v", rec)
		}
	}()

	var evt *TranscodeEvent
	err := json.Unmarshal(data, &evt)
	if err != nil {
		glog.Errorf("Malformed event in stream. err=%q, data=%q", err, data)
		return
	}

	if glog.V(10) {
		glog.Infof("Read event from stream. consumer=%q, stream=%q, offset=%d, seqNo=%d, startTimeAge=%q, latency=%q, success=%v",
			consumer.GetName(), consumer.GetStreamName(), consumer.GetOffset(), evt.Segment.SeqNo,
			time.Since(time.Unix(0, evt.StartTime)), time.Duration(evt.LatencyMs)*time.Millisecond, evt.Success)
	}

	healthcore.HandleEvent(evt)
}

func timeOffsetBy(span time.Duration) stream.OffsetSpecification {
	return event.OffsetSpec.Timestamp(time.Now().Add(-span).UnixNano() / 1e6)
}

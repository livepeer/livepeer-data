package health

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/healthy-streams/event"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

var (
	streamOptions = stream.StreamOptions{
		MaxLengthBytes:      event.ByteCapacity.GB(1), // TODO prod: 90GB?
		MaxSegmentSizeBytes: event.ByteCapacity.KB(5), // TODO prod: 500MB
		MaxAge:              1 * time.Hour,            // TODO prod: 30 days?
	}
	bindingKey = "#.stream_health.transcode.#"

	StreamConsumer event.StreamConsumer
)

type StreamOptions struct {
	StreamName, ExchangeName string
	ConsumerName             string
}

func Stream(ctx context.Context, healthcore *Core, streamName, exchange, consumerName string) error {
	if StreamConsumer == nil {
		return errors.New("StreamConsumer must be initialized")
	}

	msgs, err := StreamConsumer.Consume(ctx, streamName, event.ConsumeOptions{
		StreamOptions: &event.StreamOptions{
			StreamOptions: streamOptions,
			Bindings: []event.BindingArgs{
				{Key: bindingKey, Exchange: exchange},
			},
		},
		ConsumerOptions: stream.NewConsumerOptions().
			SetConsumerName(consumerName).
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

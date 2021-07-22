package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"math/rand"
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/healthy-streams/api"
	"github.com/livepeer/healthy-streams/event"
	"github.com/livepeer/healthy-streams/health"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const streamUri = "rabbitmq-stream://guest:guest@localhost:5552/livepeer"
const exchange = "lp_golivepeer_metadata"
const binding = "#.stream_health.transcode.#"

var streamName = "sq_stream_health_v0"

var healthcore health.Core

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()
	glog.Info("Hello")

	// Set log level, not mandatory by default is INFO
	stream.SetLevelInfo(logs.DEBUG)

	glog.Info("Getting started with Streaming client for RabbitMQ")
	glog.Info("Connecting to RabbitMQ streaming...")

	// Connect to the broker ( or brokers )
	consumer, err := event.NewStreamConsumer(streamUri, "")
	CheckErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startOffset := time.Now().Add(-5*time.Minute).UnixNano() / 1e6 // start consuming from 5 mins ago
	msgs, err := consumer.Consume(ctx, streamName, event.ConsumeOptions{
		StreamOptions: &event.StreamOptions{
			StreamOptions: stream.StreamOptions{
				MaxLengthBytes:      event.ByteCapacity.GB(1),
				MaxSegmentSizeBytes: event.ByteCapacity.KB(5), // should be like 500MB in production
				MaxAge:              1 * time.Hour,
			},
			Bindings: []event.BindingArgs{
				{Key: binding, Exchange: exchange},
			},
		},
		ConsumerOptions: stream.NewConsumerOptions().
			SetConsumerName("my_consumer"). // set a consumer name
			SetOffset(event.OffsetSpec.Timestamp(startOffset)),
		MemorizeOffset: true,
	})
	CheckErr(err)

	go func() {
		for msg := range msgs {
			for _, data := range msg.Data {
				var evt *health.TranscodeEvent
				err := json.Unmarshal(data, &evt)
				CheckErr(err)

				if rand.Intn(100) < 100 {
					evt.Success = rand.Intn(100) < 98
				}

				if glog.V(100) {
					glog.Infof("received message. consumer=%q, offset=%d, seqNo=%d, startTimeAge=%q, latency=%q, success=%v",
						msg.Consumer.GetName(), msg.Consumer.GetOffset(), evt.Segment.SeqNo,
						time.Since(time.Unix(0, evt.StartTime)), time.Duration(evt.LatencyMs)*time.Millisecond, evt.Success)
				}

				healthcore.HandleEvent(evt)
			}
		}
	}()

	go func() {
		defer cancel()
		glog.Infoln("Stream name", streamName)
		glog.Infoln("Press any key to stop")
		bufio.NewReader(os.Stdin).ReadString('\n')
	}()

	err = api.ListenAndServe(ctx, ":8080", 1*time.Second, healthcore)
	CheckErr(err)
}

func CheckErr(err error) {
	if err != nil {
		glog.Fatalln("error", err)
	}
}

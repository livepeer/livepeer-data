package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/livepeer/healthy-streams/event"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const streamUri = "rabbitmq-stream://guest:guest@localhost:5552/livepeer"
const exchange = "lp_golivepeer_metadata"
const binding = "#.stream_health.transcode.#"

// var streamName = "sq_stream_health_" + time.Now().Format(time.RFC3339)
var streamName = "sq_stream_health_2021-07-15T17:43:23-03:00"

var byteCapacity = stream.ByteCapacity{}

func main() {
	log.Print("Hello")

	// Set log level, not mandatory by default is INFO
	stream.SetLevelInfo(logs.DEBUG)

	fmt.Println("Getting started with Streaming client for RabbitMQ")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	// Connect to the broker ( or brokers )
	consumer, err := event.NewStreamConsumer(streamUri)
	CheckErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	startOffset := time.Now().Add(-5*time.Minute).UnixNano() / 1e6 // start consuming from 5 mins ago
	msgs, err := consumer.Consume(ctx, streamName, event.ConsumeOptions{
		StreamOptions: &event.StreamOptions{
			StreamOptions: stream.StreamOptions{
				MaxLengthBytes:      byteCapacity.KB(10),
				MaxSegmentSizeBytes: byteCapacity.KB(1),
				MaxAge:              5 * time.Minute,
			},
			BindingOptions: &event.BindingOptions{binding, exchange, nil},
		},
		ConsumerOptions: stream.NewConsumerOptions().
			SetConsumerName("my_consumer"). // set a consumer name
			SetOffset(stream.OffsetSpecification{}.Timestamp(startOffset)),
	})
	CheckErr(err)

	go func() {
		defer cancel()
		fmt.Println("Stream name", streamName)
		fmt.Println("Press any key to stop ")
		bufio.NewReader(os.Stdin).ReadString('\n')
		time.Sleep(200 * time.Millisecond)
	}()

	for msg := range msgs {
		// json, err := json.Marshal(message.Properties)
		var v struct {
			Segment struct {
				SeqNo int
			}
			StartTime int64
			LatencyMs int64
		}
		err := json.Unmarshal(msg.Data[0], &v)
		CheckErr(err)
		log.Print("received message.",
			" consumer=", msg.Consumer.GetName(),
			" offset=", msg.Consumer.GetOffset(),
			" seqNo=", v.Segment.SeqNo,
			" starTimeAge=", time.Since(time.Unix(0, v.StartTime)),
			" latency=", time.Duration(v.LatencyMs)*time.Millisecond)
		// err := consumerContext.Consumer.StoreOffset()
		// CheckErr(err)
	}
}

func CheckErr(err error) {
	if err != nil {
		log.Fatalln("error", err)
	}
}

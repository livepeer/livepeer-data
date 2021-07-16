package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	ramqp "github.com/streadway/amqp"
)

const uri = "amqp://localhost:5672/livepeer"
const streamUri = "rabbitmq-stream://guest:guest@localhost:5552/livepeer"
const exchange = "lp_golivepeer_metadata"
const binding = "#.stream_health.transcode.#"

var queueName = "stream_health_" + time.Now().Format(time.RFC3339)

// var streamName = "sq_stream_health_" + time.Now().Format(time.RFC3339)

var streamName = "sq_stream_health_2021-07-15T17:43:23-03:00"

var byteCapacity = stream.ByteCapacity{}

func main() {
	log.Print("Hello")
	mainStream()

	// for {
	// 	err := consumeOnce()
	// 	time.Sleep(time.Second)
	// 	log.Error("Consume stopped, restarting", "err", err.Error())
	// }
}

// func consumeOnce() error {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	channel, err := amqpConnect(ctx, uri, queueName)
// 	if err != nil {
// 		return err
// 	}
// 	delivery, err := channel.Consume(queueName, "default", true, true, false, false, nil)
// 	if err != nil {
// 		return err
// 	}
// 	go func() {
// 		for msg := range delivery {
// 			log.Info("Received message", "id", msg.MessageId, "body", string(msg.Body))
// 		}
// 	}()
// 	closed := channel.NotifyClose(make(chan *ramqp.Error, 1))
// 	return <-closed
// }

// func amqpConnect(ctx context.Context, uri, queue string) (*ramqp.Channel, error) {
// 	conn, err := ramqp.Dial(uri)
// 	if err != nil {
// 		return nil, fmt.Errorf("dial: %w", err)
// 	}
// 	go func() {
// 		<-ctx.Done()
// 		conn.Close()
// 	}()

// 	channel, err := conn.Channel()
// 	if err != nil {
// 		conn.Close()
// 		return nil, fmt.Errorf("open channel: %w", err)
// 	}
// 	if err := channel.Confirm(false); err != nil {
// 		conn.Close()
// 		return nil, fmt.Errorf("request confirms: %w", err)
// 	}

// 	var (
// 		durable    = true
// 		autoDelete = false
// 		exclusive  = false
// 		noWait     = false
// 	)
// 	q, err := channel.QueueDeclare(queue, durable, autoDelete, exclusive, noWait, ramqp.Table{
// 		"x-expires": int((5 * time.Minute) / time.Millisecond),
// 	})
// 	if err != nil {
// 		conn.Close()
// 		return nil, fmt.Errorf("queue declare: %w", err)
// 	}
// 	log.Info("Declared queue", "name", q.Name, "consumers", q.Consumers, "messages", q.Messages)

// 	err = channel.QueueBind(queue, binding, exchange, false, nil)
// 	if err != nil {
// 		conn.Close()
// 		return nil, fmt.Errorf("queue bind: %w", err)
// 	}
// 	return channel, nil
// }

// func handlePublishConfirm(confirms stream.ChannelPublishConfirm) {
// 	go func() {
// 		for confirmed := range confirms {
// 			for _, msg := range confirmed {
// 				if msg.Confirmed {
// 					fmt.Printf("message %s stored \n  ", msg.Message.GetData())
// 				} else {
// 					fmt.Printf("message %s failed \n  ", msg.Message.GetData())
// 				}

// 			}
// 		}
// 	}()
// }

func CheckErr(err error) {
	if err != nil {
		log.Fatalln("error", err)
	}
}

func notifyConsumerClose(channelClose stream.ChannelClose) {
	event := <-channelClose
	fmt.Printf("Consumer: %s closed on the stream: %s, reason: %s \n", event.Name, event.StreamName, event.Reason)
}

func mainStream() {
	reader := bufio.NewReader(os.Stdin)
	// Set log level, not mandatory by default is INFO
	stream.SetLevelInfo(logs.DEBUG)

	fmt.Println("Getting started with Streaming client for RabbitMQ")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	// Connect to the broker ( or brokers )
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().SetUri(streamUri))
	CheckErr(err)

	// Create a stream, you can create streams without any option like:
	// err = env.DeclareStream(streamName, nil)
	// it is a best practise to define a size,  1GB for example:
	ok, err := env.StreamExists(streamName)
	CheckErr(err)
	if !ok {
		err = env.DeclareStream(streamName,
			&stream.StreamOptions{
				MaxLengthBytes:      byteCapacity.KB(10),
				MaxSegmentSizeBytes: byteCapacity.KB(1),
				MaxAge:              5 * time.Minute,
			},
		)
		CheckErr(err)
	}

	err = bindQueue(uri, streamName, binding, exchange)
	CheckErr(err)

	// go func() {
	// 	// Get a new producer for a stream
	// 	producer, err := env.NewProducer(streamName, nil)
	// 	CheckErr(err)

	// 	//optional publish confirmation channel
	// 	chPublishConfirm := producer.NotifyPublishConfirmation()
	// 	handlePublishConfirm(chPublishConfirm)

	// 	// the send method automatically aggregates the messages
	// 	// based on batch size
	// 	for i := 0; i < 1000; i++ {
	// 		time.Sleep(1 * time.Second)
	// 		err := producer.Send(amqp.NewMessage([]byte("hello_world_" + strconv.Itoa(i))))
	// 		CheckErr(err)
	// 	}

	// 	err = producer.Close()
	// 	CheckErr(err)
	// }()

	// Define a consumer per stream, there are different offset options to define a consumer, default is
	//env.NewConsumer(streamName, func(Context streaming.ConsumerContext, message *amqp.Message) {
	//
	//}, nil)
	// if you need to track the offset you need a consumer name like:
	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		// json, err := json.Marshal(message.Properties)
		var v struct {
			Segment struct {
				SeqNo int
			}
			StartTime int64
			LatencyMs int64
		}
		err := json.Unmarshal(message.Data[0], &v)
		CheckErr(err)
		log.Print("received message.",
			" consumer=", consumerContext.Consumer.GetName(),
			" offset=", consumerContext.Consumer.GetOffset(),
			" seqNo=", v.Segment.SeqNo,
			" starTimeAge=", time.Since(time.Unix(0, v.StartTime)),
			" latency=", time.Duration(v.LatencyMs)*time.Millisecond)
		// err := consumerContext.Consumer.StoreOffset()
		// CheckErr(err)
	}

	startOffset := time.Now().Add(-5*time.Minute).UnixNano() / 1e6 // start consuming from 5 mins ago
	consumer, err := env.NewConsumer(
		streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetConsumerName("my_consumer"). // set a consumer name
			SetOffset(stream.OffsetSpecification{}.Timestamp(startOffset)))
	CheckErr(err)

	consumer.QueryOffset()

	// channelClose receives all the closing events, here you can handle the
	// client reconnection or just log
	go notifyConsumerClose(consumer.NotifyClose())

	fmt.Println("Stream name", streamName)
	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	err = consumer.Close()
	time.Sleep(200 * time.Millisecond)
	CheckErr(err)
	err = env.DeleteStream(streamName)
	CheckErr(err)
	err = env.Close()
	CheckErr(err)
}

func bindQueue(uri, queue, binding, exchange string) error {
	conn, err := ramqp.Dial(uri)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer channel.Close()

	err = channel.QueueBind(queue, binding, exchange, false, nil)
	if err != nil {
		return fmt.Errorf("queue bind: %w", err)
	}
	return nil
}

package main

import (
	"bufio"
	"context"
	"flag"
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

var streamName = "lp_stream_health_v" + time.Now().String()

var healthcore = &health.Core{}

func main() {
	stream.SetLevelInfo(logs.DEBUG)
	flag.Set("logtostderr", "true")
	flag.Parse()

	glog.Info("Stream health care system starting up...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer, err := event.NewStreamConsumer(streamUri, "")
	if err != nil {
		glog.Fatalf("Error creating stream consumer. err=%q", err)
	}
	health.StreamConsumer = consumer

	host, _ := os.Hostname()
	err = health.Stream(ctx, healthcore, streamName, exchange, "healthy-streams-"+host)
	if err != nil {
		glog.Fatalf("Error starting health stream. err=%q", err)
	}

	go func() {
		defer cancel()
		glog.Infoln("Stream name", streamName)
		glog.Infoln("Press any key to stop")
		bufio.NewReader(os.Stdin).ReadString('\n')
	}()

	err = api.ListenAndServe(ctx, ":8080", 1*time.Second, healthcore)
	if err != nil {
		glog.Fatalf("Error starting api server. err=%q", err)
	}
}

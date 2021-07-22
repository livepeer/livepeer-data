package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
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

var streamName = "lp_stream_health_v" + time.Now().UTC().Format(time.RFC3339)

var healthcore = &health.Core{}

func main() {
	stream.SetLevelInfo(logs.DEBUG)
	flag.Set("logtostderr", "true")
	flag.Parse()

	glog.Info("Stream health care system starting up...")
	ctx := contextUntilSignal(context.Background(), syscall.SIGINT, syscall.SIGTERM)

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

	err = api.ListenAndServe(ctx, ":8080", 1*time.Second, healthcore)
	if err != nil {
		glog.Fatalf("Error starting api server. err=%q", err)
	}
}

func contextUntilSignal(parent context.Context, sigs ...os.Signal) context.Context {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		defer cancel()
		waitSignal(sigs...)
	}()
	return ctx
}

func waitSignal(sigs ...os.Signal) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, sigs...)

	signal := <-sigc
	switch signal {
	case syscall.SIGINT:
		glog.Infof("Got Ctrl-C, shutting down")
	case syscall.SIGTERM:
		glog.Infof("Got SIGTERM, shutting down")
	default:
		glog.Infof("Got signal %d, shutting down", signal)
	}
}

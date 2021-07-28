package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/api"
	"github.com/livepeer/livepeer-data/event"
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/health/reducers"
	"github.com/peterbourgon/ff"
)

var (
	// Version content of this constant will be set at build time,
	// using -ldflags, using output of the `git describe` command.
	Version = "undefined"

	// CLI flags
	fs = flag.NewFlagSet("healthanalyzer", flag.ExitOnError)

	host = fs.String("host", "localhost", "Hostname to bind to")
	port = fs.Uint("port", 8080, "Port to listen on")

	rabbitmqStreamUri = fs.String("rabbitmqStreamUri", "rabbitmq-stream://guest:guest@localhost:5552/livepeer", "Rabbitmq-stream URI to consume from")
	amqpUri           = fs.String("amqpUri", "", "Explicit AMQP URI in case of non-default protocols/ports (optional). Must point to the same cluster as rabbitmqStreamUri")
	streamingOpts     = health.StreamingOptions{} // flags bound in init below
)

func init() {
	// Streaming options
	fs.StringVar(&streamingOpts.Stream, "streamName", "lp_stream_health_v0", "Name of RabbitMQ stream to create and consume from")
	fs.StringVar(&streamingOpts.Exchange, "exchange", "lp_golivepeer_metadata", "Name of RabbitMQ exchange to bind the stream to on creation")
	fs.StringVar(&streamingOpts.ConsumerName, "consumerName", "", `Consumer name to use when consuming stream (default "healthanalyzer-${hostname}")`)
	fs.StringVar(&streamingOpts.MaxLengthBytes, "streamMaxLength", "50gb", "When creating a new stream, config for max total storage size")
	fs.StringVar(&streamingOpts.MaxSegmentSizeBytes, "streamMaxSegmentSize", "500mb", "When creating a new stream, config for max stream segment size in storage")
	fs.DurationVar(&streamingOpts.MaxAge, "streamMaxAge", 30*24*time.Hour, `When creating a new stream, config for max age of stored events`)

	flag.Set("logtostderr", "true")
	glogVFlag := flag.Lookup("v")
	verbosity := fs.Int("v", 0, "Log verbosity {0-10}")

	fs.String("config", "", "config file (optional)")
	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("LP"),
	)
	flag.CommandLine.Parse(nil)
	glogVFlag.Value.Set(strconv.Itoa(*verbosity))

	if streamingOpts.ConsumerName == "" {
		streamingOpts.ConsumerName = "healthanalyzer-" + hostname()
	}
}

func main() {
	glog.Infof("Stream health care system starting up... version=%q", Version)
	ctx := contextUntilSignal(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	consumer, err := event.NewStreamConsumer(*rabbitmqStreamUri, *amqpUri)
	if err != nil {
		glog.Fatalf("Error creating stream consumer. err=%q", err)
	}
	defer consumer.Stop()

	reducers, startTimeOffset := reducers.DefaultPipeline()
	healthcore := health.NewCore(health.CoreOptions{
		Streaming:       streamingOpts,
		StartTimeOffset: startTimeOffset,
	}, consumer)
	err = healthcore.Use(reducers...).Start(ctx)
	if err != nil {
		glog.Fatalf("Error starting health core. err=%q", err)
	}

	glog.Info("Starting server...")
	err = api.ListenAndServe(ctx, *host, *port, 1*time.Second, healthcore)
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
	defer signal.Stop(sigc)

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

func hostname() string {
	host, err := os.Hostname()
	if err != nil {
		glog.Fatalf("Failed to read hostname. err=%q", err)
	}
	return host
}

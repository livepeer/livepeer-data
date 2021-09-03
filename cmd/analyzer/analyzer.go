package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/api"
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/health/reducers"
	"github.com/livepeer/livepeer-data/pkg/event"
	"github.com/peterbourgon/ff"
)

var (
	// Version content of this constant will be set at build time,
	// using -ldflags, using output of the `git describe` command.
	Version = "undefined"

	// CLI flags, bound in init below
	rabbitmqUri string
	amqpUri     string

	golivepeerExchange string
	shardPrefixesFlag  string
	shardPrefixes      []string

	serverOpts       api.ServerOptions
	streamingOpts    health.StreamingOptions
	memoryRecordsTtl time.Duration
)

func init() {
	fs := flag.NewFlagSet("analyzer", flag.ExitOnError)

	fs.StringVar(&rabbitmqUri, "rabbitmq-uri", "amqp://guest:guest@localhost:5672/livepeer", "URI for RabbitMQ server to consume from. Can be specified as a default AMQP URI which will be converted to stream protocol.")
	fs.StringVar(&amqpUri, "amqp-uri", "", "Explicit AMQP URI in case of non-default protocols/ports (optional). Must point to the same cluster as rabbitmqUri")

	fs.StringVar(&golivepeerExchange, "golivepeer-exchange", "lp_golivepeer_metadata", "Name of RabbitMQ exchange to bind the stream to on creation")
	fs.StringVar(&shardPrefixesFlag, "shard-prefixes", "", "Comma-separated list of prefixes of manifest IDs to process events from")

	// Server options
	fs.StringVar(&serverOpts.Host, "host", "localhost", "Hostname to bind to")
	fs.UintVar(&serverOpts.Port, "port", 8080, "Port to listen on")
	fs.StringVar(&serverOpts.APIRoot, "api-root", "/data", "Root path where to bind the API to")
	fs.DurationVar(&serverOpts.ShutdownGracePeriod, "shutdown-grace-perod", 15*time.Second, "Grace period to wait for server shutdown before using the force")

	// Streaming options
	fs.StringVar(&streamingOpts.Stream, "rabbitmq-stream-name", "lp_stream_health_v0", "Name of RabbitMQ stream to create and consume from")
	fs.StringVar(&streamingOpts.ConsumerName, "consumer-name", "", `Consumer name to use when consuming stream (default "analyzer-${hostname}")`)
	fs.StringVar(&streamingOpts.MaxLengthBytes, "stream-max-length", "50gb", "When creating a new stream, config for max total storage size")
	fs.StringVar(&streamingOpts.MaxSegmentSizeBytes, "stream-max-segment-size", "500mb", "When creating a new stream, config for max stream segment size in storage")
	fs.DurationVar(&streamingOpts.MaxAge, "stream-max-age", 30*24*time.Hour, `When creating a new stream, config for max age of stored events`)
	fs.DurationVar(&memoryRecordsTtl, "memory-records-ttl", 24*time.Hour, `How long to keep data records in memory about inactive streams`)

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
		streamingOpts.ConsumerName = "analyzer-" + hostname()
	}
	if shardPrefixesFlag != "" {
		shardPrefixes = strings.Split(shardPrefixesFlag, ",")
	}
}

func main() {
	glog.Infof("Stream health care system starting up... version=%q", Version)
	ctx := contextUntilSignal(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	streamUri := rabbitmqUri
	if amqpUri == "" && strings.HasPrefix(streamUri, "amqp") {
		streamUri, amqpUri = "", streamUri
	}
	consumer, err := event.NewStreamConsumer(streamUri, amqpUri)
	if err != nil {
		glog.Fatalf("Error creating stream consumer. err=%q", err)
	}
	defer consumer.Close()

	reducer := reducers.Default(golivepeerExchange, shardPrefixes)
	healthcore := health.NewCore(health.CoreOptions{
		Streaming:        streamingOpts,
		StartTimeOffset:  reducers.DefaultStarTimeOffset(),
		MemoryRecordsTtl: memoryRecordsTtl,
	}, consumer, reducer)
	if err := healthcore.Start(ctx); err != nil {
		glog.Fatalf("Error starting health core. err=%q", err)
	}

	glog.Info("Starting server...")
	err = api.ListenAndServe(ctx, serverOpts, healthcore)
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

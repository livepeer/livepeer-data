package analyzer

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
	"github.com/livepeer/livepeer-data/pkg/mistconnector"
	"github.com/livepeer/livepeer-data/views"
	"github.com/peterbourgon/ff"
)

// Build flags to be overwritten at build-time and passed to Run()
type BuildFlags struct {
	Version string
}

type cliFlags struct {
	mistJson bool

	rabbitmqUri string
	amqpUri     string

	golivepeerExchange  string
	shardPrefixesFlag   string
	shardPrefixes       []string
	streamStateExchange string

	serverOpts       api.ServerOptions
	streamingOpts    health.StreamingOptions
	memoryRecordsTtl time.Duration

	viewsOpts views.ClientOptions
}

func parseFlags(version string) cliFlags {
	cli := cliFlags{}
	fs := flag.NewFlagSet("analyzer", flag.ExitOnError)

	fs.BoolVar(&cli.mistJson, "j", false, "Print application info as json")

	fs.StringVar(&cli.rabbitmqUri, "rabbitmq-uri", "amqp://guest:guest@localhost:5672/livepeer", "URI for RabbitMQ server to consume from. Can be specified as a default AMQP URI which will be converted to stream protocol.")
	fs.StringVar(&cli.amqpUri, "amqp-uri", "", "Explicit AMQP URI in case of non-default protocols/ports (optional). Must point to the same cluster as rabbitmqUri")

	fs.StringVar(&cli.golivepeerExchange, "golivepeer-exchange", "lp_golivepeer_metadata", "Name of RabbitMQ exchange to bind the stream to on creation")
	fs.StringVar(&cli.shardPrefixesFlag, "shard-prefixes", "", "Comma-separated list of prefixes of manifest IDs to process events from")
	fs.StringVar(&cli.streamStateExchange, "stream-state-exchange", "lp_mist_api_connector", "Name of RabbitMQ exchange where to receive stream state events")

	// Server options
	fs.StringVar(&cli.serverOpts.Host, "host", "localhost", "Hostname to bind to")
	fs.UintVar(&cli.serverOpts.Port, "port", 8080, "Port to listen on")
	fs.DurationVar(&cli.serverOpts.ShutdownGracePeriod, "shutdown-grace-perod", 15*time.Second, "Grace period to wait for server shutdown before using the force")
	// API Handler
	fs.StringVar(&cli.serverOpts.APIRoot, "api-root", "/data", "Root path where to bind the API to")
	fs.BoolVar(&cli.serverOpts.Prometheus, "prometheus", false, "Whether to enable Prometheus metrics registry and expose /metrics endpoint")
	fs.StringVar(&cli.serverOpts.AuthURL, "auth-url", "", "Endpoint for an auth server to call for both authentication and authorization of API calls")
	fs.StringVar(&cli.serverOpts.OwnRegion, "own-region", "", "Identifier of the region where the service is running, used for triggering global request proxying")
	fs.StringVar(&cli.serverOpts.RegionalHostFormat, "regional-host-format", "localhost", "Format to build regional URL for proxying to other regions. Should contain 1 %s directive where the region will be replaced (e.g. %s.livepeer.monster)")

	// Streaming options
	fs.StringVar(&cli.streamingOpts.Stream, "rabbitmq-stream-name", "lp_stream_health_v0", "Name of RabbitMQ stream to create and consume from")
	fs.StringVar(&cli.streamingOpts.ConsumerName, "consumer-name", "", `Consumer name to use when consuming stream (default "analyzer-${hostname}")`)
	fs.StringVar(&cli.streamingOpts.MaxLengthBytes, "stream-max-length", "50gb", "When creating a new stream, config for max total storage size")
	fs.StringVar(&cli.streamingOpts.MaxSegmentSizeBytes, "stream-max-segment-size", "500mb", "When creating a new stream, config for max stream segment size in storage")
	fs.DurationVar(&cli.streamingOpts.MaxAge, "stream-max-age", 30*24*time.Hour, `When creating a new stream, config for max age of stored events`)
	fs.DurationVar(&cli.streamingOpts.EventFlowSilenceTolerance, "event-flow-silence-tolerance", 10*time.Minute, "The time to tolerate getting zero messages in the stream before giving an error on the service healthcheck")
	fs.DurationVar(&cli.memoryRecordsTtl, "memory-records-ttl", 24*time.Hour, `How long to keep data records in memory about inactive streams`)

	// Views client options
	fs.StringVar(&cli.viewsOpts.Livepeer.Server, "livepeer-api-server", "localhost:3004", "Base URL for the Livepeer API")
	fs.StringVar(&cli.viewsOpts.Livepeer.AccessToken, "livepeer-access-token", "", "Access token for Livepeer API")
	fs.StringVar(&cli.viewsOpts.Prometheus.Address, "prometheus-address", "", "Address of the Prometheus API")

	flag.Set("logtostderr", "true")
	glogVFlag := flag.Lookup("v")
	verbosity := fs.Int("v", 0, "Log verbosity {0-10}")

	fs.String("config", "", "config file (optional)")
	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("LP"),
		ff.WithEnvVarIgnoreCommas(true),
	)
	flag.CommandLine.Parse(nil)
	glogVFlag.Value.Set(strconv.Itoa(*verbosity))

	if cli.streamingOpts.ConsumerName == "" {
		cli.streamingOpts.ConsumerName = "analyzer-" + hostname()
	}
	if cli.shardPrefixesFlag != "" {
		cli.shardPrefixes = strings.Split(cli.shardPrefixesFlag, ",")
	}

	if cli.mistJson {
		mistconnector.PrintMistConfigJson(
			"LivepeerAnalyzer",
			"Stream health data analyzer service for aggregated reporting of running streams status",
			"Livepeer Analyzer",
			version,
			fs,
		)
		os.Exit(0)
	}

	return cli
}

func Run(build BuildFlags) {
	cli := parseFlags(build.Version)
	cli.serverOpts.APIHandlerOptions.ServerName = "analyzer/" + build.Version

	glog.Infof("Stream health care system starting up... version=%q", build.Version)
	ctx := contextUntilSignal(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	streamUri := cli.rabbitmqUri
	if cli.amqpUri == "" && strings.HasPrefix(streamUri, "amqp") {
		streamUri, cli.amqpUri = "", streamUri
	}
	consumer, err := event.NewStreamConsumer(streamUri, cli.amqpUri)
	if err != nil {
		glog.Fatalf("Error creating stream consumer. err=%q", err)
	}
	defer consumer.Close()

	reducer := reducers.Default(cli.golivepeerExchange, cli.shardPrefixes, cli.streamStateExchange)
	healthcore := health.NewCore(health.CoreOptions{
		Streaming:        cli.streamingOpts,
		StartTimeOffset:  reducers.DefaultStarTimeOffset(),
		MemoryRecordsTtl: cli.memoryRecordsTtl,
	}, consumer, reducer)
	if err := healthcore.Start(ctx); err != nil {
		glog.Fatalf("Error starting health core. err=%q", err)
	}

	views, err := views.NewClient(cli.viewsOpts)
	if err != nil {
		glog.Fatalf("Error creating views client. err=%q", err)
	}

	glog.Info("Starting server...")
	err = api.ListenAndServe(ctx, cli.serverOpts, healthcore, views)
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

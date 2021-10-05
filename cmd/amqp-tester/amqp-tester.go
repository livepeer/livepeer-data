package main

import (
	"context"
	"flag"
	"time"

	// "net"
	// "net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/pkg/event"
	"github.com/peterbourgon/ff"
)

var (
	// Version content of this constant will be set at build time,
	// using -ldflags, using output of the `git describe` command.
	Version = "undefined"

	// CLI flags, bound in init below
	amqpUri string

	queueName string
)

func init() {
	fs := flag.NewFlagSet("amqp-tester", flag.ExitOnError)

	fs.StringVar(&amqpUri, "amqp-uri", "amqp://localhost:5672", "AMQP URI to connect to")

	fs.StringVar(&queueName, "queue-name", "unknown_queue_does_not_exist", "Name of queue with which to test AMQP connection")

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
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = contextUntilSignal(ctx, syscall.SIGINT, syscall.SIGTERM)

	// uri, err := url.Parse(amqpUri)
	// checkErr(err, "Bad amqp url: %+v")
	// _, err = net.Dial("tcp", uri.Host)
	// checkErr(err, "failed dial: %+v")

	producer, err := event.NewAMQPQueueProducer(ctx, amqpUri, queueName)
	checkErr(err, "Cannot connect to broker: %+v")

	err = producer.Publish(ctx, "hello-world", struct{ A string }{A: "whats up"}, false)
	checkErr(err, "Cannot publish message: %+v")
}

func contextUntilSignal(parent context.Context, sigs ...os.Signal) context.Context {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		defer cancel()
		waitSignal(sigs...)
	}()
	return ctx
}

func checkErr(err error, message string) {
	if err != nil {
		glog.Fatalf(message, err)
	}
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

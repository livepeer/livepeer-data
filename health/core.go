package health

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/healthy-streams/event"
)

const bindingKey = "#.stream_health.transcode.#"

var (
	healthyMustHaves = map[ConditionType]bool{
		ConditionTranscoding: true,
		ConditionRealTime:    true,
	}
	defaultConditions = []ConditionType{ConditionTranscoding, ConditionRealTime, ConditionNoErrors}
	statsWindows      = []time.Duration{1 * time.Minute, 10 * time.Minute, 1 * time.Hour}
	maxStatsWindow    = statsWindows[len(statsWindows)-1]
)

// Purposedly made of built-in types only to bind directly to cli flags.
type StreamingOptions struct {
	Stream, Exchange string
	ConsumerName     string

	event.RawStreamOptions
}

type CoreOptions struct {
	Streaming StreamingOptions
}

type EventHandler = func(record *Record, evt Event) (newStatus Status, handled bool)

type Core struct {
	RecordStorage

	opts     CoreOptions
	consumer event.StreamConsumer
	started  bool

	handlers []EventHandler
}

func NewCore(opts CoreOptions, consumer event.StreamConsumer) *Core {
	return &Core{
		opts:     opts,
		consumer: consumer,
		handlers: []EventHandler{HandleTranscodeEvent},
	}
}

func (c *Core) Start(ctx context.Context) error {
	if c.started {
		return errors.New("health core already started")
	}
	c.started = true

	consumeOpts, err := consumeOptions(c.opts.Streaming)
	if err != nil {
		return fmt.Errorf("invalid rabbitmq options: %w", err)
	}

	err = c.consumer.Consume(ctx, consumeOpts, c)
	if err != nil {
		return fmt.Errorf("failed to consume stream: %w", err)
	}
	return nil
}

func (c *Core) HandleMessage(msg event.StreamMessage) {
	for _, data := range msg.Data {
		evt, err := ParseEvent(data)
		if err != nil {
			glog.Errorf("Health core received malformed message. err=%q, data=%q", err, data)
			continue
		}
		c.handleSingleEvent(evt)
	}
}

func (c *Core) handleSingleEvent(evt Event) {
	mid := evt.ManifestID()
	record := c.GetOrCreate(mid, defaultConditions, statsWindows)

	for _, handler := range c.handlers {
		if newStatus, handled := handler(record, evt); handled {
			record.LastStatus = newStatus
			break
		}
	}
	record.PastEvents = append(record.PastEvents, evt) // TODO: crop/drop these at some point
}

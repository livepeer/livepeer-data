package health

import (
	"context"
	"encoding/json"
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

type Core struct {
	RecordStorage

	opts     CoreOptions
	consumer event.StreamConsumer
	started  bool
}

func NewCore(opts CoreOptions, consumer event.StreamConsumer) *Core {
	return &Core{opts: opts, consumer: consumer}
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
		var evt *TranscodeEvent
		err := json.Unmarshal(data, &evt)
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

	record.LastStatus = reduceStreamHealth(record, evt)
	record.PastEvents = append(record.PastEvents, evt) // TODO: crop/drop these at some point
}

func reduceStreamHealth(record *Record, evt Event) Status {
	ts := evt.Timestamp()
	conditions := make([]*Condition, len(record.Conditions))
	for i, condType := range record.Conditions {
		status := conditionStatus(evt, condType)
		stats := record.ConditionStats[condType].Averages(record.StatsWindows, ts, ptrBoolToFloat(status))

		last := record.LastStatus.GetCondition(condType)
		conditions[i] = NewCondition(condType, ts, status, stats, last)
	}

	return Status{
		ManifestID: evt.ManifestID(),
		Healthy:    diagnoseStream(record, conditions, ts),
		Conditions: conditions,
	}
}

func conditionStatus(evtIface Event, condType ConditionType) *bool {
	switch evt := evtIface.(type) {
	case *TranscodeEvent:
		switch condType {
		case ConditionTranscoding:
			return &evt.Success
		case ConditionRealTime:
			isRealTime := evt.LatencyMs < int64(evt.Segment.Duration*1000)
			return &isRealTime
		case ConditionNoErrors:
			noErrors := true
			for _, attempt := range evt.Attempts {
				noErrors = noErrors && attempt.Error == nil
			}
			return &noErrors
		}
	}
	return nil
}

func diagnoseStream(record *Record, currConditions []*Condition, ts time.Time) Condition {
	healthyMustsCount := 0
	for _, cond := range currConditions {
		if healthyMustHaves[cond.Type] && cond.Status != nil && *cond.Status {
			healthyMustsCount++
		}
	}
	isHealthy := healthyMustsCount == len(healthyMustHaves)
	healthStats := record.HealthStats.Averages(record.StatsWindows, ts, ptrBoolToFloat(&isHealthy))

	last := &record.LastStatus.Healthy
	return *NewCondition("", ts, &isHealthy, healthStats, last)
}

func consumeOptions(opts StreamingOptions) (event.ConsumeOptions, error) {
	streamOpts, err := event.ParseStreamOptions(opts.RawStreamOptions)
	if err != nil {
		return event.ConsumeOptions{}, fmt.Errorf("bad stream options: %w", err)
	}

	startTime := time.Now().Add(-maxStatsWindow)
	return event.ConsumeOptions{
		Stream: opts.Stream,
		StreamOptions: &event.StreamOptions{
			StreamOptions: *streamOpts,
			Bindings: []event.BindingArgs{
				{Key: bindingKey, Exchange: opts.Exchange},
			},
		},
		ConsumerOptions: event.NewConsumerOptions(opts.ConsumerName, event.TimestampOffset(startTime)),
		MemorizeOffset:  true,
	}, nil
}

func ptrBoolToFloat(b *bool) *float64 {
	if b == nil {
		return nil
	}
	val := float64(0)
	if *b {
		val = 1
	}
	return &val
}

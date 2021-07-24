package health

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/healthy-streams/event"
)

var (
	statsWindows   = []time.Duration{1 * time.Minute, 10 * time.Minute, 1 * time.Hour}
	maxStatsWindow = statsWindows[len(statsWindows)-1]
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

type Reducer interface {
	Bindings(golpExchange string) []event.BindingArgs
	Conditions() []ConditionType
	Reduce(current Status, state interface{}, evt Event) (Status, interface{})
}

type ReducerFunc func(Status, interface{}, Event) (Status, interface{})

func (f ReducerFunc) Bindings(_ string) []event.BindingArgs { return nil }
func (f ReducerFunc) Conditions() []ConditionType           { return nil }
func (f ReducerFunc) Reduce(current Status, state interface{}, evt Event) (Status, interface{}) {
	return f(current, state, evt)
}

type Core struct {
	RecordStorage

	opts     CoreOptions
	consumer event.StreamConsumer
	started  bool

	reducers       []Reducer
	conditionTypes []ConditionType
}

func NewCore(opts CoreOptions, consumer event.StreamConsumer) *Core {
	reducers := []Reducer{transcodeReducer{}, ReducerFunc(ReduceHealth), statsReducer{statsWindows}}
	return &Core{
		opts:           opts,
		consumer:       consumer,
		reducers:       reducers,
		conditionTypes: conditionTypes(reducers),
	}
}

func (c *Core) Start(ctx context.Context) error {
	if c.started {
		return errors.New("health core already started")
	}
	c.started = true

	consumeOpts, err := c.consumeOptions()
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
	record := c.GetOrCreate(mid, c.conditionTypes)

	status := record.LastStatus
	for _, reducer := range c.reducers {
		state := record.ReducersState[reducer]
		status, state = reducer.Reduce(status, state, evt)
		record.ReducersState[reducer] = state
	}
	record.LastStatus = status
	record.PastEvents = append(record.PastEvents, evt) // TODO: crop/drop these at some point
}

func (c *Core) consumeOptions() (event.ConsumeOptions, error) {
	opts := c.opts.Streaming
	streamOpts, err := event.ParseStreamOptions(opts.RawStreamOptions)
	if err != nil {
		return event.ConsumeOptions{}, fmt.Errorf("bad stream options: %w", err)
	}

	bindings := []event.BindingArgs{}
	added := map[string]bool{}
	for _, reducer := range c.reducers {
		for _, newBind := range reducer.Bindings(c.opts.Streaming.Exchange) {
			key := newBind.Key + " / " + newBind.Exchange
			if added[key] {
				return event.ConsumeOptions{}, fmt.Errorf("duplicate binding: %s", key)
			}
			added[key] = true
			bindings = append(bindings, reducer.Bindings(c.opts.Streaming.Exchange)...)
		}
	}

	startTime := time.Now().Add(-maxStatsWindow)
	return event.ConsumeOptions{
		Stream: opts.Stream,
		StreamOptions: &event.StreamOptions{
			StreamOptions: *streamOpts,
			Bindings:      bindings,
		},
		ConsumerOptions: event.NewConsumerOptions(opts.ConsumerName, event.TimestampOffset(startTime)),
		MemorizeOffset:  true,
	}, nil
}

func conditionTypes(reducers []Reducer) []ConditionType {
	added := map[ConditionType]bool{}
	conds := []ConditionType{}
	for _, reducer := range reducers {
		for _, newCond := range reducer.Conditions() {
			if added[newCond] {
				continue
			}
			added[newCond] = true
			conds = append(conds, newCond)
		}
	}
	return conds
}

package health

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

var ErrStreamNotFound = errors.New("stream not found")

const eventSubscriptionBufSize = 10

// Purposedly made of built-in types only to bind directly to cli flags.
type StreamingOptions struct {
	Stream, ConsumerName string

	event.RawStreamOptions
}

type CoreOptions struct {
	Streaming       StreamingOptions
	StartTimeOffset time.Duration
}

type Core struct {
	opts     CoreOptions
	consumer event.StreamConsumer
	started  bool

	reducers       []Reducer
	conditionTypes []ConditionType

	storage RecordStorage
}

func NewCore(opts CoreOptions, consumer event.StreamConsumer) *Core {
	return &Core{
		opts:     opts,
		consumer: consumer,
	}
}

func (c *Core) Use(reducers ...Reducer) *Core {
	if c.started {
		panic("must add reducers before starting")
	}
	c.reducers = append(c.reducers, reducers...)
	return c
}

func (c *Core) Start(ctx context.Context) error {
	if c.started {
		return errors.New("health core already started")
	}
	c.started = true
	c.conditionTypes = conditionTypes(c.reducers)

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
	for _, rawEvt := range msg.Data {
		evt, err := data.ParseEvent(rawEvt)
		if err != nil {
			glog.Errorf("Health core received malformed message. err=%q, data=%q", err, rawEvt)
			continue
		}
		c.handleSingleEvent(evt)
	}
}

func (c *Core) handleSingleEvent(evt data.Event) {
	mid := evt.ManifestID()
	record := c.storage.GetOrCreate(mid, c.conditionTypes)

	status := record.LastStatus
	for i, reducer := range c.reducers {
		state := record.ReducersState[i]
		status, state = reducer.Reduce(status, state, evt)
		record.ReducersState[i] = state
	}

	record.Lock()
	defer record.Unlock()
	record.LastStatus = status
	record.PastEvents = append(record.PastEvents, evt) // TODO: Sort these. Also crop/drop them at some point
	for _, subs := range record.EventSubs {
		select {
		case subs <- evt:
		default:
			glog.Warningf("Buffer full for health event subscription, skipping message. manifestId=%q, eventTs=%q", mid, evt.Timestamp())
		}
	}
}

func (c *Core) GetStatus(manifestID string) (Status, error) {
	record, ok := c.storage.Get(manifestID)
	if !ok {
		return Status{}, ErrStreamNotFound
	}
	return record.LastStatus, nil
}

func (c *Core) GetPastEvents(manifestID string, from, to *time.Time) ([]data.Event, error) {
	record, ok := c.storage.Get(manifestID)
	if !ok {
		return nil, ErrStreamNotFound
	}
	record.RLock()
	defer record.RUnlock()
	return getPastEventsLocked(record, from, to)
}

func (c *Core) SubscribeEvents(ctx context.Context, manifestID string, from *time.Time) ([]data.Event, <-chan data.Event, error) {
	var err error
	record, ok := c.storage.Get(manifestID)
	if !ok {
		return nil, nil, ErrStreamNotFound
	}
	record.Lock()
	defer record.Unlock()

	var pastEvents []data.Event
	if from != nil {
		pastEvents, err = getPastEventsLocked(record, from, nil)
		if err != nil {
			return nil, nil, err
		}
	}
	subs := make(chan data.Event, eventSubscriptionBufSize)
	record.EventSubs = append(record.EventSubs, subs)
	go func() {
		defer close(subs)
		<-ctx.Done()

		record.Lock()
		defer record.Unlock()
		for i := range record.EventSubs {
			if subs == record.EventSubs[i] {
				record.EventSubs = append(record.EventSubs[:i], record.EventSubs[i+1:]...)
				return
			}
		}
	}()
	return pastEvents, subs, nil
}

func getPastEventsLocked(record *Record, from, to *time.Time) ([]data.Event, error) {
	fromIdx, toIdx := 0, len(record.PastEvents)
	if from != nil {
		fromIdx = firstIdxAfter(record.PastEvents, *from)
	}
	if to != nil {
		toIdx = firstIdxAfter(record.PastEvents, *to)
	}
	if toIdx < fromIdx {
		return nil, errors.New("from timestamp must be lower than to timestamp")
	}

	ret := make([]data.Event, toIdx-fromIdx)
	copy(ret, record.PastEvents[fromIdx:toIdx])
	return ret, nil
}

func firstIdxAfter(events []data.Event, threshold time.Time) int {
	return sort.Search(len(events), func(i int) bool {
		ts := events[i].Timestamp()
		return ts.After(threshold)
	})
}

func (c *Core) consumeOptions() (event.ConsumeOptions, error) {
	opts := c.opts.Streaming
	streamOpts, err := event.ParseStreamOptions(opts.RawStreamOptions)
	if err != nil {
		return event.ConsumeOptions{}, fmt.Errorf("bad stream options: %w", err)
	}
	bindings, err := bindingArgs(c.reducers)
	if err != nil {
		return event.ConsumeOptions{}, err
	}

	startTime := time.Now().Add(-c.opts.StartTimeOffset)
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

func bindingArgs(reducers []Reducer) ([]event.BindingArgs, error) {
	added := map[string]bool{}
	bindings := []event.BindingArgs{}
	for _, reducer := range reducers {
		for _, newBind := range reducer.Bindings() {
			key := newBind.Key + " / " + newBind.Exchange
			if added[key] {
				return nil, fmt.Errorf("duplicate binding: %s", key)
			}
			added[key] = true
			bindings = append(bindings, newBind)
		}
	}
	return bindings, nil
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

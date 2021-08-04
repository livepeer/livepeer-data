package health

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

var ErrStreamNotFound = errors.New("stream not found")
var ErrEventNotFound = errors.New("event not found")

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
	mid, ts := evt.ManifestID(), evt.Timestamp()
	record := c.storage.GetOrCreate(mid, c.conditionTypes)

	status := record.LastStatus
	for i, reducer := range c.reducers {
		state := record.ReducersState[i]
		status, state = reducer.Reduce(status, state, evt)
		record.ReducersState[i] = state
	}

	record.Lock()
	defer record.Unlock()
	var removed []data.Event
	record.LastStatus = status
	record.PastEvents, removed = insertEventSortedCropped(record.PastEvents, evt, c.opts.StartTimeOffset) // TODO: Rename StartTimeOffset to sth that makes sense here as well
	record.EventsByID[evt.ID()] = evt
	for _, remEvt := range removed {
		delete(record.EventsByID, remEvt.ID())
	}
	for _, subs := range record.EventSubs {
		select {
		case subs <- evt:
		default:
			glog.Warningf("Buffer full for health event subscription, skipping message. manifestId=%q, eventTs=%q", mid, ts)
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
	return getPastEventsLocked(record, nil, from, to)
}

func (c *Core) SubscribeEvents(ctx context.Context, manifestID string, lastEvtID *uuid.UUID, from *time.Time) ([]data.Event, <-chan data.Event, error) {
	var err error
	record, ok := c.storage.Get(manifestID)
	if !ok {
		return nil, nil, ErrStreamNotFound
	}
	record.Lock()
	defer record.Unlock()

	var pastEvents []data.Event
	if lastEvtID != nil || from != nil {
		pastEvents, err = getPastEventsLocked(record, lastEvtID, from, nil)
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

func getPastEventsLocked(record *Record, lastEvtID *uuid.UUID, from, to *time.Time) ([]data.Event, error) {
	fromIdx, toIdx := 0, len(record.PastEvents)
	if lastEvtID != nil {
		evtIdx, err := findEventIdx(record.PastEvents, record.EventsByID, *lastEvtID)
		if err != nil {
			return nil, err
		}
		fromIdx = evtIdx + 1
	} else if from != nil {
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

func findEventIdx(events []data.Event, byID map[uuid.UUID]data.Event, evtID uuid.UUID) (int, error) {
	evt, ok := byID[evtID]
	if !ok {
		return -1, ErrEventNotFound
	}
	timestamp := evt.Timestamp()
	afterTsIdx := firstIdxAfter(events, timestamp)

	for idx := afterTsIdx - 1; idx >= 0; idx-- {
		if evt := events[idx]; evt.ID() == evtID {
			return idx, nil
		} else if timestamp.After(evt.Timestamp()) {
			break
		}
	}
	return -1, errors.New("internal: event from map not found in slice")
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

func insertEventSortedCropped(events []data.Event, event data.Event, maxWindow time.Duration) (updated []data.Event, removed []data.Event) {
	ts := event.Timestamp()
	insertIdx := len(events)
	for insertIdx > 0 && ts.Before(events[insertIdx-1].Timestamp()) {
		insertIdx--
	}
	events = insertEventAtIdx(events, insertIdx, event)

	maxTs := events[len(events)-1].Timestamp()
	threshold := maxTs.Add(-maxWindow)
	for cropIdx := 0; cropIdx < len(events); cropIdx++ {
		if threshold.Before(events[cropIdx].Timestamp()) {
			return events[cropIdx:], events[:cropIdx]
		}
	}
	return []data.Event{}, events
}

func insertEventAtIdx(slc []data.Event, idx int, val data.Event) []data.Event {
	slc = append(slc, nil)
	copy(slc[idx+1:], slc[idx:])
	slc[idx] = val
	return slc
}

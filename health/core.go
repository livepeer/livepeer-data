package health

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/livepeer/livepeer-data/metrics"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	eventSubscriptionBufSize = 10
	processLogSampleRate     = 0.04
)

var (
	ErrStreamNotFound = errors.New("stream not found")
	ErrEventNotFound  = errors.New("event not found")

	eventsProcessedCount = metrics.Factory.NewCounterVec(prometheus.CounterOpts{
		Name: metrics.FQName("events_processed_total"),
		Help: "Count of events processed by the healthcore system, partitioned by event type",
	},
		[]string{"event_type"},
	)
	eventsProcessingDuration = metrics.Factory.NewSummaryVec(prometheus.SummaryOpts{
		Name: metrics.FQName("events_processing_duration_milliseconds"),
		Help: "Duration for processing a given event type on healthcore system in milliseconds",
	},
		[]string{"event_type"},
	)
	eventsTimeOffset = metrics.Factory.NewSummary(prometheus.SummaryOpts{
		Name: metrics.FQName("events_time_offset_seconds"),
		Help: "Offset between processed events timestamp and the current system time in seconds",
	})
	recordStorageSize = metrics.Factory.NewGauge(prometheus.GaugeOpts{
		Name: metrics.FQName("record_storage_size"),
		Help: "Gauge for the current count of streams stored in memory in the record storage",
	})
)

// Purposedly made of built-in types only to bind directly to cli flags.
type StreamingOptions struct {
	Stream, ConsumerName string

	event.RawStreamOptions
}

type CoreOptions struct {
	Streaming        StreamingOptions
	StartTimeOffset  time.Duration
	MemoryRecordsTtl time.Duration
}

type Core struct {
	opts     CoreOptions
	consumer event.StreamConsumer
	started  bool

	reducer        Reducer
	conditionTypes []data.ConditionType

	storage RecordStorage
}

func NewCore(opts CoreOptions, consumer event.StreamConsumer, reducer Reducer) *Core {
	return &Core{
		opts:     opts,
		consumer: consumer,
		reducer:  reducer,
		storage:  RecordStorage{SizeGauge: recordStorageSize},
	}
}

func (c *Core) IsHealthy() bool {
	err := c.consumer.CheckConnection()
	if err != nil {
		glog.Warningf("Health core is unhealthy. consumerErr=%q", err)
		return false
	}
	return true
}

func (c *Core) Start(ctx context.Context) error {
	if c.started {
		return errors.New("health core already started")
	}
	c.started = true
	c.conditionTypes = c.reducer.Conditions()

	consumeOpts, err := c.consumeOptions()
	if err != nil {
		return fmt.Errorf("invalid rabbitmq options: %w", err)
	}
	glog.Infof("Starting health core. conditions=%+v, stream=%s, bindings=%+v", c.conditionTypes, consumeOpts.Stream, consumeOpts.Bindings)

	err = c.consumer.Consume(ctx, consumeOpts, c)
	if err != nil {
		return fmt.Errorf("failed to consume stream: %w", err)
	}
	if c.opts.MemoryRecordsTtl > 0 {
		c.storage.StartCleanupLoop(ctx, c.opts.MemoryRecordsTtl)
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

		start := time.Now()
		c.handleSingleEvent(evt)
		dur := time.Since(start)

		eventsProcessedCount.WithLabelValues(string(evt.Type())).
			Inc()
		eventsProcessingDuration.WithLabelValues(string(evt.Type())).
			Observe(dur.Seconds() * 1000)
		if evtOffset := time.Since(evt.Timestamp()); evtOffset > 0 {
			eventsTimeOffset.Observe(evtOffset.Seconds())
		}
	}
}

func (c *Core) handleSingleEvent(evt data.Event) {
	streamID, ts := evt.StreamID(), evt.Timestamp()
	record := c.storage.GetOrCreate(streamID, c.conditionTypes)

	record.RLock()
	status, state := record.LastStatus, record.ReducerState
	record.RUnlock()
	// Only 1 go-routine processing events rn, so no need for locking here.
	status, state = c.reducer.Reduce(status, state, evt)

	record.Lock()
	defer record.Unlock()
	var removed []data.Event
	record.LastStatus, record.ReducerState = status, state
	record.PastEvents, removed = insertEventSortedCropped(record.PastEvents, evt, c.opts.StartTimeOffset) // TODO: Rename StartTimeOffset to sth that makes sense here as well
	record.EventsByID[evt.ID()] = evt
	for _, remEvt := range removed {
		delete(record.EventsByID, remEvt.ID())
	}
	if glog.V(4) && rand.Float32() < processLogSampleRate {
		glog.Infof("Sampled: Health core processing event. streamID=%s, ts=%s, pastEventsLen=%d, removedPastEvents=%d, event=%+v status=%+v",
			streamID, ts, len(record.PastEvents), len(removed), evt, status)
	}

	for _, subs := range record.EventSubs {
		select {
		case subs <- evt:
		default:
			glog.Warningf("Buffer full for health event subscription, skipping message. streamId=%q, eventTs=%q", streamID, ts)
		}
	}
}

func (c *Core) GetStatus(manifestID string) (*data.HealthStatus, error) {
	record, ok := c.storage.Get(manifestID)
	if !ok {
		return nil, ErrStreamNotFound
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
	subs := record.SubscribeLocked(ctx, make(chan data.Event, eventSubscriptionBufSize))
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

	bindings := c.reducer.Bindings()
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

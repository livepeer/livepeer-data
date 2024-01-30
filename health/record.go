package health

import (
	"context"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/prometheus/client_golang/prometheus"
)

type Record struct {
	ID         string
	Conditions []data.ConditionType

	sync.RWMutex
	initialized chan struct{}
	disposed    chan struct{}

	PastEvents []data.Event
	EventsByID map[uuid.UUID]data.Event
	EventSubs  []chan<- data.Event

	ReducerState interface{}
	LastStatus   *data.HealthStatus
}

func NewRecord(id string, conditionTypes []data.ConditionType) *Record {
	conditions := make([]*data.Condition, len(conditionTypes))
	for i, cond := range conditionTypes {
		conditions[i] = data.NewCondition(cond, time.Time{}, nil, nil)
	}
	return &Record{
		ID:          id,
		Conditions:  conditionTypes,
		initialized: make(chan struct{}),
		disposed:    make(chan struct{}),
		EventsByID:  map[uuid.UUID]data.Event{},
		LastStatus:  data.NewHealthStatus(id, conditions),
	}
}

// FlagInitialized will flag the record as initialized. It is meant to be called
// after the first event is processed, meaning the record is not empty anymore.
//
// This is used to allow waiting until a stream is started by creating its
// record in an uninitialized state first and calling `WaitInitialized`. The
// initialization flag is simply a channel that is closed, which will unblock
// all goroutines waiting to receive from it (`WaitInitialized`).
func (r *Record) FlagInitialized() {
	if !r.IsInitialized() {
		close(r.initialized)
	}
}

func (r *Record) IsInitialized() bool {
	select {
	case <-r.initialized:
		return true
	default:
		return false
	}
}

func (r *Record) WaitInitialized(ctx context.Context) error {
	select {
	case <-r.initialized:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Record) SubscribeLocked(ctx context.Context, subs chan data.Event) chan data.Event {
	r.EventSubs = append(r.EventSubs, subs)
	go func() {
		defer close(subs)
		select {
		case <-ctx.Done():
		case <-r.disposed:
		}

		r.Lock()
		defer r.Unlock()
		for i := range r.EventSubs {
			if subs == r.EventSubs[i] {
				r.EventSubs = append(r.EventSubs[:i], r.EventSubs[i+1:]...)
				return
			}
		}
	}()
	return subs
}

type RecordStorage struct {
	records   sync.Map
	SizeGauge prometheus.Gauge
}

func (s *RecordStorage) StartCleanupLoop(ctx context.Context, ttl time.Duration) {
	go func() {
		ticker := time.NewTicker(ttl / 100)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				threshold := time.Now().Add(-ttl)
				recordsSize := 0
				s.records.Range(func(key interface{}, value interface{}) bool {
					recordsSize++
					record := value.(*Record)
					lastProbeTime := record.LastStatus.Healthy.LastProbeTime
					if lastProbeTime != nil && lastProbeTime.Before(threshold) {
						glog.Infof("Disposing of health record. id=%q, lastProbeTime=%q, ttl=%q", record.ID, lastProbeTime, ttl)
						close(record.disposed)
						s.records.Delete(key)
						recordsSize--
					}
					return true
				})
				glog.Infof("Finished records clean-up loop. size=%d", recordsSize)
				if s.SizeGauge != nil {
					s.SizeGauge.Set(float64(recordsSize))
				}
			case <-ctx.Done():
				// erase any dangling references
				s.records = sync.Map{}
				return
			}
		}
	}()
}

func (s *RecordStorage) Get(id string) (*Record, bool) {
	if saved, ok := s.records.Load(id); ok {
		// Until Initialize is called, the record is considered inexistent
		if record := saved.(*Record); record.IsInitialized() {
			return record, true
		}
	}
	return nil, false
}

func (s *RecordStorage) GetOrCreate(id string, conditions []data.ConditionType) *Record {
	if saved, ok := s.Get(id); ok {
		return saved
	}
	new := NewRecord(id, conditions)
	if actual, loaded := s.records.LoadOrStore(id, new); loaded {
		return actual.(*Record)
	}
	glog.Infof("Created new health record. id=%q", id)
	if s.SizeGauge != nil {
		s.SizeGauge.Inc()
	}
	return new
}

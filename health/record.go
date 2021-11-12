package health

import (
	"context"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/livepeer/livepeer-data/pkg/data"
)

type Record struct {
	ID         string
	Conditions []ConditionType

	sync.RWMutex
	disposed chan struct{}

	PastEvents []data.Event
	EventsByID map[uuid.UUID]data.Event
	EventSubs  []chan<- data.Event

	ReducerState interface{}
	LastStatus   *Status
}

func NewRecord(id string, conditionTypes []ConditionType) *Record {
	conditions := make([]*Condition, len(conditionTypes))
	for i, cond := range conditionTypes {
		conditions[i] = NewCondition(cond, time.Time{}, nil, nil)
	}
	return &Record{
		ID:         id,
		Conditions: conditionTypes,
		disposed:   make(chan struct{}),
		EventsByID: map[uuid.UUID]data.Event{},
		LastStatus: NewStatus(id, conditions),
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
	records sync.Map
}

func (s *RecordStorage) StartCleanupLoop(ctx context.Context, ttl time.Duration) {
	go func() {
		ticker := time.NewTicker(ttl / 100)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				threshold := time.Now().Add(-ttl)
				recordsLen := 0
				s.records.Range(func(key interface{}, value interface{}) bool {
					recordsLen++
					record := value.(*Record)
					lastProbeTime := record.LastStatus.Healthy.LastProbeTime
					if lastProbeTime != nil && lastProbeTime.Before(threshold) {
						glog.Infof("Disposing of health record. id=%q, lastProbeTime=%q, ttl=%q", record.ID, lastProbeTime, ttl)
						close(record.disposed)
						s.records.Delete(key)
						recordsLen--
					}
					return true
				})
				glog.Infof("Finished records clean-up loop. len=%d", recordsLen)
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
		return saved.(*Record), true
	}
	return nil, false
}

func (s *RecordStorage) GetOrCreate(id string, conditions []ConditionType) *Record {
	if saved, ok := s.Get(id); ok {
		return saved
	}
	glog.Infof("Creating new health record. id=%q", id)
	new := NewRecord(id, conditions)
	if actual, loaded := s.records.LoadOrStore(id, new); loaded {
		return actual.(*Record)
	}
	return new
}

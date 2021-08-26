package health

import (
	"context"
	"sync"
	"time"

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

	ReducersState map[int]interface{}
	LastStatus    *Status
}

func NewRecord(id string, conditions []ConditionType) *Record {
	rec := &Record{
		ID:            id,
		Conditions:    conditions,
		disposed:      make(chan struct{}),
		EventsByID:    map[uuid.UUID]data.Event{},
		ReducersState: map[int]interface{}{},
		LastStatus: &Status{
			ID:         id,
			Healthy:    NewCondition("", time.Time{}, nil, nil, nil),
			Conditions: make([]*Condition, len(conditions)),
		},
	}
	for i, cond := range conditions {
		rec.LastStatus.Conditions[i] = NewCondition(cond, time.Time{}, nil, nil, nil)
	}
	return rec
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
				s.records.Range(func(key interface{}, value interface{}) bool {
					record := value.(*Record)
					lastProbeTime := record.LastStatus.Healthy.LastProbeTime
					if lastProbeTime != nil && lastProbeTime.Before(threshold) {
						close(record.disposed)
						s.records.Delete(key)
					}
					return true
				})
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
	new := NewRecord(id, conditions)
	if actual, loaded := s.records.LoadOrStore(id, new); loaded {
		return actual.(*Record)
	}
	return new
}

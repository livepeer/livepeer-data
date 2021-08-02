package health

import (
	"sync"
	"time"

	"github.com/livepeer/livepeer-data/pkg/data"
)

type RecordStorage struct {
	records sync.Map // TODO: crop/drop these at some point
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

type Record struct {
	ID         string
	Conditions []ConditionType

	sync.RWMutex
	PastEvents []data.Event
	EventSubs  []chan<- data.Event

	ReducersState map[int]interface{}
	LastStatus    Status
}

func NewRecord(id string, conditions []ConditionType) *Record {
	rec := &Record{
		ID:            id,
		Conditions:    conditions,
		ReducersState: map[int]interface{}{},
		LastStatus: Status{
			ID:         id,
			Healthy:    *NewCondition("", time.Time{}, nil, nil, nil),
			Conditions: make([]*Condition, len(conditions)),
		},
	}
	for i, cond := range conditions {
		rec.LastStatus.Conditions[i] = NewCondition(cond, time.Time{}, nil, nil, nil)
	}
	return rec
}

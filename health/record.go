package health

import (
	"sync"
	"time"
)

type RecordStorage struct {
	records sync.Map // TODO: crop/drop these at some point
}

func (s *RecordStorage) Get(manifestId string) (*Record, bool) {
	if saved, ok := s.records.Load(manifestId); ok {
		return saved.(*Record), true
	}
	return nil, false
}

func (s *RecordStorage) GetOrCreate(manifestId string, conditions []ConditionType) *Record {
	if saved, ok := s.Get(manifestId); ok {
		return saved
	}
	new := NewRecord(manifestId, conditions)
	if actual, loaded := s.records.LoadOrStore(manifestId, new); loaded {
		return actual.(*Record)
	}
	return new
}

type Record struct {
	ID         string
	Conditions []ConditionType

	PastEvents []Event

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

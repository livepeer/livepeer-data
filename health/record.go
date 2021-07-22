package health

import (
	"sync"
	"time"

	"github.com/livepeer/healthy-streams/stats"
)

type RecordStorage struct {
	records sync.Map
}

func (s *RecordStorage) Get(manifestId string) (*Record, bool) {
	if saved, ok := s.records.Load(manifestId); ok {
		return saved.(*Record), true
	}
	return nil, false
}

func (s *RecordStorage) GetOrCreate(manifestId string, conditions []ConditionType, statsWindows []time.Duration) *Record {
	if saved, ok := s.Get(manifestId); ok {
		return saved
	}
	new := NewRecord(manifestId, conditions, statsWindows)
	if actual, loaded := s.records.LoadOrStore(manifestId, new); loaded {
		return actual.(*Record)
	}
	return new
}

type Record struct {
	ManifestID   string
	Conditions   []ConditionType
	StatsWindows []time.Duration

	PastEvents     []Event
	HealthStats    stats.WindowAggregators
	ConditionStats map[ConditionType]stats.WindowAggregators

	LastStatus Status
}

func NewRecord(mid string, conditions []ConditionType, statsWindows []time.Duration) *Record {
	rec := &Record{
		ManifestID:     mid,
		Conditions:     conditions,
		StatsWindows:   statsWindows,
		HealthStats:    stats.WindowAggregators{},
		ConditionStats: map[ConditionType]stats.WindowAggregators{},
		LastStatus: Status{
			ManifestID: mid,
			Healthy:    *NewCondition("", time.Time{}, nil, nil, nil),
			Conditions: make([]*Condition, len(conditions)),
		},
	}
	for i, cond := range conditions {
		rec.LastStatus.Conditions[i] = NewCondition(cond, time.Time{}, nil, nil, nil)
		rec.ConditionStats[cond] = stats.WindowAggregators{}
	}
	return rec
}

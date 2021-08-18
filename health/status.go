package health

import (
	"time"

	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/stats"
)

type Status struct {
	ID          string               `json:"id"`
	Healthy     Condition            `json:"healthy"`
	Conditions  []*Condition         `json:"conditions"`
	Multistream []*MultistreamStatus `json:"multistream,omitempty"`
}

type MultistreamStatus struct {
	Target     data.MultistreamTargetInfo
	Conditions []*Condition
}

func (s Status) GetCondition(condType ConditionType) *Condition {
	for _, cond := range s.Conditions {
		if cond.Type == condType {
			return cond
		}
	}
	return nil
}

type ConditionType string

type Condition struct {
	Type               ConditionType  `json:"type,omitempty"`
	Status             *bool          `json:"status"`
	Frequency          stats.ByWindow `json:"frequency,omitempty"`
	LastProbeTime      *time.Time     `json:"lastProbeTime"`
	LastTransitionTime *time.Time     `json:"lastTransitionsTime"`
}

func NewCondition(condType ConditionType, ts time.Time, status *bool, frequency stats.ByWindow, last *Condition) *Condition {
	cond := &Condition{Type: condType}
	if last != nil && last.Type == condType {
		*cond = *last
	}
	if status != nil {
		cond.LastProbeTime = &ts
		if cond.Status == nil || *status != *cond.Status {
			cond.LastTransitionTime = &ts
		}
		cond.Status = status
	}
	if frequency != nil {
		cond.Frequency = frequency
	}
	return cond
}

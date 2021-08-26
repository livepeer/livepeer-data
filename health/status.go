package health

import (
	"time"

	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/stats"
)

// Status is a soft-immutable struct. It should never be modified inline or a
// lot of things could become inconsistent. Create a new instance/copy for any
// mutation to be performed and beware of the internal slices and pointers.
//
// Use NewMergedStatus below to facilitate the creation of new status objects
// with mutated fields. Notice that you still need to clone the internal slices
// if you want to do any mutations to them.
type Status struct {
	ID          string               `json:"id"`
	Healthy     *Condition           `json:"healthy"`
	Conditions  []*Condition         `json:"conditions"`
	Multistream []*MultistreamStatus `json:"multistream,omitempty"`
}

func NewMergedStatus(base *Status, values Status) *Status {
	if base == nil {
		return &values
	}
	new := *base
	if values.ID != "" {
		new.ID = values.ID
	}
	if values.Healthy != nil {
		new.Healthy = values.Healthy
	}
	if values.Conditions != nil {
		new.Conditions = values.Conditions
	}
	if values.Multistream != nil {
		new.Multistream = values.Multistream
	}
	return &new
}

func (s Status) ConditionsCopy() []*Condition {
	conditions := make([]*Condition, len(s.Conditions))
	copy(conditions, s.Conditions)
	return conditions
}

func (s Status) MultistreamCopy() []*MultistreamStatus {
	multistream := make([]*MultistreamStatus, len(s.Multistream))
	copy(multistream, s.Multistream)
	return multistream
}

func (s Status) GetCondition(condType ConditionType) *Condition {
	for _, cond := range s.Conditions {
		if cond.Type == condType {
			return cond
		}
	}
	return nil
}

type MultistreamStatus struct {
	Target    data.MultistreamTargetInfo `json:"target"`
	Connected *Condition                 `json:"connected"`
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

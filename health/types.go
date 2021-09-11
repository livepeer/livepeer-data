package health

import (
	"encoding/json"
	"fmt"
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
	ID         string       `json:"id"`
	Healthy    *Condition   `json:"healthy"`
	Conditions []*Condition `json:"conditions"`
	Metrics    []*Metric    `json:"metrics,omitempty"`
	// TODO: Move this `multistream` field somewhere else to make this struct more
	// generic. Maybe condition dimensions/extraArgs?
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
	if values.Metrics != nil {
		new.Metrics = values.Metrics
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

func (s Status) MetricsCopy() []*Metric {
	metrics := make([]*Metric, len(s.Metrics))
	copy(metrics, s.Metrics)
	return metrics
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
	Type               ConditionType        `json:"type,omitempty"`
	Status             *bool                `json:"status"`
	Frequency          stats.ByWindow       `json:"frequency,omitempty"`
	LastProbeTime      *data.UnixMillisTime `json:"lastProbeTime"`
	LastTransitionTime *data.UnixMillisTime `json:"lastTransitionsTime"`
}

func NewCondition(condType ConditionType, ts time.Time, status *bool, frequency stats.ByWindow, last *Condition) *Condition {
	cond := &Condition{Type: condType}
	if last != nil && last.Type == condType {
		*cond = *last
	}
	if status != nil {
		cond.LastProbeTime = &data.UnixMillisTime{Time: ts}
		if cond.Status == nil || *status != *cond.Status {
			cond.LastTransitionTime = cond.LastProbeTime
		}
		cond.Status = status
	}
	if frequency != nil {
		cond.Frequency = frequency
	}
	return cond
}

type MetricName string

type Metric struct {
	Name       MetricName        `json:"name"`
	Dimensions map[string]string `json:"dimensions"`
	Last       Measure           `json:"last"`
	Stats      *MetricStats      `json:"stats,omitempty"`
}

func (m *Metric) Matches(name MetricName, odim map[string]string) bool {
	if name != m.Name || len(odim) != len(m.Dimensions) {
		return false
	}
	for k, v := range m.Dimensions {
		if odim[k] != v {
			return false
		}
	}
	return true
}

type MetricStats struct {
	Windows []stats.Window `json:"windows,omitempty"`
	Count   []int64        `json:"count"`
	Sum     []float64      `json:"sum"`
	Min     []float64      `json:"min"`
	Max     []float64      `json:"max"`
}

type Measure struct {
	Timestamp data.UnixMillisTime
	Value     float64
}

func (m Measure) MarshalJSON() ([]byte, error) {
	return json.Marshal([]interface{}{m.Timestamp.UnixMillis(), m.Value})
}

func (m *Measure) UnmarshalJSON(raw []byte) error {
	var arr []float64
	if err := json.Unmarshal(raw, &arr); err != nil {
		return err
	} else if len(arr) != 2 {
		return fmt.Errorf("invalid measure slice (%v), must have exactly 2 elements", arr)
	}
	m.Timestamp, m.Value = data.NewUnixMillisTime(int64(arr[0])), arr[1]
	return nil
}

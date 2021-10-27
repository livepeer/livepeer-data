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
	ID          string               `json:"id"`
	Healthy     *Condition           `json:"healthy"`
	Conditions  []*Condition         `json:"conditions"`
	Metrics     MetricsMap           `json:"metrics,omitempty"`
	Multistream []*MultistreamStatus `json:"multistream,omitempty"`
}

type MultistreamStatus struct {
	Target    data.MultistreamTargetInfo `json:"target"`
	Connected *Condition                 `json:"connected"`
}

type ConditionType string

type Condition struct {
	Type               ConditionType        `json:"type,omitempty"`
	Status             *bool                `json:"status"`
	ExtraData          interface{}          `json:"extraData,omitempty"`
	Frequency          stats.ByWindow       `json:"frequency,omitempty"`
	LastProbeTime      *data.UnixMillisTime `json:"lastProbeTime"`
	LastTransitionTime *data.UnixMillisTime `json:"lastTransitionsTime"`
}

type MetricName string

type MetricsMap map[MetricName][]*Metric

type Metric struct {
	Name       MetricName        `json:"name"`
	Dimensions map[string]string `json:"dimensions,omitempty"`
	Last       Measure           `json:"last"`
	// TODO: Implement some historic `stats` here like for conditions. Or maybe
	// remove the stats from conditions if we end up never needing them.
}

type Measure struct {
	Timestamp time.Time
	Value     float64
}

// Here be functions

// Status

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

func (s *Status) Condition(typ ConditionType) *Condition {
	for _, cond := range s.Conditions {
		if cond.Type == typ {
			return cond
		}
	}
	return nil
}

func (s *Status) ConditionsCopy() []*Condition {
	conditions := make([]*Condition, len(s.Conditions))
	copy(conditions, s.Conditions)
	return conditions
}

func (s *Status) MultistreamCopy() []*MultistreamStatus {
	multistream := make([]*MultistreamStatus, len(s.Multistream))
	copy(multistream, s.Multistream)
	return multistream
}

func (s *Status) MetricsCopy() MetricsMap {
	metrics := make(MetricsMap, len(s.Metrics))
	for k, v := range s.Metrics {
		metrics[k] = v
	}
	return metrics
}

// Condition

func NewCondition(condType ConditionType, ts time.Time, status *bool, last *Condition) *Condition {
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
	return cond
}

// MetricsMap

func (m MetricsMap) GetMetric(name MetricName, dimensions map[string]string) *Metric {
	for _, metric := range m[name] {
		if metric.Matches(name, dimensions) {
			return metric
		}
	}
	return nil
}

func (m MetricsMap) Add(metric *Metric) MetricsMap {
	prev := m[metric.Name]
	new := make([]*Metric, len(prev))
	copy(new, prev)
	m[metric.Name] = replaceOrAddMetric(new, *metric)
	return m
}

func replaceOrAddMetric(metrics []*Metric, new Metric) []*Metric {
	for i, metric := range metrics {
		if metric.Matches(new.Name, new.Dimensions) {
			metrics[i] = &new
			return metrics
		}
	}
	return append(metrics, &new)
}

// Metric

func NewMetric(name MetricName, dimensions map[string]string, ts time.Time, value float64) *Metric {
	return &Metric{
		Name:       name,
		Dimensions: dimensions,
		Last:       Measure{ts, value},
	}
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

// Measure

func (m Measure) MarshalJSON() ([]byte, error) {
	return json.Marshal([]interface{}{data.UnixMillisTime{Time: m.Timestamp}, m.Value})
}

func (m *Measure) UnmarshalJSON(raw []byte) error {
	var arr []float64
	if err := json.Unmarshal(raw, &arr); err != nil {
		return err
	} else if len(arr) != 2 {
		return fmt.Errorf("invalid measure slice (%v), must have exactly 2 elements", arr)
	}
	millisTs := data.NewUnixMillisTime(int64(arr[0]))
	m.Timestamp, m.Value = millisTs.Time, arr[1]
	return nil
}

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/healthy-streams/event"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const streamUri = "rabbitmq-stream://guest:guest@localhost:5552/livepeer"
const exchange = "lp_golivepeer_metadata"
const binding = "#.stream_health.transcode.#"

var streamName = "sq_stream_health_v0"

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()
	glog.Info("Hello")

	// Set log level, not mandatory by default is INFO
	stream.SetLevelInfo(logs.DEBUG)

	glog.Info("Getting started with Streaming client for RabbitMQ")
	glog.Info("Connecting to RabbitMQ streaming...")

	// Connect to the broker ( or brokers )
	consumer, err := event.NewStreamConsumer(streamUri, "")
	CheckErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startOffset := time.Now().Add(-5*time.Minute).UnixNano() / 1e6 // start consuming from 5 mins ago
	msgs, err := consumer.Consume(ctx, streamName, event.ConsumeOptions{
		StreamOptions: &event.StreamOptions{
			StreamOptions: stream.StreamOptions{
				MaxLengthBytes:      event.ByteCapacity.GB(1),
				MaxSegmentSizeBytes: event.ByteCapacity.KB(5), // should be like 500MB in production
				MaxAge:              1 * time.Hour,
			},
			Bindings: []event.BindingArgs{
				{Key: binding, Exchange: exchange},
			},
		},
		ConsumerOptions: stream.NewConsumerOptions().
			SetConsumerName("my_consumer"). // set a consumer name
			SetOffset(event.OffsetSpec.Timestamp(startOffset)),
		MemorizeOffset: true,
	})
	CheckErr(err)

	streamHealths := sync.Map{}
	go func() {
		for msg := range msgs {
			// json, err := json.Marshal(message.Properties)
			var evt *StreamHealthTranscodeEvent
			err := json.Unmarshal(msg.Data[0], &evt)
			CheckErr(err)

			if rand.Intn(100) < 100 {
				evt.Success = rand.Intn(100) < 98
			}

			glog.Infof("received message. consumer=%q, offset=%d, seqNo=%d, startTimeAge=%q, latency=%q, success=%v",
				msg.Consumer.GetName(), msg.Consumer.GetOffset(), evt.Segment.SeqNo,
				time.Since(time.Unix(0, evt.StartTime)), time.Duration(evt.LatencyMs)*time.Millisecond, evt.Success)

			mid := evt.ManifestID
			var health *StreamHealthContext
			if saved, ok := streamHealths.Load(mid); ok {
				health = saved.(*StreamHealthContext)
			} else {
				health = NewStreamHealthContext(mid, Transcoding, RealTime, NoErrors)
				streamHealths.Store(mid, health)
			}
			health.LastStatus = ReduceStreamHealth(health, evt)
		}
	}()

	srv := &http.Server{Addr: ":8080"}
	go func() {
		defer cancel()
		glog.Infoln("Stream name", streamName)
		glog.Infoln("Press any key to stop")
		bufio.NewReader(os.Stdin).ReadString('\n')

		shutx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()
		CheckErr(srv.Shutdown(shutx))
	}()

	http.HandleFunc("/api/stream/health/", func(rw http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(r.URL.Path, "/", 6)
		if len(parts) != 5 {
			rw.WriteHeader(http.StatusBadRequest)
			return
		}
		manifestID := parts[4]
		healthIface, ok := streamHealths.Load(manifestID)
		if !ok {
			rw.WriteHeader(http.StatusNotFound)
			return
		}
		health := healthIface.(*StreamHealthContext)
		rw.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(rw).Encode(health.LastStatus); err != nil {
			glog.Errorf("Error writing stream health JSON response. err=%q", err)
		}
	})

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		CheckErr(err)
	}
}

func ReduceStreamHealth(healthCtx *StreamHealthContext, evt *StreamHealthTranscodeEvent) *StreamHealthStatus {
	ts := time.Unix(0, evt.StartTime).Add(time.Duration(evt.LatencyMs)).UTC()
	newStatus := &StreamHealthStatus{ManifestID: evt.ManifestID}
	for _, condType := range healthCtx.Conditions {
		newCond := NewHealthCondition(condType, ts,
			conditionStatus(evt, condType),
			healthCtx.LastStatus.getCondition(condType))
		newStatus.Conditions = append(newStatus.Conditions, newCond)
	}
	healthyMustsCount := 0
	for _, cond := range newStatus.Conditions {
		if healthyMustHaves[cond.Type] && cond.Status != nil && *cond.Status {
			healthyMustsCount++
		}
	}
	isHealthy := healthyMustsCount == len(healthyMustHaves)

	var last *HealthCondition
	if healthCtx.LastStatus != nil {
		last = &healthCtx.LastStatus.Healthy
	}
	newStatus.Healthy = *NewHealthCondition("", ts, &isHealthy, last)
	return newStatus
}

func conditionStatus(evt *StreamHealthTranscodeEvent, condType HealthConditionType) *bool {
	switch condType {
	case Transcoding:
		return &evt.Success
	case RealTime:
		isRealTime := evt.LatencyMs < int64(evt.Segment.Duration*1000)
		return &isRealTime
	case NoErrors:
		noErrors := true
		for _, attempt := range evt.Attempts {
			noErrors = noErrors && attempt.Error == nil
		}
		return &noErrors
	default:
		return nil
	}
}

func CheckErr(err error) {
	if err != nil {
		glog.Fatalln("error", err)
	}
}

type OrchestratorMetadata struct {
	Address       string `json:"address"`
	TranscoderUri string `json:"transcodeUri"`
}

type TranscodeAttemptInfo struct {
	Orchestrator OrchestratorMetadata `json:"orchestrator"`
	LatencyMs    int64                `json:"latencyMs"`
	Error        *string              `json:"error"`
}

type SegmentMetadata struct {
	Name     string  `json:"name"`
	SeqNo    uint64  `json:"seqNo"`
	Duration float64 `json:"duration"`
}

type StreamHealthTranscodeEvent struct {
	NodeID     string                 `json:"nodeId"`
	ManifestID string                 `json:"manifestId"`
	Segment    SegmentMetadata        `json:"segment"`
	StartTime  int64                  `json:"startTime"`
	LatencyMs  int64                  `json:"latencyMs"`
	Success    bool                   `json:"success"`
	Attempts   []TranscodeAttemptInfo `json:"attempts"`
}

type HealthConditionType string

const (
	Transcoding HealthConditionType = "Transcoding"
	RealTime    HealthConditionType = "RealTime"
	NoErrors    HealthConditionType = "NoErrors"
)

type measure struct {
	timestamp time.Time
	value     float64
}

type StatsAggregator struct {
	measures []measure
	sum      float64
}

func (a *StatsAggregator) Add(ts time.Time, value float64) *StatsAggregator {
	a.sum += value
	insertIdx := len(a.measures)
	for insertIdx > 0 && ts.Before(a.measures[insertIdx-1].timestamp) {
		insertIdx--
	}
	a.measures = insertMeasure(a.measures, insertIdx, measure{ts, value})
	return a
}

func insertMeasure(slc []measure, idx int, val measure) []measure {
	slc = append(slc, measure{})
	copy(slc[idx+1:], slc[idx:])
	slc[idx] = val
	return slc
}

func (a *StatsAggregator) Clip(window time.Duration) *StatsAggregator {
	threshold := a.measures[len(a.measures)-1].timestamp.Add(-window)
	for len(a.measures) > 0 && !threshold.Before(a.measures[0].timestamp) {
		a.sum -= a.measures[0].value
		a.measures = a.measures[1:]
	}
	return a
}

func (a StatsAggregator) Average() float64 {
	if len(a.measures) == 0 {
		return 0
	}
	return a.sum / float64(len(a.measures))
}

type TimedFrequency struct {
	Past map[string]float64

	stats [3]StatsAggregator
}

func (f *TimedFrequency) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.Past)
}

func (f *TimedFrequency) Update(ts time.Time, value bool) {
	measure := float64(0)
	if value {
		measure = 1
	}
	f.setPast(0, ts, measure, 1*time.Minute)
	f.setPast(1, ts, measure, 10*time.Minute)
	f.setPast(2, ts, measure, 1*time.Hour)
}

func (f *TimedFrequency) setPast(statsIdx int, ts time.Time, measure float64, window time.Duration) {
	if f.Past == nil {
		f.Past = map[string]float64{}
	}
	f.Past[fmt.Sprintf("%gm", window.Minutes())] = f.stats[statsIdx].
		Add(ts, measure).
		Clip(window).
		Average()
}

type HealthCondition struct {
	Type               HealthConditionType `json:"Type,omitempty"`
	Status             *bool
	Frequency          TimedFrequency
	LastProbeTime      time.Time
	LastTransitionTime time.Time
}

func NewHealthCondition(condType HealthConditionType, ts time.Time, status *bool, last *HealthCondition) *HealthCondition {
	cond := &HealthCondition{
		Type:               condType,
		Status:             status,
		LastProbeTime:      ts,
		LastTransitionTime: ts,
	}
	if last != nil && boolPtrsEq(last.Status, status) {
		cond.LastTransitionTime = last.LastTransitionTime
	}
	return cond
}

func boolPtrsEq(b1, b2 *bool) bool {
	if nil1, nil2 := b1 == nil, b2 == nil; nil1 && nil2 {
		return true
	} else if nil1 != nil2 {
		return false
	}
	return *b1 == *b2
}

var healthyMustHaves = map[HealthConditionType]bool{Transcoding: true, RealTime: true}

type StreamHealthStatus struct {
	ManifestID string
	Healthy    HealthCondition
	Conditions []*HealthCondition
}

func (s *StreamHealthStatus) getCondition(condType HealthConditionType) *HealthCondition {
	if s == nil {
		return nil
	}
	for _, cond := range s.Conditions {
		if cond.Type == condType {
			return cond
		}
	}
	return nil
}

type StreamHealthContext struct {
	ManifestID string
	Conditions []HealthConditionType

	PastEvents     []StreamHealthTranscodeEvent
	HealthStats    map[time.Duration]StatsAggregator
	ConditionStats map[HealthConditionType]map[time.Duration]StatsAggregator

	LastStatus *StreamHealthStatus
}

func NewStreamHealthContext(mid string, conditions ...HealthConditionType) *StreamHealthContext {
	ctx := &StreamHealthContext{
		HealthStats:    map[time.Duration]StatsAggregator{},
		Conditions:     conditions,
		ConditionStats: map[HealthConditionType]map[time.Duration]StatsAggregator{},
	}
	for _, cond := range conditions {
		ctx.ConditionStats[cond] = map[time.Duration]StatsAggregator{}
	}
	return ctx
}

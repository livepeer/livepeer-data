package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/healthy-streams/event"
	"github.com/livepeer/healthy-streams/health"
	"github.com/livepeer/healthy-streams/stats"
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
			var evt *health.TranscodeEvent
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
				health = NewStreamHealthContext(mid,
					[]HealthConditionType{Transcoding, RealTime, NoErrors},
					[]time.Duration{1 * time.Minute, 10 * time.Minute, 1 * time.Hour},
				)
				streamHealths.Store(mid, health)
			}
			health.LastStatus = ReduceStreamHealth(health, evt)
			health.PastEvents = append(health.PastEvents, evt) // TODO: crop/drop these at some point
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

func ReduceStreamHealth(healthCtx *StreamHealthContext, evt *health.TranscodeEvent) StreamHealthStatus {
	ts := time.Unix(0, evt.StartTime).Add(time.Duration(evt.LatencyMs)).UTC()

	conditions := make([]*HealthCondition, len(healthCtx.Conditions))
	for i, condType := range healthCtx.Conditions {
		status := conditionStatus(evt, condType)
		stats := healthCtx.ConditionStats[condType].Averages(healthCtx.StatsWindows, ts, status)

		last := healthCtx.LastStatus.getCondition(condType)
		conditions[i] = NewHealthCondition(condType, ts, status, stats, last)
	}

	return StreamHealthStatus{
		ManifestID: evt.ManifestID,
		Healthy:    diagnoseStream(healthCtx, conditions, ts),
		Conditions: conditions,
	}
}

func diagnoseStream(healthCtx *StreamHealthContext, currConditions []*HealthCondition, ts time.Time) HealthCondition {
	healthyMustsCount := 0
	for _, cond := range currConditions {
		if healthyMustHaves[cond.Type] && cond.Status != nil && *cond.Status {
			healthyMustsCount++
		}
	}
	isHealthy := healthyMustsCount == len(healthyMustHaves)
	healthStats := healthCtx.HealthStats.Averages(healthCtx.StatsWindows, ts, &isHealthy)

	last := &healthCtx.LastStatus.Healthy
	return *NewHealthCondition("", ts, &isHealthy, healthStats, last)
}

func conditionStatus(evt *health.TranscodeEvent, condType HealthConditionType) *bool {
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

type HealthConditionType string

const (
	Transcoding HealthConditionType = "Transcoding"
	RealTime    HealthConditionType = "RealTime"
	NoErrors    HealthConditionType = "NoErrors"
)

type HealthCondition struct {
	Type               HealthConditionType `json:"type,omitempty"`
	Status             *bool               `json:"status"`
	Frequency          stats.ByWindow      `json:"frequency,omitempty"`
	LastProbeTime      *time.Time          `json:"lastProbeTime"`
	LastTransitionTime *time.Time          `json:"lastTransitionsTime"`
}

func NewHealthCondition(condType HealthConditionType, ts time.Time, status *bool, frequency stats.ByWindow, last *HealthCondition) *HealthCondition {
	cond := &HealthCondition{Type: condType}
	if last != nil && last.Type == condType {
		*cond = *last
	}
	if status != nil {
		cond.LastProbeTime = &ts
		if !boolPtrsEq(status, cond.Status) {
			cond.LastTransitionTime = &ts
		}
		cond.Status = status
		cond.Frequency = frequency
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
	ManifestID string             `json:"manifestId"`
	Healthy    HealthCondition    `json:"healthy"`
	Conditions []*HealthCondition `json:"conditions"`
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
	ManifestID   string
	Conditions   []HealthConditionType
	StatsWindows []time.Duration

	PastEvents     []*health.TranscodeEvent
	HealthStats    stats.WindowAggregators
	ConditionStats map[HealthConditionType]stats.WindowAggregators

	LastStatus StreamHealthStatus
}

func NewStreamHealthContext(mid string, conditions []HealthConditionType, statsWindows []time.Duration) *StreamHealthContext {
	ctx := &StreamHealthContext{
		ManifestID:     mid,
		Conditions:     conditions,
		StatsWindows:   statsWindows,
		HealthStats:    stats.WindowAggregators{},
		ConditionStats: map[HealthConditionType]stats.WindowAggregators{},
		LastStatus: StreamHealthStatus{
			ManifestID: mid,
			Healthy:    *NewHealthCondition("", time.Time{}, nil, nil, nil),
			Conditions: make([]*HealthCondition, len(conditions)),
		},
	}
	for i, cond := range conditions {
		ctx.LastStatus.Conditions[i] = NewHealthCondition(cond, time.Time{}, nil, nil, nil)
		ctx.ConditionStats[cond] = stats.WindowAggregators{}
	}
	return ctx
}

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
			var healthCtx *StreamHealthContext
			if saved, ok := streamHealths.Load(mid); ok {
				healthCtx = saved.(*StreamHealthContext)
			} else {
				healthCtx = NewStreamHealthContext(mid, healthConditions, statsWindows)
				streamHealths.Store(mid, healthCtx)
			}
			healthCtx.LastStatus = ReduceStreamHealth(healthCtx, evt)
			healthCtx.PastEvents = append(healthCtx.PastEvents, evt) // TODO: crop/drop these at some point
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

var healthyMustHaves = map[health.ConditionType]bool{
	health.ConditionTranscoding: true,
	health.ConditionRealTime:    true,
}
var healthConditions = []health.ConditionType{health.ConditionTranscoding, health.ConditionRealTime, health.ConditionNoErrors}
var statsWindows = []time.Duration{1 * time.Minute, 10 * time.Minute, 1 * time.Hour}

func ReduceStreamHealth(healthCtx *StreamHealthContext, evt *health.TranscodeEvent) health.Status {
	ts := time.Unix(0, evt.StartTime).Add(time.Duration(evt.LatencyMs)).UTC()

	conditions := make([]*health.Condition, len(healthCtx.Conditions))
	for i, condType := range healthCtx.Conditions {
		status := conditionStatus(evt, condType)
		stats := healthCtx.ConditionStats[condType].Averages(healthCtx.StatsWindows, ts, status)

		last := healthCtx.LastStatus.GetCondition(condType)
		conditions[i] = health.NewCondition(condType, ts, status, stats, last)
	}

	return health.Status{
		ManifestID: evt.ManifestID,
		Healthy:    diagnoseStream(healthCtx, conditions, ts),
		Conditions: conditions,
	}
}

func diagnoseStream(healthCtx *StreamHealthContext, currConditions []*health.Condition, ts time.Time) health.Condition {
	healthyMustsCount := 0
	for _, cond := range currConditions {
		if healthyMustHaves[cond.Type] && cond.Status != nil && *cond.Status {
			healthyMustsCount++
		}
	}
	isHealthy := healthyMustsCount == len(healthyMustHaves)
	healthStats := healthCtx.HealthStats.Averages(healthCtx.StatsWindows, ts, &isHealthy)

	last := &healthCtx.LastStatus.Healthy
	return *health.NewCondition("", ts, &isHealthy, healthStats, last)
}

func conditionStatus(evt *health.TranscodeEvent, condType health.ConditionType) *bool {
	switch condType {
	case health.ConditionTranscoding:
		return &evt.Success
	case health.ConditionRealTime:
		isRealTime := evt.LatencyMs < int64(evt.Segment.Duration*1000)
		return &isRealTime
	case health.ConditionNoErrors:
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

type StreamHealthContext struct {
	ManifestID   string
	Conditions   []health.ConditionType
	StatsWindows []time.Duration

	PastEvents     []*health.TranscodeEvent
	HealthStats    stats.WindowAggregators
	ConditionStats map[health.ConditionType]stats.WindowAggregators

	LastStatus health.Status
}

func NewStreamHealthContext(mid string, conditions []health.ConditionType, statsWindows []time.Duration) *StreamHealthContext {
	ctx := &StreamHealthContext{
		ManifestID:     mid,
		Conditions:     conditions,
		StatsWindows:   statsWindows,
		HealthStats:    stats.WindowAggregators{},
		ConditionStats: map[health.ConditionType]stats.WindowAggregators{},
		LastStatus: health.Status{
			ManifestID: mid,
			Healthy:    *health.NewCondition("", time.Time{}, nil, nil, nil),
			Conditions: make([]*health.Condition, len(conditions)),
		},
	}
	for i, cond := range conditions {
		ctx.LastStatus.Conditions[i] = health.NewCondition(cond, time.Time{}, nil, nil, nil)
		ctx.ConditionStats[cond] = stats.WindowAggregators{}
	}
	return ctx
}

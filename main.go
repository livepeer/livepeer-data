package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
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
			var evt StreamHealthTranscodeEvent
			err := json.Unmarshal(msg.Data[0], &evt)
			CheckErr(err)

			glog.Infof("received message. consumer=%q, offset=%d, seqNo=%d, startTimeAge=%q, latency=%q",
				msg.Consumer.GetName(), msg.Consumer.GetOffset(), evt.Segment.SeqNo,
				time.Since(time.Unix(0, evt.StartTime)), time.Duration(evt.LatencyMs)*time.Millisecond)

			mid := evt.ManifestID
			var health *StreamHealthStatus
			if saved, ok := streamHealths.Load(mid); ok {
				health = saved.(*StreamHealthStatus)
			} else {
				health = &StreamHealthStatus{
					ManifestID: mid,
					Conditions: []*HealthCondition{
						{Type: Transcoding},
						{Type: RealTime},
						{Type: NoErrors},
					},
				}
				streamHealths.Store(mid, health)
			}
			ts := time.Unix(0, evt.StartTime).Add(time.Duration(evt.LatencyMs)).UTC()
			for _, cond := range health.Conditions {
				switch cond.Type {
				case Transcoding:
					cond.Update(ts, evt.Success)
				case RealTime:
					cond.Update(ts, evt.LatencyMs < int64(evt.Segment.Duration*1000))
				case NoErrors:
					noErrors := true
					for _, attempt := range evt.Attempts {
						noErrors = noErrors && attempt.Error == nil
					}
					cond.Update(ts, noErrors)
				}
			}
			healthyMustsCount := 0
			for _, cond := range health.Conditions {
				if healthyMustHaves[cond.Type] && cond.Status != nil && *cond.Status {
					healthyMustsCount++
				}
			}
			health.Healthy.Update(ts, healthyMustsCount == len(healthyMustHaves))
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
		health := healthIface.(*StreamHealthStatus)
		rw.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(rw).Encode(health); err != nil {
			glog.Errorf("Error writing stream health JSON response. err=%q", err)
		}
	})

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		CheckErr(err)
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
	PastMinute    float64
	Past10Minutes float64
	PastHour      float64

	stats [3]StatsAggregator
}

func (f *TimedFrequency) Update(ts time.Time, value bool) {
	measure := float64(0)
	if value {
		measure = 1
	}
	for i := range f.stats {
		f.stats[i].Add(ts, measure)
	}
	f.PastMinute = f.stats[0].Clip(1 * time.Minute).Average()
	f.Past10Minutes = f.stats[1].Clip(10 * time.Minute).Average()
	f.PastHour = f.stats[2].Clip(1 * time.Hour).Average()
}

type HealthCondition struct {
	Type               HealthConditionType `json:"Type,omitempty"`
	Status             *bool
	Frequency          TimedFrequency
	LastProbeTime      time.Time
	LastTransitionTime time.Time
}

func (c *HealthCondition) Update(ts time.Time, status bool) {
	c.LastProbeTime = ts
	if c.Status == nil || *c.Status != status {
		c.LastTransitionTime = ts
		c.Status = &status
	}
	c.Frequency.Update(ts, status)
}

var healthyMustHaves = map[HealthConditionType]bool{Transcoding: true, RealTime: true}

type StreamHealthStatus struct {
	ManifestID string
	Healthy    HealthCondition
	Conditions []*HealthCondition
}

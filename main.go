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
			var health StreamHealthStatus
			if saved, ok := streamHealths.Load(mid); ok {
				health = saved.(StreamHealthStatus)
			} else {
				health = StreamHealthStatus{
					ManifestID: mid,
					Conditions: []*HealthCondition{
						{Type: Transcoding},
						{Type: RealTime},
						{Type: NoErrors},
					},
				}
			}
			ts := time.Unix(0, evt.StartTime).Add(time.Duration(evt.LatencyMs))
			for _, condition := range health.Conditions {
				switch condition.Type {
				case Transcoding:
					condition.Update(ts, evt.Success)
				case RealTime:
					condition.Update(ts, evt.LatencyMs < int64(evt.Segment.Duration*1000))
				case NoErrors:
					noErrors := true
					for _, attempt := range evt.Attempts {
						noErrors = noErrors && attempt.Error == nil
					}
					condition.Update(ts, noErrors)
				}
			}
			streamHealths.Store(mid, health)
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
		health := healthIface.(StreamHealthStatus)
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

// type StatusFrequency struct {
// 	LastMinute    float64
// 	Last10Minutes float64
// 	LastHour      float64
// }

type HealthCondition struct {
	Type   HealthConditionType
	Status *bool
	// Frequency          *StatusFrequency
	LastProbeTime      time.Time
	LastTransitionTime time.Time
}

func (c *HealthCondition) Update(now time.Time, status bool) {
	c.LastProbeTime = now
	if c.Status == nil || *c.Status != status {
		c.LastTransitionTime = now
		c.Status = &status
	}
}

type StreamHealthStatus struct {
	ManifestID string
	Conditions []*HealthCondition
}

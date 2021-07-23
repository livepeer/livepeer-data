package health

import (
	"encoding/json"
	"time"
)

// Events

type Event interface {
	ManifestID() string
	Timestamp() time.Time
}

func ParseEvent(data []byte) (Event, error) {
	var evt *TranscodeEvent
	err := json.Unmarshal(data, &evt)
	if err != nil {
		return nil, err
	}
	return evt, nil
}

type TranscodeEvent struct {
	NodeID      string                 `json:"nodeId"`
	ManifestID_ string                 `json:"manifestId"`
	Segment     SegmentMetadata        `json:"segment"`
	StartTime   int64                  `json:"startTime"`
	LatencyMs   int64                  `json:"latencyMs"`
	Success     bool                   `json:"success"`
	Attempts    []TranscodeAttemptInfo `json:"attempts"`
}

func (e *TranscodeEvent) ManifestID() string {
	return e.ManifestID_
}

func (e *TranscodeEvent) Timestamp() time.Time {
	// TODO: Send a "final" timestamp directly in the event to avoid this logic
	latency := time.Duration(e.LatencyMs) * time.Millisecond
	return time.Unix(0, e.StartTime).Add(latency).UTC()
}

// TranscodeEvent inner types

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

package health

import "time"

// Events

type Event interface {
	ManifestID() string
	Timestamp() time.Time
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

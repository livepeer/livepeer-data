package data

import (
	"time"
)

const EventTypeTranscode EventType = "transcode"

func NewTranscodeEvent(nodeID, mid string, seg SegmentMetadata, startTime time.Time, success bool, attempts []TranscodeAttemptInfo) *TranscodeEvent {
	base := newEventBase(EventTypeTranscode, mid)
	return &TranscodeEvent{
		Base:      base,
		NodeID:    nodeID,
		Segment:   seg,
		StartTime: UnixNanoTime{startTime},
		LatencyMs: base.Timestamp_.Sub(startTime).Milliseconds(),
		Success:   success,
		Attempts:  attempts,
	}
}

type TranscodeEvent struct {
	Base
	NodeID    string                 `json:"nodeId"`
	Segment   SegmentMetadata        `json:"segment"`
	StartTime UnixNanoTime           `json:"startTime"`
	LatencyMs int64                  `json:"latencyMs"`
	Success   bool                   `json:"success"`
	Attempts  []TranscodeAttemptInfo `json:"attempts"`
}

type SegmentMetadata struct {
	Name     string  `json:"name"`
	SeqNo    uint64  `json:"seqNo"`
	Duration float64 `json:"duration"`
	ByteSize int     `json:"byteSize"`
}

type TranscodeAttemptInfo struct {
	Orchestrator OrchestratorMetadata `json:"orchestrator"`
	LatencyMs    int64                `json:"latencyMs"`
	Error        *string              `json:"error"`
}

type OrchestratorMetadata struct {
	Address       string `json:"address"`
	TranscoderUri string `json:"transcodeUri"`
}

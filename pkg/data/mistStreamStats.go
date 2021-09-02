package data

const EventTypeMistStreamStats EventType = "mist_stream_stats"

func NewMistStreamStatsEvent(nodeID, streamID string, viewerCount int, mediaTimeMs int64, multistream []MultistreamTargetStats) (*MistStreamStatsEvent, error) {
	return &MistStreamStatsEvent{
		Base:        newEventBase(EventTypeMistStreamStats, streamID),
		NodeID:      nodeID,
		ViewerCount: viewerCount,
		MediaTimeMs: mediaTimeMs,
		Multistream: multistream,
	}, nil
}

type MistStreamStatsEvent struct {
	Base
	NodeID string `json:"nodeId"`

	ViewerCount int   `json:"viewerCount"`
	MediaTimeMs int64 `json:"mediaTimeMs"`

	Multistream []MultistreamTargetStats
}

type MultistreamTargetStats struct {
	Target MultistreamTargetInfo `json:"target"`

	ActiveSec   int64 `json:"activeSec"`
	Bytes       int64 `json:"bytes"`
	MediaTimeMs int64 `json:"mediaTimeMs"`
}

package data

const EventTypeMistStreamStats EventType = "mist_stream_stats"

func NewMistStreamStatsEvent(nodeID, streamID string, viewerCount int, mediaTimeMs int64, multistream []*MultistreamTargetStats) *MistStreamStatsEvent {
	return &MistStreamStatsEvent{
		Base:        newEventBase(EventTypeMistStreamStats, streamID),
		NodeID:      nodeID,
		ViewerCount: viewerCount,
		MediaTimeMs: mediaTimeMs,
		Multistream: multistream,
	}
}

type MistStreamStatsEvent struct {
	Base
	NodeID string `json:"nodeId"`

	ViewerCount int   `json:"viewerCount"`
	MediaTimeMs int64 `json:"mediaTimeMs"`

	Multistream []*MultistreamTargetStats
}

type MultistreamTargetStats struct {
	Target MultistreamTargetInfo `json:"target"`
	Stats  *MultistreamStats     `json:"stats"`
}

type MultistreamStats struct {
	ActiveSec   int64 `json:"activeSec"`
	Bytes       int64 `json:"bytes"`
	MediaTimeMs int64 `json:"mediaTimeMs"`
}

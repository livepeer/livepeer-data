package data

const EventTypeMistStreamStats EventType = "mist_stream_stats"

func NewMistStreamStatsEvent(nodeID, streamID string, stats *StreamStats, multistream []*MultistreamTargetStats) *MistStreamStatsEvent {
	return &MistStreamStatsEvent{
		Base:        newEventBase(EventTypeMistStreamStats, streamID),
		NodeID:      nodeID,
		Stats:       stats,
		Multistream: multistream,
	}
}

type MistStreamStatsEvent struct {
	Base
	NodeID      string `json:"nodeId"`
	Stats       *StreamStats
	Multistream []*MultistreamTargetStats
}

type StreamStats struct {
	ViewerCount int    `json:"viewerCount"`
	MediaTimeMs *int64 `json:"mediaTimeMs"`
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

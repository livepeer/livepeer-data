package data

const EventTypeMediaServerStreamStats EventType = "media_server_stream_stats"

func NewMediaServerStreamStatsEvent(nodeID, streamID string, viewerCount int, mediaTimeMs int64, multistream []MultistreamTargetStats) (*MediaServerStreamStatsEvent, error) {
	return &MediaServerStreamStatsEvent{
		Base:        newEventBase(EventTypeMediaServerStreamStats, streamID),
		NodeID:      nodeID,
		ViewerCount: viewerCount,
		MediaTimeMs: mediaTimeMs,
		Multistream: multistream,
	}, nil
}

type MediaServerStreamStatsEvent struct {
	Base
	NodeID string `json:"nodeId"`

	ViewerCount int   `json:"viewerCount"`
	MediaTimeMs int64 `json:"mediaTimeMs"`

	Multistream []MultistreamTargetStats
}

type MultistreamTargetStats struct {
	Target MultistreamTargetInfo `json:"target"`

	ActiveSeconds int64 `json:"activeSeconds"`
	Bytes         int64 `json:"bytes"`
	MediaTimeMs   int64 `json:"mediaTimeMs"`
}

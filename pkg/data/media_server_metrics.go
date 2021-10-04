package data

const EventTypeMediaServerMetrics EventType = "media_server_metrics"

func NewMediaServerMetricsEvent(nodeID, streamID string, stats *StreamMetrics, multistream []*MultistreamTargetMetrics) *MediaServerMetricsEvent {
	return &MediaServerMetricsEvent{
		Base:        newEventBase(EventTypeMediaServerMetrics, streamID),
		NodeID:      nodeID,
		Stats:       stats,
		Multistream: multistream,
	}
}

type MediaServerMetricsEvent struct {
	Base
	NodeID      string `json:"nodeId"`
	Stats       *StreamMetrics
	Multistream []*MultistreamTargetMetrics
}

type StreamMetrics struct {
	ViewerCount int    `json:"viewerCount"`
	MediaTimeMs *int64 `json:"mediaTimeMs"`
}

type MultistreamTargetMetrics struct {
	Target  MultistreamTargetInfo `json:"target"`
	Metrics *MultistreamMetrics   `json:"metrics"`
}

type MultistreamMetrics struct {
	ActiveSec   int64 `json:"activeSec"`
	Bytes       int64 `json:"bytes"`
	MediaTimeMs int64 `json:"mediaTimeMs"`
}

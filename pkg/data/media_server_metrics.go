package data

const EventTypeMediaServerMetrics EventType = "media_server_metrics"

func NewMediaServerMetricsEvent(nodeID, region, streamID string, stats *StreamMetrics, multistream []*MultistreamTargetMetrics) *MediaServerMetricsEvent {
	return &MediaServerMetricsEvent{
		Base:        newEventBase(EventTypeMediaServerMetrics, streamID),
		NodeID:      nodeID,
		Region:      region,
		Stats:       stats,
		Multistream: multistream,
	}
}

type MediaServerMetricsEvent struct {
	Base
	NodeID      string                      `json:"nodeId"`
	Region      string                      `json:"region,omitempty"`
	Stats       *StreamMetrics              `json:"stats"`
	Multistream []*MultistreamTargetMetrics `json:"multistream"`
}

type StreamMetrics struct {
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

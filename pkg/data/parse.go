package data

import (
	"encoding/json"
	"fmt"
)

func ParseEvent(data []byte) (Event, error) {
	var base Base
	if err := json.Unmarshal(data, &base); err != nil {
		return nil, fmt.Errorf("error unmarshalling base event: %w", err)
	}
	switch base.Type() {
	case EventTypeTranscode:
		var trans *TranscodeEvent
		if err := json.Unmarshal(data, &trans); err != nil {
			return nil, fmt.Errorf("error unmarshalling transcode event: %w", err)
		}
		return trans, nil
	case EventTypeStreamState:
		var strst *StreamStateEvent
		if err := json.Unmarshal(data, &strst); err != nil {
			return nil, fmt.Errorf("error unmarshalling stream state event: %w", err)
		}
		return strst, nil
	case EventTypeWebhook:
		var hook *WebhookEvent
		if err := json.Unmarshal(data, &hook); err != nil {
			return nil, fmt.Errorf("error unmarshalling webhook event: %w", err)
		}
		return hook, nil
	case EventTypeMediaServerMetrics:
		var mesemev *MediaServerMetricsEvent
		if err := json.Unmarshal(data, &mesemev); err != nil {
			return nil, fmt.Errorf("error unmarshalling media server metrics event: %w", err)
		}
		return mesemev, nil
	default:
		return nil, fmt.Errorf("unknown event type=%q, streamId=%q, ts=%d", base.Type(), base.StreamID_, base.Timestamp_)
	}
}

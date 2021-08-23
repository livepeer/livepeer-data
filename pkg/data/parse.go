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
	switch base.Type {
	case EventTypeTranscode:
		var trans *TranscodeEvent
		if err := json.Unmarshal(data, &trans); err != nil {
			return nil, fmt.Errorf("error unmarshalling transcode event: %w", err)
		}
		return trans, nil
	case EventTypeWebhook:
		var hook *WebhookEvent
		if err := json.Unmarshal(data, &hook); err != nil {
			return nil, fmt.Errorf("error unmarshalling webhook event: %w", err)
		}
		return hook, nil
	default:
		return nil, fmt.Errorf("unknown event type=%q, mid=%q, ts=%d", base.Type, base.ManifestID_, base.Timestamp_)
	}
}

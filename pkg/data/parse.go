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
	case EventTypeTaskTrigger:
		var ttev *TaskTriggerEvent
		if err := json.Unmarshal(data, &ttev); err != nil {
			return nil, fmt.Errorf("error unmarshalling task trigger event: %w", err)
		}
		return ttev, nil
	case EventTypeTaskResult:
		var trev *TaskResultEvent
		if err := json.Unmarshal(data, &trev); err != nil {
			return nil, fmt.Errorf("error unmarshalling task result event: %w", err)
		}
		return trev, nil
	case EventTypeTaskResultPartial:
		var event *TaskResultPartialEvent
		if err := json.Unmarshal(data, &event); err != nil {
			return nil, fmt.Errorf("error unmarshalling task partial result event: %w", err)
		}
		return event, nil
	default:
		return nil, fmt.Errorf("unknown event type=%q, streamId=%q, ts=%v", base.Type(), base.StreamID_, base.Timestamp_)
	}
}

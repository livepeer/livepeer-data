package data

import (
	"encoding/json"
	"fmt"
)

const EventTypeWebhook EventType = "webhook_event"

func NewWebhookEvent(mid, event, userID, streamID, sessionID string, payload interface{}) (*WebhookEvent, error) {
	var rawPayload []byte
	var err error
	if payload != nil {
		rawPayload, err = json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("error marshalling webhook event payload: %w", err)
		}
	}
	return &WebhookEvent{
		Base:      newEventBase(EventTypeWebhook, mid),
		Event:     event,
		UserID:    userID,
		StreamID:  streamID,
		SessionID: sessionID,
		Payload:   rawPayload,
	}, nil
}

type WebhookEvent struct {
	Base
	Event     string          `json:"event"`
	UserID    string          `json:"userId"`
	StreamID  string          `json:"streamId"`
	SessionID string          `json:"sessionId,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type MultistreamWebhookPayload struct {
	Target MultistreamTargetInfo `json:"target"`
}

type MultistreamTargetInfo struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Profile string `json:"profile"`
}

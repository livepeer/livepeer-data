package data

import (
	"encoding/json"
	"fmt"
)

const EventTypeWebhook EventType = "webhook_event"

func NewWebhookEvent(streamID, event, userID, sessionID string, payload interface{}) (*WebhookEvent, error) {
	var rawPayload []byte
	var err error
	if payload != nil {
		rawPayload, err = json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("error marshalling webhook event payload: %w", err)
		}
	}
	return &WebhookEvent{
		Base:      newEventBase(EventTypeWebhook, streamID),
		Event:     event,
		UserID:    userID,
		SessionID: sessionID,
		Payload:   rawPayload,
	}, nil
}

type WebhookEvent struct {
	Base
	Event     string          `json:"event"`
	UserID    string          `json:"userId"`
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

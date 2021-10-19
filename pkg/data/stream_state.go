package data

const EventTypeStreamState EventType = "stream_state"

func NewStreamStateEvent(streamID, userID, sessionID string, state StreamState) *StreamStateEvent {
	return &StreamStateEvent{
		Base:      newEventBase(EventTypeStreamState, streamID),
		UserID:    userID,
		SessionID: sessionID,
		State:     state,
	}
}

type StreamStateEvent struct {
	Base
	UserID    string      `json:"userId"`
	SessionID string      `json:"sessionId,omitempty"`
	State     StreamState `json:"state"`
}

type StreamState struct {
	Active bool `json:"active"`
}

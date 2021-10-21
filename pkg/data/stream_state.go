package data

const EventTypeStreamState EventType = "stream_state"

func NewStreamStateEvent(streamID, userID, region string, state StreamState) *StreamStateEvent {
	return &StreamStateEvent{
		Base:   newEventBase(EventTypeStreamState, streamID),
		UserID: userID,
		Region: region,
		State:  state,
	}
}

type StreamStateEvent struct {
	Base
	UserID string      `json:"userId"`
	Region string      `json:"region,omitempty"`
	State  StreamState `json:"state"`
}

type StreamState struct {
	Active bool `json:"active"`
}

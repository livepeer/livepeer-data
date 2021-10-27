package data

const EventTypeStreamState EventType = "stream_state"

func NewStreamStateEvent(nodeID, region, userID, streamID string, state StreamState) *StreamStateEvent {
	return &StreamStateEvent{
		Base:   newEventBase(EventTypeStreamState, streamID),
		NodeID: nodeID,
		Region: region,
		UserID: userID,
		State:  state,
	}
}

type StreamStateEvent struct {
	Base
	NodeID string      `json:"nodeId"`
	Region string      `json:"region,omitempty"`
	UserID string      `json:"userId"`
	State  StreamState `json:"state"`
}

type StreamState struct {
	Active bool `json:"active"`
}

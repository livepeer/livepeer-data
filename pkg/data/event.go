package data

import (
	"time"

	"github.com/google/uuid"
)

// TODO: Call these just Message for consistency with livepeer-com
type Event interface {
	Type() EventType
	ID() uuid.UUID
	Timestamp() time.Time
	StreamID() string
}

type EventType string

type Base struct {
	Type_      EventType      `json:"type"`
	ID_        uuid.UUID      `json:"id"`
	Timestamp_ UnixMillisTime `json:"timestamp"`
	StreamID_  string         `json:"streamId"`
}

var _ Event = (*Base)(nil)

func newEventBase(type_ EventType, streamID string) Base {
	return Base{
		Type_:      type_,
		ID_:        uuid.New(),
		Timestamp_: UnixMillisTime{time.Now().UTC()},
		StreamID_:  streamID,
	}
}

func (b *Base) Type() EventType {
	return b.Type_
}

func (b *Base) ID() uuid.UUID {
	return b.ID_
}

func (b *Base) StreamID() string {
	return b.StreamID_
}

func (b *Base) Timestamp() time.Time {
	return b.Timestamp_.Time
}

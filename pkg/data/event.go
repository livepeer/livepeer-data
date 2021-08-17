package data

import (
	"time"

	"github.com/google/uuid"
)

// TODO: Call these just Message for consistency with livepeer-com
type Event interface {
	ID() uuid.UUID
	Timestamp() time.Time
	ManifestID() string
}

type EventType string

type Base struct {
	Type        EventType      `json:"type"`
	ID_         uuid.UUID      `json:"id"`
	Timestamp_  UnixMillisTime `json:"timestamp"`
	ManifestID_ string         `json:"manifestId"`
}

var _ Event = (*Base)(nil)

func newEventBase(type_ EventType, mid string) Base {
	return Base{
		Type:        type_,
		ID_:         uuid.New(),
		Timestamp_:  UnixMillisTime{time.Now().UTC()},
		ManifestID_: mid,
	}
}

func (b *Base) ID() uuid.UUID {
	return b.ID_
}

func (b *Base) ManifestID() string {
	return b.ManifestID_
}

func (b *Base) Timestamp() time.Time {
	return b.Timestamp_.Time
}

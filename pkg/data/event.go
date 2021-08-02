package data

import "time"

type Event interface {
	Timestamp() time.Time
	ManifestID() string
}

type Base struct {
	Type        string `json:"type"`
	Timestamp_  int64  `json:"timestamp"`
	ManifestID_ string `json:"manifestId"`
}

func (b *Base) ManifestID() string {
	return b.ManifestID_
}

func (b *Base) Timestamp() time.Time {
	return time.Unix(0, b.Timestamp_).UTC()
}

package ai

import (
	"time"
)

type QueryFilter struct {
	StreamID string
}

type QuerySpec struct {
	From, To *time.Time
	Filter   QueryFilter
}

func NewQuerySpec(streamID string, from, to *time.Time) QuerySpec {
	return QuerySpec{
		From: from,
		To:   to,
		Filter: QueryFilter{
			StreamID: streamID,
		},
	}
}

package usage

import (
	"fmt"
	"strings"
	"time"
)

type FromToQuerySpec struct {
	From, To *time.Time
}

type QueryFilter struct {
	CreatorID string
	UserID    string
}

type QuerySpec struct {
	TimeStep    string
	From, To    *time.Time
	Filter      QueryFilter
	BreakdownBy []string
}

var allowedTimeSteps = map[string]bool{
	"hour": true,
	"day":  true,
}

var usageBreakdownFields = map[string]string{
	"creatorId": "creator_id",
}

func (q *QuerySpec) HasAnyBreakdown() bool {
	return len(q.BreakdownBy) > 0 || q.TimeStep != ""
}

func (q *QuerySpec) hasBreakdownBy(e string) bool {
	// callers always set `e` as a string literal so we can panic if it's not valid
	if usageBreakdownFields[e] == "" {
		panic(fmt.Sprintf("unknown breakdown field %q", e))
	}

	for _, a := range q.BreakdownBy {
		if strings.EqualFold(a, e) {
			return true
		}
	}
	return false
}

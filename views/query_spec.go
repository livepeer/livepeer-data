package views

import (
	"fmt"
	"strings"
	"time"
)

type QueryFilter struct {
	PlaybackID       string
	CreatorID        string
	UserID           string
	ProjectID        string
	IsProjectDefault bool
}

type QuerySpec struct {
	From, To    *time.Time
	TimeStep    string
	Filter      QueryFilter
	BreakdownBy []string
	Detailed    bool
}

var viewershipBreakdownFields = map[string]string{
	"playbackId":    "playback_id",
	"dStorageUrl":   "d_storage_url",
	"deviceType":    "device_type",
	"device":        "device",
	"cpu":           "cpu",
	"os":            "os",
	"browser":       "browser",
	"browserEngine": "browser_engine",
	"continent":     "playback_continent_name",
	"country":       "playback_country_name",
	"subdivision":   "playback_subdivision_name",
	"timezone":      "playback_timezone",
	"geohash":       "playback_geo_hash",
	"viewerId":      "viewer_id",
	"creatorId":     "creator_id",
}

var realtimeViewershipBreakdownFields = map[string]string{
	"playbackId": "playback_id",
	"device":     "device",
	"browser":    "browser",
	"country":    "playback_country_name",
}

var allowedTimeSteps = map[string]bool{
	"hour":  true,
	"day":   true,
	"week":  true,
	"month": true,
	"year":  true,
}

func (q *QuerySpec) hasBreakdownBy(e string) bool {
	// callers always set `e` as a string literal so we can panic if it's not valid
	if viewershipBreakdownFields[e] == "" {
		panic(fmt.Sprintf("unknown breakdown field %q", e))
	}

	for _, a := range q.BreakdownBy {
		if strings.EqualFold(a, e) {
			return true
		}
	}
	return false
}

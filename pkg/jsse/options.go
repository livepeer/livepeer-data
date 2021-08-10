package jsse

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
)

const (
	MimeTypeEventStream = "text/event-stream"
	MimeTypeJson        = "application/json"
)

var SupportedMimeTypes = []string{MimeTypeEventStream, MimeTypeJson}

type Options struct {
	MimeType           string
	LastEventID        string
	PollMaxWaitTime    time.Duration
	ClientRetryBackoff time.Duration
	PingPeriod         time.Duration
}

func InitOptions(r *http.Request) Options {
	var (
		mimeType         = negotiateMimeType(r.Header["Accept"], SupportedMimeTypes, MimeTypeEventStream)
		lastEventID      = r.Header.Get("Last-Event-Id")
		maxWaitTime      = 10 * time.Second
		lastEventIDQuery = r.URL.Query().Get("lastEventId")
		maxWaitTimeQuery = r.URL.Query().Get("pollMaxWaitSec")
	)
	if lastEventID == "" {
		lastEventID = lastEventIDQuery
	}
	if maxWaitTimeQuery != "" {
		timeSec, err := strconv.Atoi(maxWaitTimeQuery)
		if err != nil || timeSec <= 0 {
			glog.Warningf("Ignoring bad pollMaxWaitSec query param: %q", maxWaitTimeQuery)
		} else {
			if timeSec > 60 {
				timeSec = 60
			}
			maxWaitTime = time.Duration(timeSec) * time.Second
		}
	}
	return Options{
		MimeType:        mimeType,
		LastEventID:     lastEventID,
		PollMaxWaitTime: maxWaitTime,
	}
}

func (o Options) WithClientRetryBackoff(backoff time.Duration) Options {
	o.ClientRetryBackoff = backoff
	return o
}

func (o Options) WithPing(period time.Duration) Options {
	o.PingPeriod = period
	return o
}

func negotiateMimeType(acceptedHdrs []string, supported []string, defaultMime string) string {
	for _, hdr := range acceptedHdrs {
		for _, accept := range strings.Split(hdr, ",") {
			for _, candidate := range supported {
				if strings.Contains(accept, candidate) {
					return candidate
				}
			}
		}
	}
	return defaultMime
}

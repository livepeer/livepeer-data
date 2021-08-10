package jsse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Event struct {
	ID    string          `json:"id,omitempty"`
	Event string          `json:"event,omitempty"`
	Data  json.RawMessage `json:"data"`
	retry time.Duration   `json:"-"`
}

func ServeEvents(ctx context.Context, opts Options, rw http.ResponseWriter, events <-chan Event) error {
	switch opts.MimeType {
	case MimeTypeJson:
		ctx, cancel := context.WithTimeout(ctx, opts.PollMaxWaitTime)
		firstEvents := recvFirstAvailable(ctx, events)
		cancel()

		setHeaders(rw, opts.MimeType)
		response := map[string]interface{}{"events": firstEvents}
		if err := json.NewEncoder(rw).Encode(response); err != nil {
			return err
		}
		return nil
	case MimeTypeEventStream:
		flusher, ok := rw.(http.Flusher)
		if !ok {
			return HTTPError{http.StatusInternalServerError, errors.New("server-sent events unsupported")}
		}

		setHeaders(rw, opts.MimeType)
		if retryBackoff := opts.ClientRetryBackoff; retryBackoff > 0 {
			if err := writeEvent(rw, &Event{retry: retryBackoff}); err != nil {
				return err
			}
		}
		flusher.Flush()

		var pingC <-chan time.Time
		if period := opts.PingPeriod; period > 0 {
			ticker := time.NewTicker(period)
			defer ticker.Stop()
			pingC = ticker.C
		}
		for {
			select {
			case <-pingC:
				if err := writeEvent(rw, &Event{Event: "ping"}); err != nil {
					return err
				}
			case evt, ok := <-events:
				if !ok {
					return nil
				}
				if err := writeEvent(rw, &evt); err != nil {
					return err
				}
				for recvImmediate(events, &evt) {
					if err := writeEvent(rw, &evt); err != nil {
						return err
					}
				}
			case <-ctx.Done():
				return nil
			}
			flusher.Flush()
		}
	default:
		return HTTPError{http.StatusUnsupportedMediaType, fmt.Errorf("unsupported media type: %s", opts.MimeType)}
	}
}

func recvFirstAvailable(ctx context.Context, events <-chan Event) []Event {
	select {
	case <-ctx.Done():
		return []Event{}
	case evt, ok := <-events:
		if !ok {
			return []Event{}
		}
		result := []Event{evt}
		for recvImmediate(events, &evt) {
			result = append(result, evt)
		}
		return result
	}
}

func recvImmediate(events <-chan Event, out *Event) bool {
	select {
	case evt, ok := <-events:
		*out = evt
		return ok
	default:
		return false
	}
}

func setHeaders(rw http.ResponseWriter, mimeType string) {
	rw.Header().Set("Content-Type", mimeType+"; charset=utf-8")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
}

func writeEvent(w io.Writer, evt *Event) (err error) {
	if evt.ID != "" {
		_, err = fmt.Fprintf(w, "id: %s\n", evt.ID)
	}
	if err == nil && evt.Event != "" {
		_, err = fmt.Fprintf(w, "event: %s\n", evt.Event)
	}
	if err == nil && len(evt.Data) > 0 {
		_, err = fmt.Fprintf(w, "data: %s\n", evt.Data)
	}
	if err == nil && evt.retry > 0 {
		_, err = fmt.Fprintf(w, "retry: %d\n", evt.retry.Milliseconds())
	}
	if err == nil {
		_, err = fmt.Fprint(w, "\n")
	}
	if err == nil {
		return nil
	}
	return fmt.Errorf("error writing event: %w", err)
}

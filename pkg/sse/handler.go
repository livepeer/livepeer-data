package sse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Event struct {
	ID    string
	Event string
	Data  []byte
	Retry time.Duration
}

func ServeEvents(ctx context.Context, rw http.ResponseWriter, events <-chan Event) error {
	flusher, ok := rw.(http.Flusher)
	if !ok {
		return errors.New("server-sent events unsupported")
	}

	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.WriteHeader(http.StatusOK)
	flusher.Flush()

	for {
		select {
		case evt, ok := <-events:
			if !ok {
				return nil
			}
			err := writeEvent(rw, &evt)
			if err != nil {
				return fmt.Errorf("error writing event: %w", err)
			}
			for recvImmediate(events, &evt) {
				err := writeEvent(rw, &evt)
				if err != nil {
					return fmt.Errorf("error writing event: %w", err)
				}
			}
			flusher.Flush()
		case <-ctx.Done():
			return nil
		}
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
	if err == nil && evt.Retry > 0 {
		_, err = fmt.Fprintf(w, "retry: %d\n", evt.Retry.Milliseconds())
	}
	if err == nil {
		_, err = fmt.Fprint(w, "\n")
	}
	return err
}

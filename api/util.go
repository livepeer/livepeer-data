package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/jsse"
)

func toSSEEvent(evt data.Event) (jsse.Event, error) {
	data, err := json.Marshal(evt)
	if err != nil {
		return jsse.Event{}, err
	}
	return jsse.Event{
		ID:    evt.ID().String(),
		Event: "lp_event",
		Data:  data,
	}, nil
}

func parseInputTimestamp(str string) (*time.Time, error) {
	if str == "" {
		return nil, nil
	}
	t, rfcErr := time.Parse(time.RFC3339Nano, str)
	if rfcErr == nil {
		return &t, nil
	}

	ts, unixErr := strconv.ParseInt(str, 10, 64)
	if unixErr != nil {
		return nil, fmt.Errorf("bad time %q. must be in RFC3339 or Unix Timestamp (millisecond) formats. rfcErr: %s; unixErr: %s", str, rfcErr, unixErr)
	}
	t = time.UnixMilli(ts)
	return &t, nil
}

func parseInputDuration(str string) (time.Duration, error) {
	if str == "" {
		return 0, nil
	}
	return time.ParseDuration(str)
}

func parseInputUUID(str string) (*uuid.UUID, error) {
	if str == "" {
		return nil, nil
	}
	uuid, err := uuid.Parse(str)
	if err != nil {
		return nil, fmt.Errorf("bad uuid %q: %w", str, err)
	}
	return &uuid, nil
}

func unionCtx(ctx1, ctx2 context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		select {
		case <-ctx1.Done():
		case <-ctx2.Done():
		}
	}()
	return ctx, cancel
}

func nonNilErrs(errs ...error) []error {
	var nonNil []error
	for _, err := range errs {
		if err != nil {
			nonNil = append(nonNil, err)
		}
	}
	return nonNil
}

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/sse"
)

const sseBufferSize = 128

type apiHandler struct {
	serverCtx context.Context
	core      *health.Core
}

func NewHandler(serverCtx context.Context, apiRoot string, healthcore *health.Core) http.Handler {
	handler := &apiHandler{serverCtx, healthcore}

	router := httprouter.New()
	router.HandlerFunc("GET", "/_healthz", handler.healthcheck)

	streamRoot := path.Join(apiRoot, "/stream/:manifestId")
	router.GET(streamRoot+"/health", handler.getStreamHealth)
	router.GET(streamRoot+"/events", handler.subscribeEvents)

	return router
}

func (h *apiHandler) healthcheck(rw http.ResponseWriter, r *http.Request) {
	status := http.StatusOK
	if !h.core.IsHealthy() {
		status = http.StatusServiceUnavailable
	}
	rw.WriteHeader(status)
}

func (h *apiHandler) getStreamHealth(rw http.ResponseWriter, r *http.Request, params httprouter.Params) {
	manifestID := params.ByName("manifestId")
	status, err := h.core.GetStatus(manifestID)
	if err != nil {
		respondError(rw, http.StatusInternalServerError, err)
		return
	}
	rw.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(rw).Encode(status); err != nil {
		glog.Errorf("Error writing stream health JSON response. err=%q", err)
	}
}

func (h *apiHandler) subscribeEvents(rw http.ResponseWriter, r *http.Request, params httprouter.Params) {
	var (
		manifestID        = params.ByName("manifestId")
		accept            = strings.ToLower(r.Header.Get("Accept"))
		from, err1        = parseInputTimestamp(r.URL.Query().Get("from"))
		to, err2          = parseInputTimestamp(r.URL.Query().Get("to"))
		lastEventID, err3 = parseInputUUID(r.Header.Get("Last-Event-ID"))
		mustFindLast, _   = strconv.ParseBool(r.URL.Query().Get("mustFindLast"))
	)
	if errs := nonNilErrs(err1, err2, err3); len(errs) > 0 {
		respondError(rw, http.StatusBadRequest, errs...)
		return
	}
	if !strings.Contains(accept, "text/event-stream") {
		events, err := h.core.GetPastEvents(manifestID, from, to)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}
		response := map[string]interface{}{"events": events}
		respondJson(rw, http.StatusOK, response)
		return
	}

	ctx, cancel := unionCtx(r.Context(), h.serverCtx)
	defer cancel()
	pastEvents, subscription, err := h.core.SubscribeEvents(ctx, manifestID, lastEventID, from)
	if err == health.ErrEventNotFound && !mustFindLast {
		pastEvents, subscription, err = h.core.SubscribeEvents(ctx, manifestID, nil, nil)
	}
	if err != nil {
		respondError(rw, http.StatusInternalServerError, err)
		return
	}

	sseEvents := makeSSEEventChan(ctx, pastEvents, subscription)
	err = sse.ServeEvents(ctx, rw, sseEvents)
	if err != nil {
		glog.Errorf("Error serving SSE events. err=%q", err)
		respondError(rw, http.StatusInternalServerError, err)
		return
	}
}

func makeSSEEventChan(ctx context.Context, pastEvents []data.Event, subscription <-chan data.Event) <-chan sse.Event {
	events := make(chan sse.Event, sseBufferSize)
	send := func(evt data.Event) bool {
		sseEvt, err := toSSEEvent(evt)
		if err != nil {
			glog.Errorf("Skipping bad event due to error converting to SSE. evtID=%q, manifestID=%q, err=%q", evt.ID(), evt.ManifestID(), err)
			return true
		}
		select {
		case events <- sseEvt:
			return true
		case <-ctx.Done():
			return false
		}
	}
	go func() {
		defer close(events)
		for _, evt := range pastEvents {
			if !send(evt) {
				return
			}
		}
		for evt := range subscription {
			if !send(evt) {
				return
			}
		}
	}()
	return events
}

func toSSEEvent(evt data.Event) (sse.Event, error) {
	data, err := json.Marshal(evt)
	if err != nil {
		return sse.Event{}, err
	}
	return sse.Event{
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
		return nil, fmt.Errorf("bad time %q. must be in RFC3339 or Unix Timestamp (sec) formats. rfcErr: %s; unixErr: %s", str, rfcErr, unixErr)
	}
	if ts > 1e13 {
		t = time.Unix(0, ts)
	} else {
		t = time.Unix(ts, 0)
	}
	return &t, nil
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

type errorResponse struct {
	Errors []string
}

func respondError(rw http.ResponseWriter, defaultStatus int, errs ...error) {
	status := defaultStatus
	response := errorResponse{}
	for _, err := range errs {
		response.Errors = append(response.Errors, err.Error())
		if errors.Is(err, health.ErrStreamNotFound) || errors.Is(err, health.ErrEventNotFound) {
			status = http.StatusNotFound
		}
	}
	respondJson(rw, status, response)
}

func respondJson(rw http.ResponseWriter, status int, response interface{}) {
	rw.Header().Set("Content-Type", "application/json; charset=utf-8")
	rw.WriteHeader(status)
	if err := json.NewEncoder(rw).Encode(response); err != nil {
		glog.Errorf("Error writing response. err=%q, response=%+v", err, response)
	}
}

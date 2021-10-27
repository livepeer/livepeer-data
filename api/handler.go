package api

import (
	"bytes"
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
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/jsse"
	"github.com/nbio/hitch"
)

const (
	sseRetryBackoff = 10 * time.Second
	ssePingDelay    = 20 * time.Second
	sseBufferSize   = 128
	proxyLoopHeader = "X-Livepeer-Proxy"
)

type contextKey int

const (
	streamStatusKey contextKey = iota
)

type APIHandlerOptions struct {
	APIRoot                       string
	AuthURL                       string
	RegionalHostFormat, OwnRegion string
}

type apiHandler struct {
	opts      APIHandlerOptions
	serverCtx context.Context
	core      *health.Core
}

func NewHandler(serverCtx context.Context, opts APIHandlerOptions, healthcore *health.Core) http.Handler {
	handler := &apiHandler{opts, serverCtx, healthcore}

	router := hitch.New()
	router.Use(cors)
	router.HandleFunc("GET", "/_healthz", handler.healthcheck)

	streamApiRoot := path.Join(opts.APIRoot, "/stream/:streamId")
	middlewares := []hitch.Middleware{
		streamStatus(healthcore, "streamId"),
		regionProxy(opts.RegionalHostFormat, opts.OwnRegion),
	}
	if opts.AuthURL != "" {
		middlewares = append(middlewares, authorization(opts.AuthURL))
	}
	router.Get(streamApiRoot+"/health", http.HandlerFunc(handler.getStreamHealth), middlewares...)
	router.Get(streamApiRoot+"/events", http.HandlerFunc(handler.subscribeEvents), middlewares...)

	return router.Handler()
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		next.ServeHTTP(rw, r)
	})
}

func streamStatus(healthcore *health.Core, streamIDParam string) hitch.Middleware {
	return inlineMiddleware(func(rw http.ResponseWriter, r *http.Request, next http.Handler) {
		params := hitch.Params(r)
		streamID := params.ByName(streamIDParam)
		if streamID == "" {
			next.ServeHTTP(rw, r)
			return
		}
		status, err := healthcore.GetStatus(streamID)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), streamStatusKey, status))
		next.ServeHTTP(rw, r)
	})
}

func getStreamStatus(r *http.Request) *health.Status {
	return r.Context().Value(streamStatusKey).(*health.Status)
}

func authorization(authUrl string) hitch.Middleware {
	return inlineMiddleware(func(rw http.ResponseWriter, r *http.Request, next http.Handler) {
		// TODO: Move all this to a proper client
		status := getStreamStatus(r)
		authReq := map[string]interface{}{
			"resource": map[string]string{
				"method":   r.Method,
				"url":      r.URL.String(),
				"streamID": status.ID,
			},
		}
		payload, err := json.Marshal(authReq)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, fmt.Errorf("error creating authorization payload: %w", err))
			return
		}
		req, err := http.NewRequestWithContext(r.Context(), "POST", authUrl, bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}
		for _, header := range []string{"Authorization", "Proxy-Authorization", "Cookie"} {
			req.Header[header] = r.Header[header]
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, fmt.Errorf("error authorizing request: %w", err))
			return
		}

		if res.StatusCode != http.StatusOK {
			outerErr := fmt.Errorf("authorization failed: %d %s", res.StatusCode, res.Status)
			var errResp errorResponse
			if err := json.NewDecoder(res.Body).Decode(&errResp); err == nil && len(errResp.Errors) > 0 {
				outerErr = fmt.Errorf("authorization failed: %s", strings.Join(errResp.Errors, "; "))
			}
			respondError(rw, res.StatusCode, outerErr)
			return
		}
		next.ServeHTTP(rw, r)
	})
}

func inlineMiddleware(middleware func(rw http.ResponseWriter, r *http.Request, next http.Handler)) hitch.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			middleware(rw, r, next)
		})
	}
}

func (h *apiHandler) healthcheck(rw http.ResponseWriter, r *http.Request) {
	status := http.StatusOK
	if !h.core.IsHealthy() {
		status = http.StatusServiceUnavailable
	}
	rw.WriteHeader(status)
}

func (h *apiHandler) getStreamHealth(rw http.ResponseWriter, r *http.Request) {
	respondJson(rw, http.StatusOK, getStreamStatus(r))
}

func (h *apiHandler) subscribeEvents(rw http.ResponseWriter, r *http.Request) {
	var (
		streamStatus = getStreamStatus(r)
		streamID     = streamStatus.ID
		sseOpts      = jsse.InitOptions(r).
				WithClientRetryBackoff(sseRetryBackoff).
				WithPing(ssePingDelay)

		lastEventID, err = parseInputUUID(sseOpts.LastEventID)
		from, err1       = parseInputTimestamp(r.URL.Query().Get("from"))
		to, err2         = parseInputTimestamp(r.URL.Query().Get("to"))
		mustFindLast, _  = strconv.ParseBool(r.URL.Query().Get("mustFindLast"))
	)
	if errs := nonNilErrs(err, err1, err2); len(errs) > 0 {
		respondError(rw, http.StatusBadRequest, errs...)
		return
	}

	var (
		pastEvents   []data.Event
		subscription <-chan data.Event
	)
	ctx, cancel := unionCtx(r.Context(), h.serverCtx)
	defer cancel()
	if to != nil {
		if from == nil {
			respondError(rw, http.StatusBadRequest, errors.New("query 'from' is required when using 'to'"))
			return
		}
		pastEvents, err = h.core.GetPastEvents(streamID, from, to)
	} else {
		pastEvents, subscription, err = h.core.SubscribeEvents(ctx, streamID, lastEventID, from)
		if err == health.ErrEventNotFound && !mustFindLast {
			pastEvents, subscription, err = h.core.SubscribeEvents(ctx, streamID, nil, nil)
		}
	}
	if err != nil {
		respondError(rw, http.StatusInternalServerError, err)
		return
	}

	sseEvents := makeSSEEventChan(ctx, pastEvents, subscription)
	err = jsse.ServeEvents(ctx, sseOpts, rw, sseEvents)
	if err != nil {
		status := http.StatusInternalServerError
		if httpErr, ok := err.(jsse.HTTPError); ok {
			status, err = httpErr.StatusCode, httpErr.Cause
		}
		glog.Errorf("Error serving events. err=%q", err)
		respondError(rw, status, err)
	}
}

func makeSSEEventChan(ctx context.Context, pastEvents []data.Event, subscription <-chan data.Event) <-chan jsse.Event {
	if subscription == nil {
		events := make(chan jsse.Event, len(pastEvents))
		for _, evt := range pastEvents {
			sendEvent(ctx, events, evt)
		}
		close(events)
		return events
	}
	events := make(chan jsse.Event, sseBufferSize)
	go func() {
		defer close(events)
		for _, evt := range pastEvents {
			if !sendEvent(ctx, events, evt) {
				return
			}
		}
		for evt := range subscription {
			if !sendEvent(ctx, events, evt) {
				return
			}
		}
	}()
	return events
}

func sendEvent(ctx context.Context, dest chan<- jsse.Event, evt data.Event) bool {
	sseEvt, err := toSSEEvent(evt)
	if err != nil {
		glog.Errorf("Skipping bad event due to error converting to SSE. evtID=%q, streamID=%q, err=%q", evt.ID(), evt.StreamID(), err)
		return true
	}
	select {
	case dest <- sseEvt:
		return true
	case <-ctx.Done():
		return false
	}
}

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
	t = data.NewUnixMillisTime(ts).Time
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

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"path"
	"strconv"
	"time"

	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/health/reducers"
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
	RegionalHostFormat, OwnRegion string
}

type apiHandler struct {
	opts        APIHandlerOptions
	serverCtx   context.Context
	core        *health.Core
	regionProxy *httputil.ReverseProxy
}

func NewHandler(serverCtx context.Context, opts APIHandlerOptions, healthcore *health.Core) http.Handler {
	regionProxy := &httputil.ReverseProxy{
		Director:      regionProxyDirector(opts.RegionalHostFormat),
		FlushInterval: 100 * time.Millisecond,
	}
	handler := &apiHandler{opts, serverCtx, healthcore, regionProxy}

	router := hitch.New()
	router.Use(cors)
	router.HandleFunc("GET", "/_healthz", handler.healthcheck)

	streamRoot := path.Join(opts.APIRoot, "/stream/:manifestId")
	router.Get(streamRoot+"/health", http.HandlerFunc(handler.getStreamHealth), handler.withRegionProxy)
	router.Get(streamRoot+"/events", http.HandlerFunc(handler.subscribeEvents), handler.withRegionProxy)

	return router.Handler()
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		next.ServeHTTP(rw, r)
	})
}

func (h *apiHandler) withRegionProxy(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		params := hitch.Params(r)
		manifestID := params.ByName("manifestId")
		status, err := h.core.GetStatus(manifestID)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), streamStatusKey, status))

		streamRegion := reducers.GetLastActiveData(status).Region
		if h.opts.OwnRegion == "" || streamRegion == "" || streamRegion == h.opts.OwnRegion {
			next.ServeHTTP(rw, r)
			return
		}
		if _, ok := r.Header[proxyLoopHeader]; ok {
			respondError(rw, http.StatusLoopDetected, errors.New("proxy loop detected"))
			return
		}
		h.regionProxy.ServeHTTP(rw, r)
	})
}

func regionProxyDirector(hostFormat string) func(req *http.Request) {
	return func(req *http.Request) {
		glog.V(8).Infof("Proxying request url=%s headers=%+v", req.URL, req.Header)
		status := getStreamStatus(req)
		streamRegion := reducers.GetLastActiveData(status).Region

		req.URL.Scheme = "http"
		if fwdProto := req.Header.Get("X-Forwarded-Proto"); fwdProto != "" {
			req.URL.Scheme = fwdProto
		}
		req.URL.Host = fmt.Sprintf(hostFormat, streamRegion)
		req.Host = req.URL.Host

		req.Header.Set(proxyLoopHeader, "analyzer")
		if _, ok := req.Header["User-Agent"]; !ok {
			// explicitly disable User-Agent so it's not set to default value
			req.Header.Set("User-Agent", "")
		}
	}
}

func getStreamStatus(r *http.Request) *health.Status {
	return r.Context().Value(streamStatusKey).(*health.Status)
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
		params     = hitch.Params(r)
		manifestID = params.ByName("manifestId")
		sseOpts    = jsse.InitOptions(r).
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
		pastEvents, err = h.core.GetPastEvents(manifestID, from, to)
	} else {
		pastEvents, subscription, err = h.core.SubscribeEvents(ctx, manifestID, lastEventID, from)
		if err == health.ErrEventNotFound && !mustFindLast {
			pastEvents, subscription, err = h.core.SubscribeEvents(ctx, manifestID, nil, nil)
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

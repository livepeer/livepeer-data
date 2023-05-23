package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/metrics"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/jsse"
	"github.com/livepeer/livepeer-data/usage"
	"github.com/livepeer/livepeer-data/views"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	cache "github.com/victorspringer/http-cache"
	"github.com/victorspringer/http-cache/adapter/memory"
)

const (
	sseRetryBackoff = 10 * time.Second
	ssePingDelay    = 20 * time.Second
	sseBufferSize   = 128

	streamIDParam   = "streamId"
	assetIDParam    = "assetId"
	playbackIDParam = "playbackId"
)

var httpCache *cache.Client

func init() {
	memcached, err := memory.NewAdapter(
		memory.AdapterWithAlgorithm(memory.LRU),
		memory.AdapterWithCapacity(2000),
	)
	if err != nil {
		panic(err)
	}

	httpCache, err = cache.NewClient(
		cache.ClientWithAdapter(memcached),
		cache.ClientWithTTL(5*time.Minute),
	)
	if err != nil {
		panic(err)
	}
}

type APIHandlerOptions struct {
	ServerName, APIRoot, AuthURL  string
	RegionalHostFormat, OwnRegion string
	Prometheus                    bool
}

type apiHandler struct {
	opts      APIHandlerOptions
	serverCtx context.Context
	core      *health.Core
	views     *views.Client
	usage     *usage.Client
}

func NewHandler(serverCtx context.Context, opts APIHandlerOptions, healthcore *health.Core, views *views.Client, usage *usage.Client) http.Handler {
	handler := &apiHandler{opts, serverCtx, healthcore, views, usage}

	router := chi.NewRouter()

	// don't use middlewares for the system routes
	router.Get("/_healthz", handler.healthcheck)
	if opts.Prometheus {
		router.Method("GET", "/metrics", promhttp.Handler())
	}

	router.Route(opts.APIRoot, func(router chi.Router) {
		router.Use(chimiddleware.Logger)
		router.Use(chimiddleware.NewCompressor(5, "application/json").Handler)
		router.Use(handler.cors())

		router.Mount(`/stream/{`+streamIDParam+`}`, handler.streamHealthHandler())
		router.Mount("/views", handler.viewershipHandler())
	})

	return router
}

// {streamId} variable must be set in the request context
func (h *apiHandler) streamHealthHandler() chi.Router {
	healthcore, opts := h.core, h.opts

	router := chi.NewRouter()
	if opts.AuthURL != "" {
		router.Use(authorization(opts.AuthURL))
	}
	router.Use(
		streamStatus(healthcore),
		regionProxy(opts.RegionalHostFormat, opts.OwnRegion))

	h.withMetrics(router, "get_stream_health").
		MethodFunc("GET", "/health", h.getStreamHealth)
	h.withMetrics(router, "stream_health_events").
		MethodFunc("GET", "/events", h.subscribeEvents)

	return router
}

func (h *apiHandler) viewershipHandler() chi.Router {
	opts := h.opts

	router := chi.NewRouter()
	if opts.AuthURL != "" {
		router.Use(authorization(opts.AuthURL))
	}

	// TODO: Remove this deprecated endpoint once we know no one is using it
	h.withMetrics(router, "get_total_views").
		With(h.cache(false)).
		MethodFunc("GET", fmt.Sprintf(`/{%s}/total`, assetIDParam), h.getTotalViews)

	// total views public API
	h.withMetrics(router, "query_total_viewership").
		With(h.cache(false)).
		MethodFunc("GET", fmt.Sprintf(`/query/total/{%s}`, playbackIDParam), h.queryTotalViewership)
	// creator views API, requires assetId or streamId on the query-string
	h.withMetrics(router, "query_creator_viewership").
		With(h.cache(true)).
		With(ensureIsCreatorQuery).
		MethodFunc("GET", `/query/creator`, h.queryViewership(false))
	// full application views API, gets access to all metrics and filters
	h.withMetrics(router, "query_application_viewership").
		With(h.cache(true)).
		MethodFunc("GET", `/query`, h.queryViewership(true))
	// usage API, gets access to hourly usage by userID and creatorID
	h.withMetrics(router, "query_usage").
		With(h.cache(true)).
		MethodFunc("GET", `/usage`, h.queryUsage())

	return router
}

func (h *apiHandler) withMetrics(router chi.Router, name string) chi.Router {
	if !h.opts.Prometheus {
		return router
	}
	return router.With(func(handler http.Handler) http.Handler {
		return metrics.ObservedHandler(name, handler)
	})
}

func (h *apiHandler) cache(varyAuth bool) middleware {
	return func(next http.Handler) http.Handler {
		next = httpCache.Middleware(next)

		return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			rw.Header().Set("Cache-Control", "public, max-age=60, s-maxage=300, stale-while-revalidate=3600, stale-if-error=86400")

			if varyAuth {
				rw.Header().Add("Vary", "Authorization")

				// cache lib uses only URL as key so add auth header to the query-string
				query := r.URL.Query()
				query.Add("auth-header", r.Header.Get("Authorization"))
				r.URL.RawQuery = query.Encode()
			}

			next.ServeHTTP(rw, r)
		})
	}
}

func (h *apiHandler) cors() middleware {
	return inlineMiddleware(func(rw http.ResponseWriter, r *http.Request, next http.Handler) {
		if h.opts.ServerName != "" {
			rw.Header().Set("Server", h.opts.ServerName)
		}
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		rw.Header().Set("Access-Control-Allow-Headers", "*")
		if origin := r.Header.Get("Origin"); origin != "" {
			rw.Header().Set("Access-Control-Allow-Origin", origin)
			rw.Header().Set("Access-Control-Allow-Credentials", "true")
		}
		next.ServeHTTP(rw, r)
	})
}

func (h *apiHandler) healthcheck(rw http.ResponseWriter, r *http.Request) {
	status := http.StatusOK
	if !h.core.IsHealthy() {
		status = http.StatusServiceUnavailable
	}
	rw.WriteHeader(status)
}

func (h *apiHandler) queryTotalViewership(rw http.ResponseWriter, r *http.Request) {
	playbackID := apiParam(r, playbackIDParam)

	metric, err := h.views.QuerySummary(r.Context(), playbackID)
	if err != nil {
		respondError(rw, http.StatusInternalServerError, err)
		return
	}

	if metric == nil {
		metric = &views.Metric{PlaybackID: playbackID}
		if dStorageURL := views.ToDStorageURL(playbackID); dStorageURL != "" {
			metric = &views.Metric{DStorageURL: dStorageURL}
		}
	}

	respondJson(rw, http.StatusOK, metric)
}

func ensureIsCreatorQuery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// disallow querying creator metrics with a playback ID
		qs := r.URL.Query()
		qs.Del("playbackId")
		r.URL.RawQuery = qs.Encode()

		hasAsset, hasStream := qs.Get("assetId") != "", qs.Get("streamId") != ""
		if hasAsset == hasStream {
			respondError(rw, http.StatusBadRequest, errors.New("must provide exactly 1 of assetId or streamId for creator query"))
			return
		}

		next.ServeHTTP(rw, r)
	})
}

func (h *apiHandler) queryViewership(detailed bool) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		var (
			from, err1 = parseInputTimestamp(r.URL.Query().Get("from"))
			to, err2   = parseInputTimestamp(r.URL.Query().Get("to"))
		)
		if errs := nonNilErrs(err1, err2); len(errs) > 0 {
			respondError(rw, http.StatusBadRequest, errs...)
			return
		}

		userId, ok := r.Context().Value(userIdContextKey).(string)
		if !ok {
			respondError(rw, http.StatusInternalServerError, errors.New("request not authenticated"))
			return
		}

		qs := r.URL.Query()
		assetID, streamID := qs.Get("assetId"), qs.Get("streamId")
		query := views.QuerySpec{
			From:     from,
			To:       to,
			TimeStep: qs.Get("timeStep"),
			Filter: views.QueryFilter{
				UserID:     userId,
				PlaybackID: qs.Get("playbackId"),
				CreatorID:  qs.Get("creatorId"),
			},
			BreakdownBy: qs["breakdownBy[]"],
			Detailed:    detailed,
		}

		metrics, err := h.views.QueryEvents(r.Context(), query, assetID, streamID)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}
		respondJson(rw, http.StatusOK, metrics)
	}
}

func (h *apiHandler) queryUsage() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		var (
			from, err1 = parseInputTimestamp(r.URL.Query().Get("from"))
			to, err2   = parseInputTimestamp(r.URL.Query().Get("to"))
		)
		if errs := nonNilErrs(err1, err2); len(errs) > 0 {
			respondError(rw, http.StatusBadRequest, errs...)
			return
		}
		userId, ok := r.Context().Value(userIdContextKey).(string)

		if !ok {
			respondError(rw, http.StatusInternalServerError, errors.New("request not authenticated"))
			return
		}

		// TODO allow only admin to call this endpoint

		qs := r.URL.Query()
		creatorId := qs.Get("creatorId")
		paramUserId := qs.Get("userId")

		if paramUserId == "" {
			respondError(rw, http.StatusBadRequest, errors.New("userId is required"))
			return
		}

		query := usage.QuerySpec{
			From: from,
			To:   to,
			Filter: usage.QueryFilter{
				UserID:    paramUserId,
				CreatorID: qs.Get("creatorId"),
			},
		}

		usage, err := h.usage.QuerySummary(r.Context(), userId, creatorId, query)

		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}

		respondJson(rw, http.StatusOK, usage)
	}
}

func (h *apiHandler) getTotalViews(rw http.ResponseWriter, r *http.Request) {
	assetID := apiParam(r, assetIDParam)

	totalViews, err := h.views.Deprecated_GetTotalViews(r.Context(), assetID)
	if err != nil {
		respondError(rw, http.StatusInternalServerError, err)
		return
	}

	playbackID, oldStartViews := totalViews[0].ID, totalViews[0].StartViews
	if playbackID == "" {
		respondError(rw, http.StatusInternalServerError, errors.New("playbackId is empty"))
		return
	}

	metric, err := h.views.QuerySummary(r.Context(), playbackID)
	if err != nil {
		respondError(rw, http.StatusInternalServerError, err)
		return
	}

	if metric != nil {
		totalViews = []views.TotalViews{{
			ID:         totalViews[0].ID,
			StartViews: metric.ViewCount,
		}}
	}

	userId := r.Context().Value(userIdContextKey)
	glog.Infof("Used deprecated get total views endpoint userId=%v assetId=%v playbackId=%v oldStartViews=%v newViewCount=%v",
		userId, assetID, totalViews[0].ID, oldStartViews, totalViews[0].StartViews)

	respondJson(rw, http.StatusOK, totalViews)
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

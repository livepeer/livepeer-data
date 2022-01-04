package api

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/livepeer-data/metrics"
)

type middleware func(http.Handler) http.Handler

func apiParam(r *http.Request, name string) string {
	return httprouter.ParamsFromContext(r.Context()).ByName(name)
}

func inlineMiddleware(middleware func(rw http.ResponseWriter, r *http.Request, next http.Handler)) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			middleware(rw, r, next)
		})
	}
}

func prepareHandlerFunc(name string, withMetrics bool, handler http.HandlerFunc, middlewares ...middleware) http.Handler {
	return prepareHandler(name, withMetrics, handler, middlewares...)
}

func prepareHandler(name string, withMetrics bool, handler http.Handler, middlewares ...middleware) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	if withMetrics {
		handler = metrics.ObservedHandler(name, handler)
	}
	return handler
}

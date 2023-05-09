package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type middleware = func(http.Handler) http.Handler

func apiParam(r *http.Request, name string) string {
	return chi.URLParam(r, name)
}

func inlineMiddleware(middleware func(rw http.ResponseWriter, r *http.Request, next http.Handler)) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			middleware(rw, r, next)
		})
	}
}

package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/health"
)

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

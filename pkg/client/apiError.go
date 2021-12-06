package client

import (
	"errors"
	"net/http"
	"strings"
)

type APIError struct {
	StatusCode int
	Errors     []string
}

func (h APIError) Error() string {
	return strings.Join(h.Errors, "; ")
}

func IsNotFound(err error) bool {
	var apiErr APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound
}

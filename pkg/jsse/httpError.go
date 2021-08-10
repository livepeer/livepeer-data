package jsse

type HTTPError struct {
	StatusCode int
	Cause      error
}

func (h HTTPError) Error() string {
	return h.Cause.Error()
}

func (h HTTPError) Unwrap() error {
	return h.Cause
}

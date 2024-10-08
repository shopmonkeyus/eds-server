package util

import (
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/shopmonkeyus/go-common/logger"
)

const (
	defaultTimeout = time.Second * 30
)

type HTTPRetry struct {
	attempts int
	started  *time.Time
	timeout  time.Duration
	req      *http.Request
	logger   logger.Logger
}

func (r *HTTPRetry) shouldRetry(resp *http.Response, err error) bool {
	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "connection reset") || strings.Contains(msg, "connection refused") {
			return r.started.Add(r.timeout).After(time.Now())
		}
	}
	if resp != nil {
		switch resp.StatusCode {
		case http.StatusRequestTimeout, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusTooManyRequests:
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return true
		}
	}
	return false
}

func (r *HTTPRetry) Do() (*http.Response, error) {
	if r.started == nil {
		tv := time.Now()
		r.started = &tv
	}
	r.attempts++
	resp, err := http.DefaultClient.Do(r.req)
	if r.shouldRetry(resp, err) {
		jitter := time.Duration(time.Millisecond*100 + time.Millisecond*time.Duration(rand.Int63n(int64(500*r.attempts))))
		if r.logger != nil {
			var code int
			if resp != nil {
				code = resp.StatusCode
			}
			r.logger.Trace("%s request failed (path: %s) (status: %d), retrying request in %v", r.req.Method, r.req.URL.String(), code, jitter)
		}
		time.Sleep(jitter)
		return r.Do()
	}
	return resp, err
}

type HTTPRetryOption func(*HTTPRetry)

// WithLogger sets the logger for the HTTP retry utility.
func WithLogger(logger logger.Logger) HTTPRetryOption {
	return func(r *HTTPRetry) {
		r.logger = logger
	}
}

// WithTimeout sets the timeout for the HTTP request.
func WithTimeout(dur time.Duration) HTTPRetryOption {
	return func(r *HTTPRetry) {
		r.timeout = dur
	}
}

// NewHTTPRetry creates a new utility for retrying HTTP requests.
func NewHTTPRetry(req *http.Request, opts ...HTTPRetryOption) *HTTPRetry {
	retry := HTTPRetry{
		req:     req,
		timeout: defaultTimeout,
	}
	for _, opt := range opts {
		opt(&retry)
	}
	return &retry
}

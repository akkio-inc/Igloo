package ai

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// client sends providers' requests: JSON over HTTPS, retried after rate
// limits and failures that may pass, as long as the statement lasts.
type client struct {
	http *http.Client
	// timeout bounds each attempt.
	timeout time.Duration
	// attempts is how many times a request is sent at most.
	attempts int
	// wait sleeps between attempts; tests shorten it.
	wait func(ctx context.Context, d time.Duration) error
}

const (
	defaultAttempts = 5
	// maxRetryAfter bounds how long a retry-after header makes a request
	// wait.
	maxRetryAfter = time.Minute
	// maxResponse bounds what is read of an answer.
	maxResponse = 64 << 20
)

// newClient makes a client that connects with dial (the egress guard's)
// and gives each attempt timeout.
func newClient(dial func(ctx context.Context, network, address string) (net.Conn, error), timeout time.Duration) *client {
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	transport := &http.Transport{
		// No proxy: connections go to the address the guard vetted.
		Proxy:                 nil,
		DialContext:           dial,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:   30 * time.Second,
		ResponseHeaderTimeout: timeout,
		MaxIdleConnsPerHost:   maxConcurrency,
		IdleConnTimeout:       90 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &client{
		http: &http.Client{
			Transport: transport,
			// Never follow a redirect: it could lead elsewhere with the
			// integration's keys.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		timeout:  timeout,
		attempts: defaultAttempts,
		wait:     sleep,
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// meterKey carries a *atomic.Int64 that counts the bytes requests send.
type meterKey struct{}

func withMeter(ctx context.Context, n *atomic.Int64) context.Context {
	return context.WithValue(ctx, meterKey{}, n)
}

// StatusError is a provider's answer with an HTTP error status.
type StatusError struct {
	Status int
	// Message is the provider's own message, if it gave one.
	Message string
	// Code is its error's code or type, if it gave one.
	Code string
}

func (e *StatusError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = http.StatusText(e.Status)
	}
	return fmt.Sprintf("the provider answered %d: %s", e.Status, msg)
}

// retryable reports whether a status may pass if the request is sent
// again: rate limits, timeouts and the provider's own failures.
func retryable(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusRequestTimeout || status == http.StatusConflict ||
		status >= 500
}

// request is one request to send: built anew for each attempt, as signing
// (Bedrock) dates it.
type request struct {
	method, url string
	header      http.Header
	body        []byte
	// sign, if set, signs each attempt.
	sign func(ctx context.Context, r *http.Request, body []byte) error
}

// postJSON sends body, marshaled, and decodes the answer into out.
func (c *client) postJSON(ctx context.Context, url string, header http.Header, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return c.send(ctx, request{method: http.MethodPost, url: url, header: header, body: b}, out)
}

// send sends r until it succeeds, fails for good or the attempts run out,
// and decodes a successful answer into out. A failure for the request's
// own content (400, 413, 422) is a RowError.
func (c *client) send(ctx context.Context, r request, out any) error {
	var last error
	for attempt := 1; ; attempt++ {
		status, header, body, err := c.attempt(ctx, r)
		switch {
		case err == nil && status/100 == 2:
			if out == nil {
				return nil
			}
			if err := json.Unmarshal(body, out); err != nil {
				return rowErrorf("the provider's answer is not what was expected: %v", err)
			}
			return nil
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil:
			last = fmt.Errorf("could not reach the provider: %w", err)
		default:
			se := statusError(status, body)
			switch {
			case se.Code == "enforced_spend_limit_reached":
				return fmt.Errorf("%w (the provider's spend limit)", se)
			case status == http.StatusBadRequest || status == http.StatusRequestEntityTooLarge ||
				status == http.StatusUnprocessableEntity:
				return &RowError{Msg: se.Error()}
			case !retryable(status):
				return se
			}
			last = se
			if d := retryAfter(header); d > 0 && attempt < c.attempts {
				if err := c.wait(ctx, d); err != nil {
					return err
				}
				continue
			}
		}
		if attempt >= c.attempts {
			return fmt.Errorf("%w; gave up after %d attempts", last, attempt)
		}
		// 0.5 s, 1 s, 2 s, 4 s, with jitter.
		d := time.Duration(float64(250*time.Millisecond<<attempt) * (0.75 + rand.Float64()/2))
		if err := c.wait(ctx, d); err != nil {
			return err
		}
	}
}

// attempt sends r once.
func (c *client) attempt(ctx context.Context, r request) (int, http.Header, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, r.method, r.url, bytes.NewReader(r.body))
	if err != nil {
		return 0, nil, nil, err
	}
	for k, vs := range r.header {
		req.Header[k] = vs
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if r.sign != nil {
		if err := r.sign(ctx, req, r.body); err != nil {
			return 0, nil, nil, err
		}
	}
	if n, ok := ctx.Value(meterKey{}).(*atomic.Int64); ok {
		n.Add(int64(len(r.body)))
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse))
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, body, nil
}

// statusError reads a provider's error answer: OpenAI's, Anthropic's,
// Gemini's and Bedrock's shapes alike.
func statusError(status int, body []byte) *StatusError {
	se := &StatusError{Status: status}
	var e struct {
		Error json.RawMessage `json:"error"`
		// Bedrock's.
		Message string `json:"message"`
		Type    string `json:"__type"`
	}
	if json.Unmarshal(body, &e) != nil {
		se.Message = strings.TrimSpace(firstLine(string(body), 300))
		return se
	}
	se.Message, se.Code = e.Message, e.Type
	if len(e.Error) > 0 {
		var inner struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    any    `json:"code"`
			Status  string `json:"status"`
			Details struct {
				ErrorCode string `json:"error_code"`
			} `json:"details"`
		}
		if json.Unmarshal(e.Error, &inner) == nil {
			se.Message = inner.Message
			se.Code = inner.Type
			if inner.Details.ErrorCode != "" {
				se.Code = inner.Details.ErrorCode
			}
			if c, ok := inner.Code.(string); ok && se.Code == "" {
				se.Code = c
			}
		} else {
			var s string
			if json.Unmarshal(e.Error, &s) == nil {
				se.Message = s
			}
		}
	}
	se.Message = firstLine(se.Message, 300)
	return se
}

func firstLine(s string, n int) string {
	if i := strings.IndexAny(s, "\r\n"); i >= 0 {
		s = s[:i]
	}
	if len(s) > n {
		s = s[:n] + "..."
	}
	return s
}

// retryAfter reads a retry-after header, in seconds or as a date, bounded
// by maxRetryAfter; 0 when there is none.
func retryAfter(h http.Header) time.Duration {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	var d time.Duration
	if secs, err := strconv.ParseFloat(v, 64); err == nil {
		d = time.Duration(secs * float64(time.Second))
	} else if t, err := http.ParseTime(v); err == nil {
		d = time.Until(t)
	}
	return min(max(d, 0), maxRetryAfter)
}

// fatal reports whether err fails the statement rather than one row.
func fatal(err error) bool {
	return err != nil && !IsRowError(err)
}

// errSkipped is a request a statement did not send because another failed
// for good first.
var errSkipped = errors.New("skipped: another call failed")

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
)

const (
	maxWebhookRedirects = 3
	maxWebhookBodyRead  = 64 << 10
	maxWebhookBodyKept  = 4 << 10
)

var allowedMethods = map[string]bool{
	http.MethodGet: true, http.MethodPost: true, http.MethodPut: true, http.MethodPatch: true, http.MethodDelete: true,
}

// NewWebhookClient returns an http.Client with bounded redirects and no client-level timeout:
// the per-attempt ctx deadline governs the request.
func NewWebhookClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) >= maxWebhookRedirects {
				return fmt.Errorf("stopped after %d redirects", maxWebhookRedirects)
			}
			return nil
		},
	}
}

type webhookPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    json.RawMessage   `json:"body"`
}

// NewHTTPWebhook calls payload.url and propagates X-Job-ID / X-Attempt-Number. Network errors,
// 5xx, 408 and 429 are transient; invalid input and other non-2xx statuses are permanent.
func NewHTTPWebhook(client *http.Client, userAgent string) Handler {
	return func(ctx context.Context, job Job) (json.RawMessage, error) {
		var p webhookPayload
		if err := decodePayload(job.Payload, &p); err != nil {
			return nil, err
		}
		u, err := url.Parse(p.URL)
		if err != nil {
			return nil, domain.Permanent(fmt.Errorf("invalid payload: url: %w", err))
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, domain.Permanent(fmt.Errorf("invalid payload: url scheme %q not allowed (http/https only)", u.Scheme))
		}
		if u.Host == "" {
			return nil, domain.Permanent(errors.New("invalid payload: url host is required"))
		}
		method := strings.ToUpper(p.Method)
		if method == "" {
			method = http.MethodPost
		}
		if !allowedMethods[method] {
			return nil, domain.Permanent(fmt.Errorf("invalid payload: method %q not allowed", p.Method))
		}

		var body io.Reader
		if len(p.Body) > 0 {
			body = bytes.NewReader(p.Body)
		}
		req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
		if err != nil {
			return nil, domain.Permanent(fmt.Errorf("build request: %w", err))
		}
		for k, v := range p.Headers {
			req.Header.Set(k, v)
		}
		if body != nil && req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("X-Job-ID", job.ID.String())
		req.Header.Set("X-Attempt-Number", strconv.Itoa(job.Attempt))

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("webhook request: %w", err)
		}
		defer resp.Body.Close()
		respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxWebhookBodyRead))
		if err != nil {
			return nil, fmt.Errorf("read webhook response: %w", err)
		}

		code := resp.StatusCode
		switch {
		case code >= 200 && code < 300:
			kept := respBody
			if len(kept) > maxWebhookBodyKept {
				kept = kept[:maxWebhookBodyKept]
			}
			return marshalResult(map[string]any{"status_code": code, "body": string(kept)})
		case code >= 500 || code == http.StatusRequestTimeout || code == http.StatusTooManyRequests:
			return nil, fmt.Errorf("webhook returned retryable status %d", code)
		default:
			return nil, domain.Permanent(fmt.Errorf("webhook returned status %d", code))
		}
	}
}

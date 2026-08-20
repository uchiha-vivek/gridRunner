package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Client is everything the control plane needs from GitHub.
//
// Two implementations ship: AppClient (a GitHub App, which can mint short-lived
// per-repository credentials) and HTTPClient (a personal access token, which
// cannot). NoopClient keeps local development free of GitHub credentials.
type Client interface {
	SetStatus(ctx context.Context, repo, sha, state, description, targetURL string) error

	// CloneToken returns a short-lived credential for checking out repo, or an
	// empty string when the deployment cannot mint one. An empty token means an
	// anonymous clone, which is correct for public repositories.
	CloneToken(ctx context.Context, repo string) (string, error)
}

// HTTPClient authenticates with a personal access token: one header, no key
// management, good enough for a single-owner deployment. Prefer a GitHub App
// (see AppClient) for anything shared or private.
type HTTPClient struct {
	token string
	base  string
	http  *http.Client
}

func New(token string) *HTTPClient {
	return &HTTPClient{
		token: token,
		base:  "https://api.github.com",
		http:  &http.Client{Timeout: 10 * time.Second},
	}
}

// SetStatus posts a commit status. state is one of: pending, success, failure, error.
func (c *HTTPClient) SetStatus(ctx context.Context, repo, sha, state, description, targetURL string) error {
	return postStatus(ctx, c.http, c.base, c.token, repo, sha, state, description, targetURL)
}

// CloneToken deliberately returns nothing.
//
// A personal access token is long lived and usually scoped to every repository
// the owner can see. Shipping one to every runner would make a single compromised
// build host a full account compromise. Private repositories therefore require
// GitHub App authentication, which can mint a read-only token for one repository
// that expires in an hour.
func (c *HTTPClient) CloneToken(context.Context, string) (string, error) { return "", nil }

// NoopClient is used when no credentials are configured, so local development
// works instead of failing every job at the reporting step.
type NoopClient struct{}

func (NoopClient) SetStatus(context.Context, string, string, string, string, string) error {
	return nil
}
func (NoopClient) CloneToken(context.Context, string) (string, error) { return "", nil }

// postStatus is shared by both authenticated clients: the request is identical,
// only the bearer token differs.
func postStatus(ctx context.Context, hc *http.Client, base, token, repo, sha, state, description, targetURL string) error {
	body, _ := json.Marshal(map[string]string{
		"state":       state,
		"description": truncate(description, 140),
		"context":     "forgerun",
		"target_url":  targetURL,
	})
	url := fmt.Sprintf("%s/repos/%s/statuses/%s", base, repo, sha)

	resp, err := send(ctx, hc, func() (*http.Request, error) {
		req, err := newRequest(ctx, http.MethodPost, url, body)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		return req, nil
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkResponse(resp)
}

func newRequest(ctx context.Context, method, url string, body []byte) (*http.Request, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

// send retries transient failures: connection errors, 5xx, and 429 rate limits.
//
// GitHub returns 5xx often enough that a single attempt would regularly leave a
// commit with no status at all. Attempts are few and the backoff is short,
// because the caller is either a runner waiting to start or a background report.
// The request is rebuilt each time so its body can be read again.
func send(ctx context.Context, hc *http.Client, newReq func() (*http.Request, error)) (*http.Response, error) {
	const attempts = 3
	var lastErr error

	for attempt := range attempts {
		if attempt > 0 {
			delay := time.Duration(attempt) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
		req, err := newReq()
		if err != nil {
			return nil, err
		}
		resp, err := hc.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("github request: %w", err)
			continue
		}
		if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			return resp, nil
		}
		// Drain and close so the connection can be reused by the retry.
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		lastErr = fmt.Errorf("github returned %d: %s", resp.StatusCode, msg)
	}
	return nil, fmt.Errorf("after %d attempts: %w", attempts, lastErr)
}

func checkResponse(resp *http.Response) error {
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		// A rate-limited or permission error is worth naming precisely: these are
		// the two failures an operator will actually hit.
		if remaining := resp.Header.Get("X-RateLimit-Remaining"); remaining == "0" {
			reset, _ := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64)
			return fmt.Errorf("github rate limit exhausted until %s: %s",
				time.Unix(reset, 0).UTC().Format(time.RFC3339), msg)
		}
		return fmt.Errorf("github returned %d: %s", resp.StatusCode, msg)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

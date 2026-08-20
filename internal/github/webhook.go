// Package github handles the two directions of GitHub integration:
// inbound webhooks (verified, then parsed into a Job) and outbound commit statuses.
package github

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// VerifySignature checks the X-Hub-Signature-256 header against the shared secret.
// hmac.Equal is a constant-time compare: a naive == leaks the signature byte by byte.
func VerifySignature(secret string, body []byte, header string) error {
	if secret == "" {
		return fmt.Errorf("webhook secret is not configured")
	}
	if !strings.HasPrefix(header, "sha256=") {
		return fmt.Errorf("missing or malformed signature header")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	want := mac.Sum(nil)
	got, err := hex.DecodeString(strings.TrimPrefix(header, "sha256="))
	if err != nil {
		return fmt.Errorf("signature is not valid hex")
	}
	if !hmac.Equal(want, got) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

// Event is the small, trusted subset of a webhook payload that we act on.
// Everything else in the payload is ignored rather than stored.
type Event struct {
	Repository string // owner/name
	CloneURL   string
	CommitSHA  string
	Branch     string
	EventType  string
	PRNumber   int
}

type pushPayload struct {
	Ref        string `json:"ref"`
	After      string `json:"after"`
	Deleted    bool   `json:"deleted"`
	Repository struct {
		FullName string `json:"full_name"`
		CloneURL string `json:"clone_url"`
	} `json:"repository"`
}

type pullRequestPayload struct {
	Action      string `json:"action"`
	Number      int    `json:"number"`
	PullRequest struct {
		Head struct {
			SHA  string `json:"sha"`
			Ref  string `json:"ref"`
			Repo struct {
				FullName string `json:"full_name"`
				CloneURL string `json:"clone_url"`
			} `json:"repo"`
		} `json:"head"`
	} `json:"pull_request"`
}

// ErrIgnored means the payload was valid but is not something we build
// (a branch deletion, a PR comment edit, an unsupported event type).
var ErrIgnored = fmt.Errorf("event ignored")

func ParseEvent(eventType string, body []byte) (*Event, error) {
	switch eventType {
	case "push":
		var p pushPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("decode push payload: %w", err)
		}
		if p.Deleted || !strings.HasPrefix(p.Ref, "refs/heads/") {
			return nil, ErrIgnored
		}
		e := &Event{
			Repository: p.Repository.FullName,
			CloneURL:   p.Repository.CloneURL,
			CommitSHA:  p.After,
			Branch:     strings.TrimPrefix(p.Ref, "refs/heads/"),
			EventType:  "push",
		}
		return e, validate(e)

	case "pull_request":
		var p pullRequestPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("decode pull_request payload: %w", err)
		}
		switch p.Action {
		case "opened", "synchronize", "reopened":
		default:
			return nil, ErrIgnored
		}
		e := &Event{
			Repository: p.PullRequest.Head.Repo.FullName,
			CloneURL:   p.PullRequest.Head.Repo.CloneURL,
			CommitSHA:  p.PullRequest.Head.SHA,
			Branch:     p.PullRequest.Head.Ref,
			EventType:  "pull_request",
			PRNumber:   p.Number,
		}
		return e, validate(e)

	case "ping":
		return nil, ErrIgnored
	}
	return nil, ErrIgnored
}

// validate rejects payloads whose fields we would otherwise hand to git or Docker.
// Webhook bodies are attacker-influenced on a public repository.
func validate(e *Event) error {
	if !strings.Contains(e.Repository, "/") {
		return fmt.Errorf("invalid repository %q", e.Repository)
	}
	if len(e.CommitSHA) != 40 || strings.Trim(e.CommitSHA, "0123456789abcdef") != "" {
		return fmt.Errorf("invalid commit sha %q", e.CommitSHA)
	}
	if !strings.HasPrefix(e.CloneURL, "https://") {
		return fmt.Errorf("clone url must be https, got %q", e.CloneURL)
	}
	if e.Branch == "" {
		return fmt.Errorf("empty branch")
	}
	return nil
}

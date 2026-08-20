package github

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
)

func sign(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestVerifySignature(t *testing.T) {
	body := `{"hello":"world"}`
	if err := VerifySignature("s3cret", []byte(body), sign("s3cret", body)); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	bad := []struct{ name, secret, sig string }{
		{"wrong secret", "s3cret", sign("other", body)},
		{"tampered body", "s3cret", sign("s3cret", body+"x")},
		{"missing prefix", "s3cret", "deadbeef"},
		{"not hex", "s3cret", "sha256=zzzz"},
		{"empty secret", "", sign("s3cret", body)},
	}
	for _, c := range bad {
		if err := VerifySignature(c.secret, []byte(body), c.sig); err == nil {
			t.Errorf("%s: expected rejection", c.name)
		}
	}
}

const sha = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678"

func TestParsePush(t *testing.T) {
	body := `{"ref":"refs/heads/main","after":"` + sha + `","repository":{"full_name":"acme/app","clone_url":"https://github.com/acme/app.git"}}`
	e, err := ParseEvent("push", []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if e.Repository != "acme/app" || e.Branch != "main" || e.CommitSHA != sha {
		t.Errorf("parsed = %+v", e)
	}
}

func TestParsePullRequest(t *testing.T) {
	body := `{"action":"opened","number":42,"pull_request":{"head":{"sha":"` + sha + `","ref":"feature","repo":{"full_name":"acme/app","clone_url":"https://github.com/acme/app.git"}}}}`
	e, err := ParseEvent("pull_request", []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if e.PRNumber != 42 || e.Branch != "feature" || e.EventType != "pull_request" {
		t.Errorf("parsed = %+v", e)
	}
}

func TestIgnoredEvents(t *testing.T) {
	cases := map[string]string{
		"ping":        `{}`,
		"issues":      `{}`,
		"push_delete": `{"ref":"refs/heads/x","deleted":true,"after":"` + sha + `"}`,
		"pr_closed":   `{"action":"closed"}`,
	}
	types := map[string]string{"ping": "ping", "issues": "issues", "push_delete": "push", "pr_closed": "pull_request"}
	for name, body := range cases {
		if _, err := ParseEvent(types[name], []byte(body)); !errors.Is(err, ErrIgnored) {
			t.Errorf("%s: expected ErrIgnored, got %v", name, err)
		}
	}
}

func TestRejectsUntrustedFields(t *testing.T) {
	cases := []string{
		`{"ref":"refs/heads/main","after":"not-a-sha","repository":{"full_name":"acme/app","clone_url":"https://github.com/acme/app.git"}}`,
		`{"ref":"refs/heads/main","after":"` + sha + `","repository":{"full_name":"acme/app","clone_url":"file:///etc/passwd"}}`,
		`{"ref":"refs/heads/main","after":"` + sha + `","repository":{"full_name":"noslash","clone_url":"https://github.com/x.git"}}`,
	}
	for i, body := range cases {
		if _, err := ParseEvent("push", []byte(body)); err == nil || errors.Is(err, ErrIgnored) {
			t.Errorf("case %d: expected a validation error, got %v", i, err)
		}
	}
}

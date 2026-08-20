package github

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testKey returns a small RSA key. 1024 bits is far too weak for production and
// exactly right for a test: key generation dominates the runtime otherwise.
func testKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func testApp(t *testing.T, baseURL string) (*AppClient, *rsa.PrivateKey) {
	t.Helper()
	key := testKey(t)
	c := &AppClient{
		appID:     "12345",
		key:       key,
		base:      baseURL,
		http:      &http.Client{Timeout: 5 * time.Second},
		installs:  map[string]int64{},
		tokenPool: map[string]cachedToken{},
	}
	return c, key
}

// The JWT is hand-rolled, so its shape is worth asserting: GitHub rejects the
// whole exchange if the algorithm, claims or signature are wrong.
func TestAppJWTIsValidRS256(t *testing.T) {
	c, key := testApp(t, "https://example.invalid")

	token, err := c.appJWT()
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d segments, want 3", len(parts))
	}

	var header struct{ Alg, Typ string }
	decodeSegment(t, parts[0], &header)
	if header.Alg != "RS256" || header.Typ != "JWT" {
		t.Errorf("header = %+v", header)
	}

	var claims struct {
		Iat int64  `json:"iat"`
		Exp int64  `json:"exp"`
		Iss string `json:"iss"`
	}
	decodeSegment(t, parts[1], &claims)
	if claims.Iss != "12345" {
		t.Errorf("iss = %q, want the app id", claims.Iss)
	}
	if claims.Iat >= time.Now().Unix() {
		t.Error("iat must be backdated to survive clock skew")
	}
	// GitHub refuses anything longer than ten minutes.
	if life := claims.Exp - claims.Iat; life > 600 {
		t.Errorf("token lives %ds, GitHub allows at most 600", life)
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], sig); err != nil {
		t.Fatalf("signature does not verify: %v", err)
	}
}

// A clone token must be read-only and scoped to one repository: it is the only
// credential that leaves the control plane.
func TestCloneTokenIsScopedAndCached(t *testing.T) {
	var installLookups, tokenMints atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/repos/acme/widget/installation":
			installLookups.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 42})

		case r.URL.Path == "/app/installations/42/access_tokens":
			tokenMints.Add(1)
			var body struct {
				Repositories []string          `json:"repositories"`
				Permissions  map[string]string `json:"permissions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode token request: %v", err)
			}
			if len(body.Repositories) != 1 || body.Repositories[0] != "widget" {
				t.Errorf("token scoped to %v, want [widget]", body.Repositories)
			}
			if body.Permissions["contents"] != "read" {
				t.Errorf("permissions = %v, want contents:read", body.Permissions)
			}
			if _, ok := body.Permissions["statuses"]; ok {
				t.Error("a clone token must not carry write permissions")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"token":      "ghs_clone",
				"expires_at": time.Now().Add(time.Hour),
			})

		default:
			t.Errorf("unexpected request to %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c, _ := testApp(t, srv.URL)
	ctx := context.Background()

	for range 3 {
		token, err := c.CloneToken(ctx, "acme/widget")
		if err != nil {
			t.Fatal(err)
		}
		if token != "ghs_clone" {
			t.Fatalf("token = %q", token)
		}
	}
	// Three calls, one mint: an expensive exchange must not run per job.
	if got := tokenMints.Load(); got != 1 {
		t.Errorf("minted %d tokens, want 1 (caching is broken)", got)
	}
	if got := installLookups.Load(); got != 1 {
		t.Errorf("looked up the installation %d times, want 1", got)
	}
}

// An expired cached token must be replaced rather than handed out again.
func TestExpiredTokenIsRefreshed(t *testing.T) {
	var mints atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/installation") {
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 7})
			return
		}
		mints.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token": "ghs_fresh", "expires_at": time.Now().Add(time.Hour),
		})
	}))
	defer srv.Close()

	c, _ := testApp(t, srv.URL)
	c.tokenPool["clone:acme/widget"] = cachedToken{value: "ghs_stale", expires: time.Now().Add(-time.Minute)}

	token, err := c.CloneToken(context.Background(), "acme/widget")
	if err != nil {
		t.Fatal(err)
	}
	if token != "ghs_fresh" {
		t.Errorf("token = %q, want a freshly minted one", token)
	}
	if mints.Load() != 1 {
		t.Errorf("minted %d times, want 1", mints.Load())
	}
}

// GitHub 5xx responses are common enough that a single attempt would regularly
// lose a commit status.
func TestStatusRetriesServerErrors(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	c := New("pat")
	c.base = srv.URL
	if err := c.SetStatus(context.Background(), "acme/widget", "deadbeef", "success", "ok", ""); err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	if attempts.Load() != 3 {
		t.Errorf("made %d attempts, want 3", attempts.Load())
	}
}

// A 4xx is the caller's fault and must fail immediately, not three times.
func TestStatusDoesNotRetryClientErrors(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := New("pat")
	c.base = srv.URL
	if err := c.SetStatus(context.Background(), "acme/gone", "deadbeef", "success", "ok", ""); err == nil {
		t.Fatal("expected an error for 404")
	}
	if attempts.Load() != 1 {
		t.Errorf("made %d attempts, want 1", attempts.Load())
	}
}

// A personal access token is too broad to hand to a runner; only an App can mint
// a clone credential.
func TestPersonalAccessTokenMintsNoCloneToken(t *testing.T) {
	token, err := New("pat").CloneToken(context.Background(), "acme/widget")
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		t.Errorf("PAT client returned a clone token %q", token)
	}
}

func TestConfigurePrefersTheApp(t *testing.T) {
	key := testKey(t)
	pkcs1 := pem.EncodeToMemory(&pem.Block{
		Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	pkcs8 := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8Bytes})

	cases := []struct {
		name string
		opts Options
		want string
	}{
		{"app wins over token", Options{AppID: "1", PrivateKeyPEM: string(pkcs1), Token: "pat"}, "github-app"},
		{"pkcs8 keys work too", Options{AppID: "1", PrivateKeyPEM: string(pkcs8)}, "github-app"},
		{"token is the fallback", Options{Token: "pat"}, "personal-access-token"},
		{"nothing configured", Options{}, "disabled"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, mode, err := Configure(tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			if mode != tc.want {
				t.Errorf("mode = %q, want %q", mode, tc.want)
			}
			if client == nil {
				t.Error("client is nil")
			}
		})
	}

	if _, _, err := Configure(Options{AppID: "1", PrivateKeyPEM: "not a key"}); err == nil {
		t.Error("a malformed private key must fail loudly at startup")
	}
}

func decodeSegment(t *testing.T, segment string, out any) {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		t.Fatalf("segment is not base64url: %v", err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatalf("segment is not JSON: %v", err)
	}
}

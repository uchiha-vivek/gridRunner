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
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// AppClient authenticates as a GitHub App.
//
// The exchange has two steps. A JWT signed with the app's private key proves we
// are the app; it is only good for app-level endpoints and lasts ten minutes. To
// touch a repository we swap that JWT for an installation token, which is what
// actually authorises status writes and clones.
//
// This is worth the extra machinery over a personal access token because the
// installation token is short lived (one hour), scoped to a single repository,
// and carries only the permissions we ask for. That is what makes it safe to hand
// a clone credential to a runner: see CloneToken.
type AppClient struct {
	appID string
	key   *rsa.PrivateKey
	base  string
	http  *http.Client

	mu        sync.Mutex
	installs  map[string]int64       // repo -> installation id
	tokenPool map[string]cachedToken // cache key -> installation token
}

type cachedToken struct {
	value   string
	expires time.Time
}

// Permissions we request. Keeping these separate is the point: the control plane
// needs to write commit statuses, a runner only needs to read code, and no token
// we mint carries more than its job requires.
var (
	controlPlanePerms = map[string]string{"contents": "read", "statuses": "write"}
	clonePerms        = map[string]string{"contents": "read"}
)

// NewApp builds a client from an app id and a PEM-encoded RSA private key.
func NewApp(appID, privateKeyPEM string) (*AppClient, error) {
	key, err := parsePrivateKey(privateKeyPEM)
	if err != nil {
		return nil, err
	}
	if appID == "" {
		return nil, fmt.Errorf("github app id is empty")
	}
	return &AppClient{
		appID:     appID,
		key:       key,
		base:      "https://api.github.com",
		http:      &http.Client{Timeout: 10 * time.Second},
		installs:  map[string]int64{},
		tokenPool: map[string]cachedToken{},
	}, nil
}

// SetStatus posts a commit status using an installation token for that repository.
func (c *AppClient) SetStatus(ctx context.Context, repo, sha, state, description, targetURL string) error {
	token, err := c.token(ctx, repo, "status:"+repo, controlPlanePerms)
	if err != nil {
		return fmt.Errorf("installation token for %s: %w", repo, err)
	}
	return postStatus(ctx, c.http, c.base, token, repo, sha, state, description, targetURL)
}

// CloneToken mints a read-only, repository-scoped credential a runner can use to
// check out a private repository.
//
// It expires in an hour and can do nothing but read this one repository's code,
// which is what makes handing it to the data plane acceptable. It is still a
// secret: it goes to the runner host, never into a job container.
func (c *AppClient) CloneToken(ctx context.Context, repo string) (string, error) {
	return c.token(ctx, repo, "clone:"+repo, clonePerms)
}

// token returns a cached installation token or mints a new one. Tokens are cached
// per (repository, permission set) and refreshed two minutes early, so a token
// never expires mid-clone.
func (c *AppClient) token(ctx context.Context, repo, cacheKey string, perms map[string]string) (string, error) {
	c.mu.Lock()
	if t, ok := c.tokenPool[cacheKey]; ok && time.Now().Before(t.expires) {
		c.mu.Unlock()
		return t.value, nil
	}
	c.mu.Unlock()

	installID, err := c.installationID(ctx, repo)
	if err != nil {
		return "", err
	}
	name := repo
	if i := strings.Index(repo, "/"); i >= 0 {
		name = repo[i+1:]
	}
	body, _ := json.Marshal(map[string]any{
		"repositories": []string{name},
		"permissions":  perms,
	})

	jwt, err := c.appJWT()
	if err != nil {
		return "", err
	}
	url := fmt.Sprintf("%s/app/installations/%d/access_tokens", c.base, installID)

	var out struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := c.call(ctx, http.MethodPost, url, jwt, body, &out); err != nil {
		return "", err
	}
	if out.Token == "" {
		return "", fmt.Errorf("github returned an empty installation token")
	}

	c.mu.Lock()
	c.tokenPool[cacheKey] = cachedToken{value: out.Token, expires: out.ExpiresAt.Add(-2 * time.Minute)}
	c.mu.Unlock()
	return out.Token, nil
}

// installationID finds which installation of the app covers a repository.
//
// The webhook payload also carries installation.id, but looking it up keeps the
// job model free of GitHub-specific plumbing and self-corrects if an app is
// reinstalled. The answer never changes in practice, so it is cached for the
// lifetime of the process.
func (c *AppClient) installationID(ctx context.Context, repo string) (int64, error) {
	c.mu.Lock()
	if id, ok := c.installs[repo]; ok {
		c.mu.Unlock()
		return id, nil
	}
	c.mu.Unlock()

	jwt, err := c.appJWT()
	if err != nil {
		return 0, err
	}
	var out struct {
		ID int64 `json:"id"`
	}
	if err := c.call(ctx, http.MethodGet, c.base+"/repos/"+repo+"/installation", jwt, nil, &out); err != nil {
		return 0, err
	}
	if out.ID == 0 {
		return 0, fmt.Errorf("the app is not installed on %s", repo)
	}
	c.mu.Lock()
	c.installs[repo] = out.ID
	c.mu.Unlock()
	return out.ID, nil
}

// appJWT builds and signs the app-level JSON Web Token.
//
// It is written by hand rather than pulling in a JWT library: RS256 here is a
// SHA-256 hash and an RSA signature over two base64url segments, and we only ever
// sign, never verify. A dependency whose job is parsing attacker-supplied tokens
// would be a bigger liability than these twenty lines.
func (c *AppClient) appJWT() (string, error) {
	now := time.Now()
	// iat is backdated a minute because GitHub rejects tokens issued in the
	// future and small clock skew between hosts is normal. exp must be <= 10 min.
	claims := fmt.Sprintf(`{"iat":%d,"exp":%d,"iss":%q}`,
		now.Add(-60*time.Second).Unix(), now.Add(9*time.Minute).Unix(), c.appID)

	signing := encodeSegment([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." + encodeSegment([]byte(claims))
	digest := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, c.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign app jwt: %w", err)
	}
	return signing + "." + encodeSegment(sig), nil
}

// call performs one authenticated app-level request, decoding JSON into out.
func (c *AppClient) call(ctx context.Context, method, url, jwt string, body []byte, out any) error {
	resp, err := send(ctx, c.http, func() (*http.Request, error) {
		req, err := newRequest(ctx, method, url, body)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+jwt)
		return req, nil
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := checkResponse(resp); err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func encodeSegment(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// parsePrivateKey accepts the PKCS#1 key GitHub hands out and the PKCS#8 form
// produced by openssl conversions, because both are common in the wild.
func parsePrivateKey(data string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(data))
	if block == nil {
		return nil, fmt.Errorf("github app private key is not valid PEM")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse github app private key: %w", err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("github app private key must be RSA, got %T", parsed)
	}
	return key, nil
}

// Options describes the GitHub credentials a deployment has available.
type Options struct {
	AppID          string
	PrivateKeyPEM  string // the key inline, for environments that inject secrets as env vars
	PrivateKeyPath string // or a path to the .pem file GitHub gives you
	Token          string // personal access token fallback
}

// Configure picks the strongest authentication available: a GitHub App if one is
// configured, a personal access token if not, and a no-op client when neither is
// set, so local development needs no GitHub credentials at all. The returned
// string names the mode for the startup log.
func Configure(o Options) (Client, string, error) {
	keyPEM := o.PrivateKeyPEM
	if keyPEM == "" && o.PrivateKeyPath != "" {
		data, err := os.ReadFile(o.PrivateKeyPath)
		if err != nil {
			return nil, "", fmt.Errorf("read github app private key: %w", err)
		}
		keyPEM = string(data)
	}
	if o.AppID != "" && keyPEM != "" {
		app, err := NewApp(o.AppID, keyPEM)
		if err != nil {
			return nil, "", err
		}
		return app, "github-app", nil
	}
	if o.Token != "" {
		return New(o.Token), "personal-access-token", nil
	}
	return NoopClient{}, "disabled", nil
}

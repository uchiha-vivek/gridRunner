package runner

import (
	"encoding/base64"
	"strings"
	"testing"
)

// The clone credential must be passed per command, never written into the
// workspace: the workspace is bind-mounted into the container that runs untrusted
// repository code.
func TestAuthArgsKeepTheTokenOutOfTheWorkspace(t *testing.T) {
	if args := authArgs(""); args != nil {
		t.Errorf("a public checkout needs no credentials, got %v", args)
	}

	args := authArgs("ghs_secret")
	if len(args) != 2 || args[0] != "-c" {
		t.Fatalf("args = %v, want a -c override", args)
	}
	if !strings.HasPrefix(args[1], "http.extraheader=AUTHORIZATION: basic ") {
		t.Fatalf("args = %v, want an extraheader override", args)
	}
	// http.extraheader applies to this invocation only. A credential in the
	// remote URL would be persisted to .git/config, inside the workspace.
	if strings.Contains(args[1], "url.") || strings.Contains(args[1], "credential.helper") {
		t.Error("the token must not be written to git configuration")
	}

	encoded := strings.TrimPrefix(args[1], "http.extraheader=AUTHORIZATION: basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("header is not base64: %v", err)
	}
	if string(decoded) != "x-access-token:ghs_secret" {
		t.Errorf("credential = %q", decoded)
	}
}

func TestGitPrependsAuthOverrides(t *testing.T) {
	cmd := git(authArgs("tok"), "fetch", "origin")
	if cmd[0] != "git" || cmd[1] != "-c" || cmd[3] != "fetch" {
		t.Fatalf("command = %v, want overrides before the subcommand", cmd)
	}
}

// git failures are written to the build log, so they must not leak the token.
func TestRedactHidesTheToken(t *testing.T) {
	msg := "fatal: could not read Authorization for ghs_secret"
	if got := redact(msg, "ghs_secret"); strings.Contains(got, "ghs_secret") {
		t.Errorf("redact left the token in %q", got)
	}
	if got := redact(msg, ""); got != msg {
		t.Errorf("redact changed the message when there was no token: %q", got)
	}
}

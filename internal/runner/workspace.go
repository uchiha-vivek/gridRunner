package runner

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// checkout describes one repository checkout.
type checkout struct {
	Root      string // where workspaces live
	JobID     string
	CloneURL  string
	CommitSHA string
	Branch    string
	Token     string // short-lived credential for a private repository, or empty
}

// prepareWorkspace creates a fresh directory and checks out exactly one commit.
//
// The workspace is per job and deleted afterwards: a runner never carries
// repository state from one job to the next, so a malicious PR cannot leave
// anything behind for the next build to pick up.
func prepareWorkspace(ctx context.Context, c checkout) (string, error) {
	dir := filepath.Join(c.Root, "forgerun-"+c.JobID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create workspace: %w", err)
	}

	steps := [][]string{
		{"git", "init", "-q"},
		{"git", "remote", "add", "origin", c.CloneURL},
	}
	for _, s := range steps {
		if out, err := run(ctx, dir, s...); err != nil {
			os.RemoveAll(dir)
			return "", fmt.Errorf("%s: %w: %s", strings.Join(s, " "), err, out)
		}
	}

	auth := authArgs(c.Token)

	// Fetching the exact SHA is the cheapest correct checkout. Some remotes
	// refuse it, so fall back to a shallow fetch of the branch.
	if _, err := run(ctx, dir, git(auth, "fetch", "-q", "--depth", "1", "origin", c.CommitSHA)...); err != nil {
		if out, err := run(ctx, dir, git(auth, "fetch", "-q", "--depth", "50", "origin", c.Branch)...); err != nil {
			os.RemoveAll(dir)
			return "", fmt.Errorf("fetch %s: %w: %s", c.Branch, err, redact(out, c.Token))
		}
	}
	if out, err := run(ctx, dir, git(auth, "checkout", "-q", c.CommitSHA)...); err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("checkout %s: %w: %s", c.CommitSHA, err, redact(out, c.Token))
	}
	return dir, nil
}

// authArgs turns a clone token into git configuration for this invocation only.
//
// The token is passed with -c rather than embedded in the remote URL on purpose:
// a URL credential is written into .git/config, and the workspace is bind-mounted
// into the job container, which runs untrusted repository code. Anything on disk
// in the workspace should be assumed readable by the build. Passing it per
// command keeps it in the runner process and out of the container.
func authArgs(token string) []string {
	if token == "" {
		return nil
	}
	basic := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + token))
	return []string{"-c", "http.extraheader=AUTHORIZATION: basic " + basic}
}

func git(auth []string, args ...string) []string {
	cmd := make([]string, 0, 1+len(auth)+len(args))
	cmd = append(cmd, "git")
	cmd = append(cmd, auth...)
	return append(cmd, args...)
}

// redact keeps a credential out of an error message that will be logged and
// shown in the build output.
func redact(s, token string) string {
	if token == "" {
		return s
	}
	return strings.ReplaceAll(s, token, "***")
}

func run(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Dir = dir
	// Never prompt for credentials: a hung git prompt would block the runner.
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

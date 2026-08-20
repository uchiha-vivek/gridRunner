package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

// DockerExecutor runs each job in a fresh container that is destroyed afterwards.
//
// SECURITY: repository code is untrusted. Every container therefore gets
//   - no network by default (DOCKER_NETWORK=none)
//   - a hard CPU and memory cap, so one job cannot starve the host
//   - a wall-clock timeout enforced by the caller's context
//   - no-new-privileges and a dropped capability set
//   - no Docker socket, and no host paths beyond the job's own workspace
//
// This is isolation, not a sandbox: a container shares the host kernel, so a
// kernel exploit escapes it. See docs/security.md.
type DockerExecutor struct {
	cli *client.Client
}

func NewDocker() (*DockerExecutor, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("connect to docker: %w", err)
	}
	return &DockerExecutor{cli: cli}, nil
}

func (d *DockerExecutor) Ping(ctx context.Context) error {
	_, err := d.cli.Ping(ctx)
	return err
}

func (d *DockerExecutor) Close() error { return d.cli.Close() }

func (d *DockerExecutor) Execute(ctx context.Context, req Request, logs io.Writer) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()

	if err := d.ensureImage(ctx, req.Spec.Image, logs); err != nil {
		return Result{}, err
	}

	// "set -e" makes the container exit at the first failing command, which is
	// what a CI user expects from a list of steps.
	script := "set -e\n" + strings.Join(req.Spec.Commands, "\n")

	env := make([]string, 0, len(req.Spec.Env)+4)
	for k, v := range req.Spec.Env {
		env = append(env, k+"="+v)
	}
	// Only non-secret job metadata crosses into the container. GitHub tokens and
	// database URLs stay in the control plane and the runner process.
	env = append(env,
		"CI=true",
		"FORGERUN_JOB_ID="+req.Job.ID,
		"FORGERUN_COMMIT="+req.Job.CommitSHA,
		"FORGERUN_BRANCH="+req.Job.Branch,
	)

	created, err := d.cli.ContainerCreate(ctx,
		&container.Config{
			Image:      req.Spec.Image,
			Cmd:        []string{"/bin/sh", "-c", script},
			WorkingDir: "/workspace",
			Env:        env,
			Tty:        false,
		},
		&container.HostConfig{
			Mounts: []mount.Mount{{
				Type:   mount.TypeBind,
				Source: req.Workspace,
				Target: "/workspace",
			}},
			NetworkMode: container.NetworkMode(req.Network),
			Resources: container.Resources{
				NanoCPUs:  int64(req.CPUs * 1e9),
				Memory:    req.MemoryMB * 1024 * 1024,
				PidsLimit: ptr(int64(512)),
			},
			SecurityOpt: []string{"no-new-privileges"},
			CapDrop:     []string{"ALL"},
			AutoRemove:  false, // we remove it ourselves so logs are never truncated
		},
		nil, nil, "forgerun-"+req.Job.ID)
	if err != nil {
		return Result{}, fmt.Errorf("create container: %w", err)
	}

	// Cleanup runs even if the job is cancelled or the context expired, using a
	// fresh context so the removal itself is not cancelled.
	defer func() {
		rmCtx, rmCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer rmCancel()
		_ = d.cli.ContainerRemove(rmCtx, created.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})
	}()

	if err := d.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return Result{}, fmt.Errorf("start container: %w", err)
	}

	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		out, err := d.cli.ContainerLogs(ctx, created.ID, container.LogsOptions{
			ShowStdout: true, ShowStderr: true, Follow: true,
		})
		if err != nil {
			fmt.Fprintf(logs, "forgerun: cannot stream logs: %v\n", err)
			return
		}
		defer out.Close()
		// Without a TTY, Docker multiplexes stdout and stderr; StdCopy demuxes
		// them back into one readable stream.
		if _, err := stdcopy.StdCopy(logs, logs, out); err != nil && ctx.Err() == nil {
			fmt.Fprintf(logs, "forgerun: log stream ended: %v\n", err)
		}
	}()

	statusCh, errCh := d.cli.ContainerWait(context.WithoutCancel(ctx), created.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		<-streamDone
		return Result{}, fmt.Errorf("wait for container: %w", err)

	case status := <-statusCh:
		<-streamDone
		return Result{ExitCode: int(status.StatusCode)}, nil

	case <-ctx.Done():
		<-streamDone
		// Two different things end a build early, and the log should say which:
		// the deadline we set, or a cancellation from the control plane.
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			fmt.Fprintf(logs, "\nforgerun: job exceeded its %s timeout, killing container\n", req.Timeout)
			return Result{ExitCode: -1, TimedOut: true}, nil
		}
		fmt.Fprint(logs, "\nforgerun: job stopped, killing container\n")
		return Result{ExitCode: -1}, ctx.Err()
	}
}

// ensureImage pulls the image only when it is missing, so repeat jobs start fast.
func (d *DockerExecutor) ensureImage(ctx context.Context, ref string, logs io.Writer) error {
	list, err := d.cli.ImageList(ctx, image.ListOptions{
		Filters: filters.NewArgs(filters.Arg("reference", ref)),
	})
	if err != nil {
		return fmt.Errorf("list images: %w", err)
	}
	if len(list) > 0 {
		return nil
	}
	fmt.Fprintf(logs, "forgerun: pulling image %s\n", ref)
	rc, err := d.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("pull image %s: %w", ref, err)
	}
	defer rc.Close()
	// The pull progress stream must be drained for the pull to finish.
	_, err = io.Copy(io.Discard, rc)
	return err
}

func ptr[T any](v T) *T { return &v }

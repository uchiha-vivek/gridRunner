# ForgeRun reference

The full detail: API surface, design decisions, configuration and extension
points. For the commands to run it, see the [README](../README.md).

A self-hosted CI/CD runner orchestration platform. A GitHub webhook becomes a job,
the job is queued in Redis, a scheduler assigns it to a runner that matches its
labels, and the runner executes it inside a throwaway Docker container and reports
the result back to GitHub.

No GitHub-hosted runners are involved. Redis and PostgreSQL run only in Docker
Compose — nothing is installed on your host.

```
GitHub ──webhook──▶ API ──▶ Postgres (job row)
                     │
                     └──▶ Redis queue ──▶ Scheduler ──▶ runner mailbox
                                                             │
                                            runner long-polls the API
                                                             │
                                      git checkout ─▶ Docker container ─▶ exit code
                                                             │
                          logs + result ──▶ API ──▶ Postgres ──▶ GitHub commit status
```

## Requirements

- Go 1.25+ (an older Go works too: the default `GOTOOLCHAIN=auto` fetches the
  toolchain go.mod asks for)
- Docker (Docker Desktop on Windows/macOS) — for Redis, Postgres and job containers
- git on the machine running the runner agent

## Quick start

```bash
cp .env.example .env          # optional: only needed for GitHub integration
docker compose up -d          # Postgres, Redis, API, scheduler
go run ./cmd/runner           # the runner agent runs on your machine
```

The runner stays on the host because it drives the Docker engine directly. Check
that everything is alive:

```bash
curl localhost:8080/health          # {"status":"ok"}
curl localhost:8080/ready           # postgres + redis probes
curl localhost:8080/api/v1/runners  # your runner, status IDLE
```

### Running the control plane from source instead

```bash
docker compose up -d postgres redis   # dependencies only
go run ./cmd/api
go run ./cmd/scheduler
go run ./cmd/runner
```

`.env.example` already points at `localhost` for this mode.

### Make targets

| Command | What it does |
|---|---|
| `make up` | Build and start the control plane in the background |
| `make dev` | Same, then tail the API and scheduler logs |
| `make down` | Stop everything and delete the volumes |
| `make build` | Build all three binaries into `./bin` |
| `make test` | Unit tests (no infrastructure needed) |
| `make test-integration` | End-to-end test (needs `make up` first) |
| `make migrate-down` | Undo the last migration (`STEPS=2` for more). Drops data. |
| `make lint` | `go vet` plus a gofmt check |

On Windows without `make`, run the underlying commands directly — they are all
plain `go` and `docker compose` invocations.

## Triggering a build

### From GitHub

Add a webhook to your repository:

- Payload URL: `https://<your-host>/webhooks/github`
- Content type: `application/json`
- Secret: the same value as `GITHUB_WEBHOOK_SECRET`
- Events: pushes and pull requests

Put a `forgerun.yml` in the repository root:

```yaml
jobs:
  test:
    image: node:22
    commands:
      - npm install
      - npm test
```

Push a commit and the job appears in `GET /api/v1/jobs`.

### Private repositories

Checking out private code needs a GitHub App, not a personal access token.
Create one, give it **Contents: read** and **Commit statuses: read and write**,
install it on the repositories you want built, then:

```bash
GITHUB_APP_ID=123456
GITHUB_APP_PRIVATE_KEY_PATH=/etc/forgerun/app.pem
```

The control plane mints a fresh token per job: scoped to that one repository,
read-only, and valid for an hour. It goes to the runner, never into the job
container. A personal access token is deliberately never used for cloning — see
[docs/security.md](security.md).

### Without GitHub

Send a signed webhook yourself:

```bash
BODY='{"ref":"refs/heads/main","after":"<40-char-sha>","repository":{"full_name":"you/repo","clone_url":"https://github.com/you/repo.git"}}'
SIG="sha256=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$GITHUB_WEBHOOK_SECRET" | awk '{print $2}')"

curl -X POST localhost:8080/webhooks/github \
  -H "X-GitHub-Event: push" \
  -H "X-Hub-Signature-256: $SIG" \
  -H "Content-Type: application/json" \
  -d "$BODY"
```

Then watch it: `curl localhost:8080/api/v1/jobs` and
`curl localhost:8080/api/v1/jobs/<id>/logs`.

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Liveness. Never touches a dependency. |
| GET | `/ready` | Readiness. Probes Postgres and Redis. |
| POST | `/webhooks/github` | Signed GitHub webhook intake. |
| POST | `/api/v1/runners/register` | Runner joins the pool, receives an id and token. |
| POST | `/api/v1/runners/{id}/heartbeat` | Liveness and lease renewal. The response carries `cancel` when the runner must stop its job. |
| GET | `/api/v1/runners/{id}/job` | Long-poll for assigned work, with a clone credential if the repository needs one. |
| GET | `/api/v1/runners` | List runners. |
| GET | `/api/v1/runners/{id}` | One runner. |
| GET | `/api/v1/jobs` | List jobs (`?limit=`). |
| GET | `/api/v1/jobs/{id}` | One job. |
| GET | `/api/v1/jobs/{id}/logs` | Plain-text build log. |
| POST | `/api/v1/jobs/{id}/cancel` | Cancel a job. A running container is killed within one heartbeat. |
| POST | `/api/v1/jobs/{id}/start` | Runner reports it started. |
| POST | `/api/v1/jobs/{id}/logs` | Runner ships a log chunk (`X-Log-Seq` makes it idempotent). |
| POST | `/api/v1/jobs/{id}/result` | Runner reports the exit code. |

Runner endpoints require `Authorization: Bearer <token>` and `X-Runner-ID`.
Registration requires `X-Registration-Token`.

## How it fits together

**Control plane** — `cmd/api` and `cmd/scheduler`. Owns Postgres, Redis and all
job metadata. Runs anywhere.

**Data plane** — `cmd/runner`. Owns nothing but a Docker socket and a temp
directory. Runs on the machines that execute builds.

The two only ever talk over HTTP, so runners need no database credentials, no
Redis access and no inbound connectivity. Adding a machine to the pool is one
command:

```bash
go run ./cmd/runner --server http://control-plane:8080 --labels linux,arm64,gpu
```

### Job lifecycle

```
QUEUED ──▶ ASSIGNED ──▶ RUNNING ──▶ SUCCESS
   │           │            │
   │           └────────────┴──▶ FAILED
   │           │            │
   │           └────────────┴──▶ QUEUED   (runner died; job is rescued)
   └────────────────────────────▶ CANCELLED
```

Every transition is checked against the state machine in `internal/models` and
applied with an optimistic `WHERE status = <expected>` clause, so two components
can never disagree about a job.

### Where to extend it

| Want to add | Implement | Nothing else changes |
|---|---|---|
| Kafka / SQS instead of Redis | `queue.Queue` | scheduler, API |
| Kubernetes / Firecracker execution | `executor.Executor` | runner agent |
| Bin-packing or priority scheduling | `scheduler.Scheduler` | everything else |
| Checks API instead of commit statuses | `github.Client` | API handlers |
| GPU / ARM / Windows pools | nothing — they are labels | — |

## Configuration

Everything comes from environment variables; see `.env.example`. The important
ones:

| Variable | Meaning |
|---|---|
| `DATABASE_URL`, `REDIS_URL` | Control-plane dependencies |
| `GITHUB_WEBHOOK_SECRET` | Webhook HMAC secret. Unsigned deliveries are refused. |
| `GITHUB_APP_ID` + `GITHUB_APP_PRIVATE_KEY_PATH` | GitHub App credentials. Required for private repositories. |
| `GITHUB_TOKEN` | Personal access token fallback: commit statuses only, public repositories only. |
| `RUNNER_REGISTRATION_TOKEN` | Shared secret a runner needs to join |
| `HEARTBEAT_TIMEOUT` | Silence after which a runner is OFFLINE and its job requeued |
| `JOB_TIMEOUT`, `JOB_CPUS`, `JOB_MEMORY_MB` | Per-container limits |
| `DOCKER_NETWORK` | Network mode for job containers. `none` by default. |

## Testing

```bash
make test                                  # unit tests, no infrastructure
docker compose up -d postgres redis
make test-integration                      # job -> Redis -> scheduler -> runner -> result
```

The end-to-end tests create a real git repository, push it through the whole
pipeline with a stub executor, and assert: a job ends SUCCESS with logs and a
freed runner; a non-zero exit becomes FAILED; cancelling a *running* job stops the
executor and frees the runner; and a replayed log chunk is stored once. Queue
tests run against the compose Redis and skip when it is absent.

To exercise real containers as well (workspace mounting, `set -e`, exit codes,
timeout kills, and the full pipeline through an actual container):

```bash
FORGERUN_DOCKER=1 FORGERUN_INTEGRATION=1 go test ./tests/... -v
```

## Troubleshooting

**`unknown command 'blmove'`** — something other than the compose Redis is
answering on your host port. ForgeRun needs Redis 6.2+; an older Redis already
listening on 6379 will shadow the container. Set `REDIS_PORT=6380` and
`REDIS_URL=redis://localhost:6380/0` in `.env`, then `docker compose up -d`.

**Postgres authentication fails** — same cause: a local PostgreSQL on that port.
Set `POSTGRES_PORT` to a free port and update `DATABASE_URL` to match.

**Jobs stay QUEUED** — no runner is registered, or none carries the job's labels.
Check `curl localhost:8080/api/v1/runners` and the scheduler logs.

**A job fails with `cannot read forgerun.yml`** — the repository has no build
definition at its root. That is the expected result, not a bug.

## Security

CI runs untrusted code by design: a pull request can contain anything. Read
[docs/security.md](security.md) before exposing this to the internet — in
particular, container isolation is not a VM sandbox, and that document also lists
the known gaps this MVP deliberately leaves open.

## Further reading

- [docs/architecture.md](architecture.md) — how the pieces fit, and why each
  design choice was made
- [docs/security.md](security.md) — the threat model and the limits of
  container isolation

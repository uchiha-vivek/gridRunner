# ForgeRun — commands

## Run everything in Docker

```bash
docker compose up -d            # postgres, redis, api, scheduler
go run ./cmd/runner             # runner stays on the host (needs Docker)
docker compose up -d --build    # rebuild images after a code change
```

## Run the control plane from source

```bash
docker compose up -d postgres redis
go run ./cmd/api
go run ./cmd/scheduler
go run ./cmd/runner
```

## Health and status

```bash
curl localhost:8080/health
curl localhost:8080/ready
curl localhost:8080/api/v1/runners
curl localhost:8080/api/v1/jobs
```

## Jobs

```bash
curl localhost:8080/api/v1/jobs/<id>
curl localhost:8080/api/v1/jobs/<id>/logs
curl -X POST localhost:8080/api/v1/jobs/<id>/cancel
```

## Send a webhook without GitHub

```bash
BODY='{"ref":"refs/heads/main","after":"<40-char-sha>","repository":{"full_name":"you/repo","clone_url":"https://github.com/you/repo.git"}}'
SIG="sha256=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$GITHUB_WEBHOOK_SECRET" | awk '{print $2}')"

curl -X POST localhost:8080/webhooks/github \
  -H "X-GitHub-Event: push" \
  -H "X-Hub-Signature-256: $SIG" \
  -H "Content-Type: application/json" \
  -d "$BODY"
```

## Add a runner on another machine

```bash
go run ./cmd/runner --server http://<control-plane>:8080 --labels linux,amd64,docker
```

## Tests

```bash
go test ./...                                                # unit only
docker compose up -d postgres redis
FORGERUN_INTEGRATION=1 go test ./tests/...                   # full pipeline
FORGERUN_INTEGRATION=1 FORGERUN_DOCKER=1 go test ./tests/... # + real containers
```

## Build and lint

```bash
go build -o bin/ ./cmd/...
go vet ./...
gofmt -l cmd internal tests
```

## Migrations

```bash
go run ./cmd/api                  # migrations apply on startup
go run ./cmd/api --rollback=1     # undo the last migration
```

## Make targets

```bash
make up
make dev
make down
make build
make test
make test-integration
make migrate-down
make lint
```

## Logs, stop, reset

```bash
docker compose logs -f api scheduler
docker compose ps
docker compose down       # stop, keep data
docker compose down -v    # stop and wipe data
```

## Configuration

```bash
cp .env.example .env
```

## Docs

```bash
docs/reference.md
docs/architecture.md
docs/security.md
```

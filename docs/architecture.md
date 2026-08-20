# Architecture

## The pipeline

```
GitHub ──POST /webhooks/github──▶  API
                                    │  verify HMAC, parse, validate
                                    ▼
                              jobs table (QUEUED)
                                    │
                                    ▼
                        Redis list: forgerun:jobs:pending
                                    │  BLMOVE (atomic)
                                    ▼
                        Redis list: forgerun:jobs:inflight
                                    │
                                Scheduler
                                    │  label match + least recently used
                                    ▼
                     jobs.status = ASSIGNED, runner.status = BUSY
                                    │
                       Redis list: forgerun:runner:<id>:mailbox
                                    │  runner long-polls the API (25s)
                                    ▼
                               Runner agent
                                    │  git checkout into a fresh workspace
                                    │  read forgerun.yml
                                    ▼
                          ephemeral Docker container
                                    │  stdout/stderr, batched every 500ms
                                    ▼
                      POST /jobs/{id}/logs, /jobs/{id}/result
                                    │
                                    ▼
                       jobs.status = SUCCESS | FAILED
                                    │
                                    ▼
                        GitHub commit status API
```

## Control plane vs data plane

| | Control plane | Data plane |
|---|---|---|
| Processes | `cmd/api`, `cmd/scheduler` | `cmd/runner` |
| Owns | Postgres, Redis, all job metadata | Docker engine, a temp directory |
| Trust | Handles secrets | Handles untrusted code |
| Talks to | Each other, GitHub | Only the API, over HTTP |

A runner has no database credentials, no Redis access and needs no inbound
connectivity. That is what makes "add a machine to the pool" a single command, and
what keeps a compromised runner from reaching the rest of the system.

## Decisions worth knowing

**Job delivery is a long poll, not a push.** The scheduler writes the job id to a
per-runner Redis mailbox; the runner holds a 25-second `GET .../job` open and the
API drains the mailbox on its behalf. Pushing to runners would require every
runner to be addressable, which fails the moment one sits behind NAT. Letting
runners read Redis directly would put infrastructure credentials in the untrusted
half of the system.

**The heartbeat is the lease, and the only way back into a busy runner.** A
runner that is executing a job has stopped polling for work, so there is no open
channel to push a cancellation down. Rather than add one, the heartbeat carries
the job id the runner believes it owns, and the response can say "stop". That
makes cancelling a running build kill the container within one heartbeat
(seconds, not the job timeout), and it makes a requeue safe: a runner the reaper
declared dead is told to abandon its job the moment it comes back, so two runners
never execute the same job. Every connection stays outbound.

**The queue is two Redis lists, not Streams.** `BLMOVE` atomically moves a job id
from `pending` to `inflight`, so a scheduler crash between dequeue and assignment
loses nothing: `Recover()` moves the in-flight entries back at startup. Streams
with consumer groups are the right answer for several competing schedulers; for
one scheduler process they add moving parts without adding safety. The `Queue`
interface means switching later touches one file.

**Every state change is guarded twice.** In Go by the state machine
(`internal/models`), and in SQL by `WHERE status = <the value we read>`. The first
catches logic errors; the second catches races between the scheduler, the runner
and a user pressing cancel. A job that changed underneath you produces an error,
never a silent overwrite.

**Scheduling is label matching plus least-recently-used.** Deliberately not
first-fit: LRU spreads load and makes a broken runner obvious instead of it
absorbing every job. Runner pools — GPU, ARM, Windows — are just labels, so
adding one needs no scheduler change.

**Idempotency lives in the database.** A partial unique index over
`(repository, commit_sha, event_type)` for unfinished jobs makes a webhook
redelivery return `duplicate` instead of building the same commit twice. Log
chunks carry a sequence number for the same reason at a smaller scale: a chunk
the runner re-sends after a timeout is stored once.

**GitHub credentials are minted per job, not shared.** With a GitHub App
configured, a JWT signed with the app key is exchanged for an installation token
scoped to a single repository. The control plane keeps one with `statuses: write`
for reporting, and hands the runner a separate `contents: read` token that
expires within the hour. That separation is what makes private-repository
checkout possible without giving build hosts a credential worth stealing. A
personal access token still works for public repositories, but cannot mint either.

**Failure handling is the reaper.** A runner that stops heartbeating for
`HEARTBEAT_TIMEOUT` is marked OFFLINE, its job returns to QUEUED and goes back on
the queue. This is the one path that turns a crashed machine into a retried build
rather than a job stuck forever.

## Directory map

```
cmd/api          control plane HTTP entry point (also runs migrations)
cmd/scheduler    assignment loop + runner reaper
cmd/runner       the agent that executes jobs
internal/api         handlers, middleware, auth (thin; no business logic)
internal/models      Job and Runner types + their state machines
internal/store       every SQL statement, nothing else
internal/queue       Queue interface + Redis implementation
internal/scheduler   Scheduler interface + capability scheduler + service loop
internal/executor    Executor interface + Docker implementation
internal/runner      agent: register, heartbeat, poll, checkout, execute, report
internal/github      webhook verification/parsing + commit status client
internal/jobspec     forgerun.yml parser
internal/config      environment configuration
internal/logging     JSON logger
migrations           embedded SQL, applied automatically at API startup
tests                end-to-end tests (stubbed executor and real containers)
```

## Extending it

Each infrastructure dependency sits behind an interface, so the intended growth
path replaces implementations rather than rewriting flow:

- **Kafka / SQS** — implement `queue.Queue`.
- **Kubernetes, Firecracker, a remote VM** — implement `executor.Executor`.
- **Bin packing, priority classes, fair-share** — implement `scheduler.Scheduler`.
- **GitHub Checks API** (annotations, re-run buttons) — extend `github.Client`;
  the App authentication it needs is already in place.
- **Multiple schedulers** — switch the queue to Redis Streams with a consumer
  group; the assignment transaction already tolerates concurrent schedulers.
- **Multi-job workflows** — `forgerun.yml` is already a map of jobs and the parser
  keeps all of them; only `Primary()` and the runner loop need to change.

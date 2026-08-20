# Security model

ForgeRun executes code that arrives from the internet. A pull request from a
stranger is, for our purposes, an attacker with a shell. Everything below follows
from that assumption.

## What is implemented today

### Webhook intake
- Every delivery must carry a valid `X-Hub-Signature-256` HMAC over the raw body,
  compared in constant time (`hmac.Equal`). The body is read first and verified
  before it is parsed, so no unauthenticated input is ever acted on.
- Only `push` and `pull_request` are accepted; everything else is ignored.
- Fields we later hand to git are validated: the commit SHA must be 40 hex
  characters, the repository must look like `owner/name`, and the clone URL must
  be `https://`. This is what stops a crafted payload from turning into
  `git clone file:///etc/...` or an argument-injection attempt.
- A unique index on `(repository, commit_sha, event_type)` over unfinished jobs
  makes redelivery idempotent: GitHub retrying a webhook cannot start a second
  build of the same commit.

### Runner authentication
- Joining the pool requires the shared `RUNNER_REGISTRATION_TOKEN`. Without it,
  anyone who can reach the API could register a runner and receive other people's
  source code.
- Each runner gets a 256-bit random token. Only its SHA-256 is stored, so a
  database dump does not yield working credentials. Comparison is constant time.
- A runner may only act on the job assigned to it. Start, log and result
  endpoints all check `job.runner_id == X-Runner-ID`, so one compromised runner
  cannot corrupt another's build.
- Each heartbeat renews a lease on the job the runner claims to be running. If
  that job was cancelled, finished, or handed to someone else, the response tells
  the runner to stop, and it kills the container. This is also what stops two
  runners executing the same job after a requeue.

### Job execution
Each job runs in a container that is created for it and destroyed afterwards:

| Control | Setting | Why |
|---|---|---|
| Network | `--network none` by default | A build cannot exfiltrate data or scan your network unless you opt in |
| CPU | `JOB_CPUS` (1.0) | One job cannot starve the host |
| Memory | `JOB_MEMORY_MB` (1024) | Same, and OOM kills the container, not the host |
| Processes | PID limit 512 | Blocks fork bombs |
| Privileges | `no-new-privileges`, all capabilities dropped | No setuid escalation inside the container |
| Filesystem | Only the job's own workspace is bind-mounted | No host paths, no other jobs' data |
| Lifetime | `JOB_TIMEOUT` (10m), enforced by context | A hung or deliberately looping build is killed |
| Cleanup | Container force-removed in a `defer` with its own context | Cleanup still happens when the job is cancelled |

The Docker socket is **never** mounted into a job container. A job that could
reach the socket could start a privileged container and own the host.

### Secret isolation
- Job containers receive only `CI`, `FORGERUN_JOB_ID`, `FORGERUN_COMMIT` and
  `FORGERUN_BRANCH`, plus whatever the repository's own `forgerun.yml` sets.
- `GITHUB_TOKEN`, `DATABASE_URL`, `REDIS_URL` and the registration token exist in
  the control plane and the runner process, never inside the container.
- Checkout happens on the runner host, not in the job container, so credentials
  used to fetch code never enter the untrusted environment.

### Clone credentials for private repositories
With a GitHub App configured, the control plane mints a short-lived installation
token for the repository and hands it to the runner with the job. The token is
reused until shortly before it expires, then replaced:

| Property | Value | Why |
|---|---|---|
| Scope | One repository | A stolen token reaches nothing else |
| Permissions | `contents: read` only | It cannot write code, statuses or issues |
| Lifetime | One hour, refreshed automatically | Useless soon after the build ends |
| Storage | Never written to disk or the database | Only in flight and in runner memory |

The token is passed to git with `-c http.extraheader=...` rather than embedded in
the remote URL. A URL credential is persisted into `.git/config`, and the
workspace is bind-mounted into the container running untrusted code, so anything
written inside the workspace must be assumed readable by the build. Passing it
per invocation keeps it in the runner process. git failures are redacted before
they reach the build log.

The control plane's own token is minted separately with `statuses: write`, so the
credential a runner receives can never post a commit status.

A personal access token is never used for cloning. It is long lived and usually
covers every repository its owner can see, so shipping one to build hosts would
turn one compromised runner into an account compromise. Private repositories
therefore require GitHub App authentication.

### Workspace isolation
Every job gets a fresh directory that is deleted when the job ends, whatever the
outcome. A runner never carries repository state between jobs, so one build cannot
plant something for the next one to pick up.

## What this is not

**Docker isolation is not a hardened VM sandbox.** Containers share the host
kernel. A kernel vulnerability, a misconfigured seccomp profile, or a future
`--privileged` escape hatch turns "container escape" into "host compromise". If
you run untrusted pull requests from the public internet, you want a stronger
boundary than a container:

- disposable VMs (Firecracker, Cloud Hypervisor) per job, or
- gVisor / Kata Containers for a second kernel boundary, or
- at minimum, one throwaway host per untrusted tenant.

The `Executor` interface exists precisely so a `FirecrackerExecutor` can be added
without touching anything else.

## Known gaps in this MVP

These are deliberate omissions, not oversights. Address them before production:

1. **Runner tokens never expire or rotate.** A leaked token is valid forever.
   Installation tokens do expire; the runner's own bearer token does not.
2. **The registration token is shared by all runners.** One leak lets an attacker
   join the pool. Per-runner enrolment tokens or mTLS is the fix.
3. **A clone token is visible in the runner host's process list** while git runs,
   because `-c http.extraheader=...` is a command-line argument and `/proc` is
   readable by other local users. Job containers cannot see it (they have their
   own PID namespace), but anyone with a shell on the runner host can. The fix is
   a credential helper reading the token from an environment variable; the
   trade-off was chosen deliberately, since the alternative writes the token into
   the workspace the untrusted build can read.
4. **Logs are stored unbounded in Postgres.** A malicious build can write gigabytes.
   Chunks are capped at 1 MB each, but the total is not, and there is no retention.
5. **No per-repository authorisation.** Any repository that can reach the webhook
   endpoint with a valid signature gets builds. An allowlist belongs here.
6. **No rate limiting** on the webhook or the API.
7. **PR builds run untrusted code with the same privileges as branch builds.**
   Real systems distinguish `pull_request` from `pull_request_target` and require
   approval for first-time contributors. This is the gap that matters most if you
   accept pull requests from strangers.
8. **The control plane speaks plain HTTP.** Terminate TLS in front of it; runner
   tokens are bearer credentials on the wire.

## Reporting

Treat any way for repository code to influence the runner host — not just the
container — as critical.

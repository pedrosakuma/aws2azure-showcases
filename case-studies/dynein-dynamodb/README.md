# Case study: dynein (real DynamoDB CLI client) over aws2azure

**Status:** ✅ Verified end-to-end locally (dynein's own release binary +
aws2azure + a local Azure Cosmos DB emulator). Not yet gated in CI (see "CI
status" below).

## What this demonstrates

[`dynein`](https://github.com/awslabs/dynein) is a real DynamoDB CLI client
maintained by AWS (`awslabs/dynein`), built on the standard Rust AWS SDK.
This case study points it at `aws2azure`'s DynamoDB module instead of real
AWS DynamoDB, backed by the local Azure Cosmos DB emulator, and drives it
through a full table lifecycle — `CreateTable`, `PutItem`, `GetItem`,
`Query`, `Scan`, `DeleteItem`, `DeleteTable` — with **zero dynein code or
build changes**: only the standard `AWS_ENDPOINT_URL` environment variable
pointed at the proxy (dynein's own `--region`/`--port` flags only support
`localhost`, so the env var is what makes a custom-host proxy like
aws2azure work at all).

## Why this case study exists — an upstream bug had to be fixed first

Building this case study surfaced a real bug in aws2azure's own shipped
`docker-compose.yml`:

- **[aws2azure#967](https://github.com/pedrosakuma/aws2azure/issues/967)**
  (fixed in
  [#968](https://github.com/pedrosakuma/aws2azure/pull/968)) — the
  repo's own `docker-compose.yml` / `deploy/docker-compose.yml` pinned the
  Cosmos DB emulator to the `:latest` tag, which is **HTTPS-only** (with a
  self-signed cert). But `docker/config.json`'s DynamoDB binding — and the
  project's own integration/perf test suites — target the emulator over
  **plain HTTP** (`http://cosmos:8081/`), which only the newer
  `:vnext-preview` tag (a Postgres/pgcosmos-backed emulator) actually
  serves. Following the shipped compose file as-is meant every DynamoDB
  request failed with `InternalServerError: Service Unavailable` (a TLS
  handshake failure between the plain-HTTP-configured proxy and the
  HTTPS-only emulator), even with a correctly bootstrapped database.
  **Fixed upstream**: the Cosmos image tag was corrected to
  `:vnext-preview` in both compose files, and a stale doc line claiming
  the emulator "serves a self-signed certificate" was corrected too.

With that fix, a raw boto3 client (`CreateTable`/`PutItem`/`GetItem`) and
then the real `dynein` binary were both verified working end-to-end
against aws2azure before this case study was finalized.

## A real-world gotcha worth knowing about (not aws2azure-specific)

- **The Cosmos DB database must be pre-created.** aws2azure's DynamoDB
  module proxies onto an *existing* Cosmos database; it does not create
  one. `scripts/bootstrap_cosmos_db.py` creates the `aws2azure` database
  directly against the emulator (bypassing aws2azure) using the
  `azure-cosmos` Python SDK, with **`enable_endpoint_discovery=False`** —
  without it, the SDK follows internal Docker-network replica IPs
  advertised by the emulator's gateway that aren't reachable from the
  host, causing `ServiceRequestTimeoutError`. This must run once after
  `docker compose up` and before any DynamoDB call.
- **`dy admin delete table` needs a real tty.** dynein prompts for an
  interactive `[y/n]` confirmation before deleting a table, and panics
  (`IO(Custom { kind: NotConnected, error: "not a terminal" })`) if stdin
  isn't a genuine terminal — there's no `--yes`/`--force` flag to skip it.
  This is a real dynein limitation for scripted/CI use, not an aws2azure
  issue. `scripts/verify_e2e_run.py` works around it by allocating a
  pseudo-terminal with Python's `pty` module and answering the prompt
  once it appears (a plain shell pipe like `echo y | dy ...` does **not**
  work — dynein still sees a non-tty stdin through a pipe and panics; it
  needs an actual pty).

## Stack

- `cosmos` — the Azure Cosmos DB emulator, pinned to the `vnext-preview`
  tag (see the bug above) so it serves plain HTTP on 8081, matching
  `config/aws2azure-config.json`'s target.
- `aws2azure` — built directly from the upstream repo (no published image
  yet) via a remote git build context; routes DynamoDB traffic to the
  Cosmos emulator. Reached via a `dynamodb.aws2azure` network alias, since
  aws2azure routes DynamoDB requests by Host header (must start with
  `"dynamodb."`/`"dynamodb-"` or equal `"dynamodb"` exactly).

## Running it

```bash
cd case-studies/dynein-dynamodb
docker compose up --build -d
# wait for both services to report healthy
python3 -m venv .venv && . .venv/bin/activate
pip install azure-cosmos
python3 scripts/bootstrap_cosmos_db.py   # create the Cosmos database once
python3 scripts/verify_e2e_run.py        # downloads dynein and runs the full lifecycle
```

`verify_e2e_run.py`:
1. Downloads the official `dynein` v0.3.0 Linux release binary (`dy`) from
   `awslabs/dynein`'s GitHub releases, if not already present.
2. Waits for aws2azure to accept DynamoDB requests.
3. Runs `dy admin create table`, `dy put`, `dy get`, `dy query`, `dy scan`,
   `dy del`, and `dy admin delete table` (via the pty workaround above)
   against `aws2azure`, asserting each step's expected output.

## CI status

Wired into `.github/workflows/dynein-dynamodb-e2e.yml`, mirroring
`airflow-celery-sqs-e2e.yml`: runs on manual `workflow_dispatch`, nightly
schedule, or on a PR touching this case study (gated behind a
`run-integration` label or a direct path match), spinning up the stack,
bootstrapping the Cosmos database, and running `scripts/verify_e2e_run.py`.

## Relates to

- pedrosakuma/aws2azure#967
- pedrosakuma/aws2azure#968

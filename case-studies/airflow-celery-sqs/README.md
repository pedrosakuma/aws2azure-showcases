# Case study: Apache Airflow CeleryExecutor over SQS (via aws2azure)

**Status:** ✅ Verified end-to-end locally (Airflow CeleryExecutor + kombu's
SQS transport + aws2azure + a local Azure Service Bus emulator). Not yet
gated in CI (see "CI status" below).

## What this demonstrates

Airflow supports `CeleryExecutor` with Celery's built-in [SQS
transport](https://docs.celeryq.dev/projects/kombu/en/stable/reference/kombu.transport.SQS.html)
(`kombu[sqs]`) as the task broker — no Redis/RabbitMQ required. This case
study points that broker at `aws2azure` instead of real AWS SQS, backed by
the local Azure Service Bus emulator, and proves a real Airflow DAG run
executes correctly through it: the scheduler enqueues a task message, the
Celery worker (`airflow-worker`) receives and executes it via the
SQS-compatible queue, and the queue drains — with **zero Airflow/Celery
code changes**, only a broker URL pointed at the proxy.

## Why this case study exists — two upstream bugs had to be fixed first

Earlier attempts to build this case study failed because of two aws2azure
bugs discovered while testing the exact call pattern kombu's SQS transport
uses (`GetQueueUrl` → `CreateQueue` if missing → cache the URL →
send/receive/delete):

- **[aws2azure#955](https://github.com/pedrosakuma/aws2azure/issues/955)**
  — `GetQueueUrl` returned a false-positive success for a queue that had
  never been created, against the local Service Bus emulator. kombu's
  create-if-missing logic depends on `GetQueueUrl` correctly raising
  `QueueDoesNotExist` for an unknown queue; with the false positive, kombu
  believed the queue already existed and never issued `CreateQueue`, so
  every send silently targeted a queue that was never actually provisioned.
  **Fixed upstream** (merged, closed).
- **[aws2azure#962](https://github.com/pedrosakuma/aws2azure/issues/962)**
  (fixed in #963/#964) — an unrelated Secrets Manager bug (Key Vault
  rejects secret names containing `/`) found and fixed in the same release
  cycle as #955; noted here because both fixes landed together and were
  verified as part of the same upstream re-test pass.
- **[aws2azure#965](https://github.com/pedrosakuma/aws2azure/issues/965)**
  (fixed in #966) — `SqsServiceModule` never enabled
  `BuffersRequestBodyForSigV4`, unlike the other AWS-JSON modules
  (DynamoDB/Kinesis/SNS). botocore's default (non-S3) `SigV4Auth` — used by
  every real SQS client, including kombu's — signs the request body but
  never sends an `x-amz-content-sha256` header (only S3's SigV4Auth
  subclass does). Without buffering, aws2azure fell back to a sentinel
  payload hash that never matched, so **every** body-bearing SQS call
  (`GetQueueUrl`, `CreateQueue`, `SendMessage`, ...) from a standard boto3
  client was rejected with `InvalidSignatureException`. This is the bug
  that actually blocked a real Celery/kombu client (as opposed to a
  hand-crafted boto3 call with a manually-added header) from working at
  all. **Fixed upstream** (merged, closed).

With all three fixes merged, the full kombu call sequence
(`GetQueueUrl` → `QueueDoesNotExist` → `CreateQueue` → cache → `SendMessage`
→ `ReceiveMessage` → `DeleteMessage` → `DeleteQueue`) was replayed via raw
boto3 and passed cleanly, which is what made building this case study with
a real `CeleryExecutor`/kombu stack (rather than just a raw-boto3 replay)
viable.

## Two platform divergences worth knowing about

- **SQS `VisibilityTimeout` vs. Service Bus `LockDuration`.** SQS allows up
  to 43200s (12h); Azure Service Bus caps `LockDuration` at 5 minutes.
  aws2azure validates `VisibilityTimeout` against SQS's own legal range but
  does not clamp/reject before forwarding to Service Bus, so a
  Celery/kombu default (`visibility_timeout: 1800`, i.e. 30 minutes) passes
  aws2azure's validation and then fails with an opaque `InternalFailure`
  from the Service Bus backend. This case study works around it by setting
  `visibility_timeout: 300` (5 minutes, Service Bus's max) in
  `AIRFLOW__CELERY__BROKER_TRANSPORT_OPTIONS`, and matches it with
  `LockDuration: PT5M` on the pre-declared queue.
- **boto3/kombu's SQS client isn't fork-safe.** Airflow's `CeleryExecutor`
  sends tasks to the broker using a multi-process pool
  (`sync_parallelism`, default > 1) for throughput. Forking after the
  first HTTP connection pool is warmed up, then publishing concurrently
  from the forked children, produced hangs (`AirflowTaskTimeout`) and
  internal kombu channel-pool errors — this is a known real-world
  Celery+SQS+fork caveat, unrelated to aws2azure. Fixed by setting
  `AIRFLOW__CELERY__SYNC_PARALLELISM: "1"` (serial task sending).

## Stack

- `postgres` — Airflow metadata DB (also used as the Celery result
  backend, via `db+postgresql://`, so no Redis/RabbitMQ is needed).
- `mssql` (Azure SQL Edge) — backing store required by the Service Bus
  emulator.
- `servicebus-emulator` — local Azure Service Bus emulator. The `default`
  queue (Celery's actual default queue name) is pre-declared in
  `deploy/servicebus/Config.json` with `LockDuration: PT5M` to mirror
  aws2azure's own dev convention, but this isn't strictly required:
  aws2azure's `CreateQueue` now creates Service Bus queues dynamically (the
  #955 fix), so kombu's own create-if-missing logic works against a queue
  that was never pre-declared too.
- `aws2azure` — built directly from the upstream repo (no published image
  yet) via a remote git build context; routes SQS traffic to the emulator.
  Reached via a `sqs.aws2azure` network alias, since aws2azure routes SQS
  requests by Host header.
- `airflow-init` / `airflow-webserver` / `airflow-scheduler` /
  `airflow-worker` — Airflow 2.10.4 with `AIRFLOW__CORE__EXECUTOR=CeleryExecutor`
  and `AIRFLOW__CELERY__BROKER_URL` set to a `sqs://` URL pointed at
  `sqs.aws2azure:8080`.

## Running it

```bash
cd case-studies/airflow-celery-sqs
docker compose up -d
# wait for airflow-webserver to become healthy (http://localhost:8081, admin/admin)
python3 -m venv .venv && . .venv/bin/activate
pip install boto3 requests
python3 scripts/verify_e2e_run.py
```

`verify_e2e_run.py`:
1. Waits for the webserver's REST API.
2. Confirms the `default` queue is resolvable via SQS `GetQueueUrl` against
   aws2azure (proves the broker leg is live).
3. Triggers `example_bash_operator` and polls until it succeeds. Since the
   only configured executor/broker is `CeleryExecutor` over the SQS
   transport, a successful run is only possible if the task was actually
   enqueued via aws2azure, picked up by `airflow-worker`, and executed.
4. Confirms the queue's `ApproximateNumberOfMessages` returns to `0` after
   the run, proving the message was consumed through a real, addressable
   Service Bus queue behind aws2azure rather than a stub.

## CI status

Wired into `.github/workflows/airflow-celery-sqs-e2e.yml`, mirroring
`airflow-s3-logging-e2e.yml`: runs on manual `workflow_dispatch`, nightly
schedule, or on a PR touching this case study (gated behind a
`run-integration` label or a direct path match), spinning up the full
stack and running `scripts/verify_e2e_run.py`. Unlike `airflow-s3-logging`,
there is no LocalStack differential workflow yet for this case study.

## Relates to

- pedrosakuma/aws2azure-showcases#9
- pedrosakuma/aws2azure#955
- pedrosakuma/aws2azure#962
- pedrosakuma/aws2azure#965

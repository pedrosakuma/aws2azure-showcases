# Case study: Apache Airflow SNS publish over aws2azure (Service Bus Topics)

**Status:** ✅ Verified end-to-end locally (Airflow + `SnsPublishOperator` +
aws2azure + a local Azure Service Bus emulator), and wired into CI (see "CI
status" below).

## What this demonstrates

Airflow's Amazon provider ships an unmodified
[`SnsPublishOperator`](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/sns.html)
built on `SnsHook`/boto3. This case study points its `aws_conn_id` at
`aws2azure` instead of real AWS SNS, backed by a local Azure Service Bus
emulator, and proves a real Airflow DAG run publishes a message correctly
through it: `CreateTopic`, `Subscribe`, and `Publish` all go through
aws2azure with **zero Airflow/provider code changes**, only an
`endpoint_url` pointed at the proxy — and the published message, including
its `Subject` and `MessageAttributes`, is verifiably present in the backing
Azure Service Bus Topic afterward.

## What this does NOT prove — read this before drawing conclusions

aws2azure's SNS `Subscribe` is **publish-side only by design**. It records
a `Subscribe` call as an Azure Service Bus topic subscription, but it does
**not** implement active fan-out delivery — it never pushes a published
message out to a subscribed SQS queue or an HTTP(S) endpoint. This is a
documented, intentional non-goal
(`docs/gaps/sns/Subscribe.yaml` → "Subscriber delivery forwarder", `WON'T
IMPLEMENT`): active push delivery would require a stateful, always-on
dispatcher with retry/backoff and dead-lettering, which is out of scope for
a stateless request/response proxy.

Concretely, this means:

- The DAG's `bootstrap_topic_and_subscription` task does call `Subscribe`
  with `Protocol="sqs"` and a symbolic SQS endpoint ARN, to exercise the
  real call shape a fanout configuration would use — but **nothing is ever
  delivered to that ARN**. No SQS queue exists in this case study at all.
- `scripts/verify_e2e_run.py` reads the published message back **directly
  from the backing Azure Service Bus topic subscription**, using the native
  `azure-servicebus` Python SDK — bypassing aws2azure entirely on the read
  side. This is the documented supported way to consume these messages (a
  native Azure Service Bus consumer), and it is the only way to prove
  `Publish` actually worked, since aws2azure exposes no SNS-side "receive"
  API.
- This case study therefore validates aws2azure's SNS **publish** path
  (`CreateTopic` → `Subscribe` → `Publish`) with a real, unmodified AWS
  client, but does **not** validate — and cannot validate — any SNS→SQS
  fanout delivery, because aws2azure does not implement that.

## Stack

- `postgres` — Airflow metadata DB.
- `mssql` (Azure SQL Edge) — backing store required by the Service Bus
  emulator.
- `servicebus-emulator` — local Azure Service Bus emulator, with an empty
  `Topics: []` in `deploy/servicebus/Config.json`. aws2azure's `CreateTopic`
  creates Service Bus topics dynamically at runtime (confirmed by
  aws2azure's own `SnsTopicLifecycleServiceBusTests`), so no pre-declared
  topic is needed. Its AMQP (5672) and management (5300) ports are exposed
  to the host so `scripts/verify_e2e_run.py` can read the published message
  back directly (see above).
- `aws2azure` — built directly from the upstream repo (no published image
  yet) via a remote git build context; routes SNS traffic to the emulator.
  Reached via an `sns.aws2azure` network alias, since aws2azure routes SNS
  requests by Host header.
- `airflow-init` / `airflow-webserver` / `airflow-scheduler` — Airflow
  2.10.4 with `LocalExecutor`. `dags/sns_publish_dag.py` is bind-mounted
  in (rather than the named-volume pattern the other case studies use for
  `LOAD_EXAMPLES`-provided DAGs) because this case study needs a custom
  DAG: Airflow's built-in examples don't exercise `SnsPublishOperator`.

## Running it

```bash
cd case-studies/airflow-sns-fanout
docker compose up -d
# wait for airflow-webserver to become healthy (http://localhost:8081, admin/admin)
python3 -m venv .venv && . .venv/bin/activate
pip install azure-servicebus>=7.14.0 requests
python3 scripts/verify_e2e_run.py
```

`verify_e2e_run.py`:
1. Waits for the webserver's REST API and the Service Bus emulator's AMQP
   port.
2. Computes the deterministic Service Bus subscription name aws2azure's
   `Subscribe` derives (SHA-256 of `TopicArn\nProtocol\nEndpoint`,
   truncated to 20 hex chars — mirrors
   `SnsSubscriptionSupport.CreateSubscriptionId` in the aws2azure repo).
3. Triggers the `sns_publish_showcase` DAG and polls until it succeeds.
   Success requires `CreateTopic`, `Subscribe`, and `Publish` to all have
   round-tripped correctly through aws2azure.
4. Reads the message directly off the backing Service Bus topic
   subscription via `azure-servicebus`, and asserts the body, `Subject`
   (`aws.sns.Subject` application property), and a custom
   `MessageAttributes` entry all match what `SnsPublishOperator` sent.

## CI status

Wired into `.github/workflows/airflow-sns-fanout-e2e.yml`, mirroring
`airflow-s3-logging-e2e.yml`/`airflow-celery-sqs-e2e.yml`: runs on manual
`workflow_dispatch`, nightly schedule, or on a PR touching this case study
(gated behind a `run-integration` label or a direct path match), spinning
up the full stack and running `scripts/verify_e2e_run.py`.

## Relates to

- pedrosakuma/aws2azure-showcases#20

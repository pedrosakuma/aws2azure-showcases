# aws2azure showcases

Real-world, well-known open-source applications validated running against
[`aws2azure`](https://github.com/pedrosakuma/aws2azure) — the AWS-wire-protocol
→ Azure REST proxy — instead of against real AWS.

## What this repo is

A collection of small, reproducible **case studies**: take a recognizable
open-source project that talks to one or more AWS services (S3, SQS,
DynamoDB, Kinesis, SNS, Secrets Manager) through the official AWS SDK, point
it at `aws2azure` instead of `*.amazonaws.com`, and verify the exact same
application-level workflow round-trips correctly against an Azure backend.

## What this repo is **not**

- **Not** part of the `aws2azure` release. It has its own pace, its own CI (or
  none), and is not gated by `aws2azure`'s change-aware validation, footprint
  budget, or qualification process.
- **Not** a fork or vendored copy of the showcased projects — each case study
  only adds a thin config + docker-compose layer around the upstream project's
  own published image/release.
- **Not** a substitute for `aws2azure`'s own real-Azure qualification evidence
  (see `docs/workloads/` and `docs/site/workload-ga.md` in the main repo). A
  case study here demonstrates *this specific application's* workflow works;
  it is not a general compatibility claim for the underlying AWS operations
  (the main repo's generated compatibility/workload-GA pages are the
  authoritative source for that).

## Case studies

| Case study | Upstream project | AWS services exercised | Azure backend | Status |
|---|---|---|---|---|
| [`airflow-s3-logging`](case-studies/airflow-s3-logging/) | [Apache Airflow](https://airflow.apache.org/) | S3 (remote task logging) | Blob Storage (Azurite locally / real Storage account optionally) | ⚠️ call-pattern verified; full docker-compose stack not yet run end-to-end (see case study status) |

## Adding a new case study

1. Pick a project whose AWS integration is genuinely optional/pluggable (an
   `endpoint_url` override, a custom S3-compatible connection, etc.) — no
   patching the upstream project's source.
2. Check which operations it needs against `aws2azure`'s generated
   [compatibility matrix](https://github.com/pedrosakuma/aws2azure/blob/main/docs/site/workload-compatibility.md)
   and [workload GA](https://github.com/pedrosakuma/aws2azure/blob/main/docs/site/workload-ga.md)
   pages before investing time — prefer projects whose required operations
   already land in an `implemented`/GA profile.
3. New folder under `case-studies/<name>/` with its own `README.md`,
   `docker-compose.yml`, config, and a small smoke-test script that exercises
   the real application-level workflow (not just raw AWS SDK calls).
4. Add a row to the table above.

## Relationship to `aws2azure`

This repo is referenced (one-way link only) from `aws2azure`'s own
documentation as an adoption-friction resource. It does not read from or
write to the main repo's generated docs, gap docs, or CI.

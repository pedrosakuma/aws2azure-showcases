# Case study: Apache Airflow S3 remote task logging → Azure Blob Storage

[Apache Airflow](https://airflow.apache.org/) (Apache Software Foundation) is
the most widely adopted open-source workflow orchestrator in the data
engineering space. Its `apache-airflow-providers-amazon` package supports
writing task logs to S3 (`remote_logging`), configured entirely through an
Airflow *Connection* — including an `endpoint_url` override, since it's just
boto3 underneath. That makes it a clean fit for `aws2azure`: no Airflow code
changes, only configuration.

## Status

- ✅ **CI-verified, continuously**: `.github/workflows/airflow-s3-logging.yml`
  builds `aws2azure` from the upstream repo, starts it against Azurite, and
  runs `scripts/verify_s3_roundtrip.py` (the exact six-call sequence below)
  on every push/PR touching this case study —
  [![smoke test](https://github.com/pedrosakuma/aws2azure-showcases/actions/workflows/airflow-s3-logging.yml/badge.svg)](https://github.com/pedrosakuma/aws2azure-showcases/actions/workflows/airflow-s3-logging.yml).
- ✅ **CI-verified end-to-end, nightly**:
  `.github/workflows/airflow-s3-logging-e2e.yml` runs the *entire* stack
  (Postgres + Airflow webserver/scheduler + aws2azure + Azurite), triggers
  the bundled `example_bash_operator` DAG via the REST API, waits for
  `success`, then deletes the task's local on-disk log copy and confirms
  the webserver still serves the same content — proving it came from
  Azurite via `aws2azure`, not local disk. Runs nightly, on manual dispatch,
  or on a PR touching this case study —
  [![full-stack e2e](https://github.com/pedrosakuma/aws2azure-showcases/actions/workflows/airflow-s3-logging-e2e.yml/badge.svg)](https://github.com/pedrosakuma/aws2azure-showcases/actions/workflows/airflow-s3-logging-e2e.yml).
  (`scripts/verify_e2e_run.py` drives this and can also be run manually.)
- 🛠️ **One real gap found and fixed by that run**: `S3TaskHandler` never
  creates the log bucket itself (it only issues `PutObject`/`GetObject`
  against it) and `S3RemoteLogIO.write()` swallows upload errors silently —
  so without the log bucket pre-existing, every task log write failed
  silently and Airflow fell back to local-disk-only logs, with **no error
  surfaced anywhere in the UI or logs**. `docker-compose.yml`'s
  `airflow-init` now bootstraps the `airflow-logs` bucket (via `S3Hook`)
  before the webserver/scheduler start, exactly like a real deployment
  must also provision the backing Blob container once (e.g. via
  Terraform/Bicep).
- This end-to-end run is CI-enforced but on a slower cadence (nightly /
  manual dispatch / labeled PR) than the fast smoke test above, since the
  full Postgres + webserver/scheduler footprint is heavier — see
  [Known limits](#known-limits-by-design-not-a-bug).

## What's exercised

**Airflow's own runtime call pattern** (`S3TaskHandler`/`S3RemoteLogIO`, from
`apache-airflow-providers-amazon`) is narrower than it might look at first:
per task attempt it only ever issues

1. `PutObject` — write the attempt's log (`dag_id=.../run_id=.../task_id=.../attempt=N.log`)
2. `ListObjectsV2` — the webserver enumerating attempts for a task
3. `GetObject` — the webserver rendering the log
4. `DeleteObject` — retention/cleanup housekeeping

Airflow's real `S3TaskHandler` **never creates the bucket itself**, and
`S3RemoteLogIO.write()` silently swallows the upload error if the bucket is
missing — no error surfaces in the UI, logs just silently fall back to
local-disk-only. `HeadBucket`/`CreateBucket` are **not** part of Airflow's
runtime behavior; they only appear in two places in this case study, both of
which are our own setup convenience, not Airflow's doing:

- `docker-compose.yml`'s `airflow-init` bootstraps the bucket once via
  `S3Hook` before the webserver/scheduler start (this is what actually
  prevents the silent-failure bug above from being hit in this case study).
- `scripts/verify_s3_roundtrip.py` additionally simulates a
  `HeadBucket`→`CreateBucket` check as part of its own standalone,
  Airflow-less smoke test.

In any real deployment, provisioning the log bucket ahead of time is an
operator/IaC responsibility — not something to rely on `S3TaskHandler` for.

All operations above (plus `HeadBucket`/`CreateBucket` used by our own
bootstrap/smoke-test code) are `implemented` in `aws2azure` and are part of its
`s3-basic-object-crud` workload profile (GA as of the profile's last
qualification — see the
[upstream workload-GA page](https://github.com/pedrosakuma/aws2azure/blob/main/docs/site/workload-ga.md)
for the current live verdict).

## Stack

- `apache/airflow` (official image) — webserver + scheduler, `LocalExecutor`,
  Postgres metadata DB (no Celery/Redis needed for this case study — remote
  logging doesn't depend on the executor).
- `mcr.microsoft.com/azure-storage/azurite` — local Blob Storage emulator
  standing in for a real Azure Storage account. Swap the Bicep-provisioned
  real account in `.env` to validate against real Azure (see
  [Going to real Azure](#going-to-real-azure) below).
- `aws2azure` — no published image yet; built directly from the main repo via
  a remote git build context in `docker-compose.yml`
  (`https://github.com/pedrosakuma/aws2azure.git`).

## Run it

```bash
cd case-studies/airflow-s3-logging
docker compose up --build -d
```

Wait for the webserver health check (the `airflow-init` service creates the
admin user and bootstraps the `airflow-logs` bucket automatically), then
unpause and trigger the bundled example DAG:

```bash
docker compose exec airflow-webserver airflow dags unpause example_bash_operator
docker compose exec airflow-webserver airflow dags trigger example_bash_operator
```

Open http://localhost:8081 (admin/admin), open the triggered run, open a task
instance, and view its log — it's being read back from Azurite Blob Storage
through `aws2azure`, not from local disk or S3.

### Automated smoke test

`scripts/verify_s3_roundtrip.py` replays the same six-call sequence directly
(without needing the full Airflow UI) for fast CI-less verification:

```bash
python3 -m venv .venv && .venv/bin/pip install boto3
.venv/bin/python scripts/verify_s3_roundtrip.py
```

## Configuration

`config/aws2azure-config.json` — the proxy binding used by this case study:

```jsonc
{
  "services": { "s3": { "enabled": true } },
  "bindings": [
    {
      "aws": {
        "accessKeyId": "AKIA-AIRFLOW-SHOWCASE",
        "secretAccessKey": "showcase-local-dev-secret-not-real"
      },
      "azure": {
        "s3": {
          "kind": "blob",
          "target": {
            "accountName": "devstoreaccount1",
            "endpoint": "http://azurite:10000/devstoreaccount1"
          },
          "auth": {
            "mode": "sharedKey",
            "key": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
          }
        }
      }
    }
  ]
}
```

Airflow's side (`docker-compose.yml` environment): a `logging` config with
`remote_logging = True` and a connection (`aws_s3_showcase`) whose
`endpoint_url` points at `http://s3.aws2azure:8080` — `s3.aws2azure` is a
network alias on the `aws2azure` service, needed because `aws2azure` routes
S3 by Host header (must start with `s3.`/`s3-` or contain `.s3.`), exactly
like real AWS endpoints. A real deployment resolves
`s3.<region>.amazonaws.com` (or any `s3.*` name) to the sidecar via
DNS/hosts, with zero Airflow code change.

## Going to real Azure

Swap the `target.endpoint`/`accountName`/`auth` in
`config/aws2azure-config.json` for a real Storage account (shared key or
Managed Identity — see
[`docs/configuration/examples`](https://github.com/pedrosakuma/aws2azure/tree/main/docs/configuration/examples)
in the main repo), drop the `azurite` service from `docker-compose.yml`, and
re-run. This incurs real Azure Storage costs — clean up the account when
done.

## Known limits (by design, not a bug)

- This case study only exercises S3. Airflow's AWS Secrets Manager
  connections/variables backend (`SecretsManagerBackend`) is covered by the
  [`airflow-secrets-manager`](../airflow-secrets-manager/) case study.
- `LocalExecutor` is used to keep the compose stack small; a
  `CeleryExecutor` + SQS broker variant is out of scope for this case study.

# Case study: Apache Airflow Secrets Manager backend → Azure Key Vault

[Apache Airflow](https://airflow.apache.org/) (Apache Software Foundation)
supports resolving Connections/Variables from AWS Secrets Manager via
`apache-airflow-providers-amazon`'s `SecretsManagerBackend`
(`airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend`),
configured entirely through `backend_kwargs` — including an `endpoint_url`
override, since it's just boto3 underneath. That makes it a clean fit for
`aws2azure`: no Airflow code changes, only configuration, same pattern as
the [`airflow-s3-logging`](../airflow-s3-logging/) case study.

## Status

- 📝 **Config-only walkthrough, not CI-verified**: unlike Blob Storage
  (Azurite), there is no widely-used local emulator for Key Vault.
  `aws2azure`'s own config schema (`$defs/secretsManagerBackend` in
  `config.schema.json`) confirms this: the `secretsmanager` binding's
  `target` only accepts an `https` `vaultUrl`, with no local/emulator
  endpoint shape — so an automated, CI-enforced smoke test would need a
  real (low-cost, disposable) Azure Key Vault instance and Entra
  credentials, which this repo's CI does not currently provision. This case
  study is therefore documented as a manual walkthrough against a real
  vault; it has **not** been run end-to-end here (this environment had no
  Azure subscription/credentials available). If you run it, please open an
  issue/PR with the result — see [`airflow-s3-logging`](../airflow-s3-logging/)
  for the "call-pattern verified vs. full stack verified" honesty
  convention this case study follows.

## What's exercised

`SecretsManagerBackend` makes exactly **one** AWS call per lookup, at
runtime:

1. `GetSecretValue` — `SecretsManagerBackend._get_secret_value()` resolving
   a Connection (`airflow/connections/<conn_id>`) or Variable
   (`airflow/variables/<key>`).

Provisioning the secret ahead of time (as an operator/admin, not Airflow
itself) additionally uses:

2. `CreateSecret` — first-time secret creation.
3. `DescribeSecret` — confirming the secret exists/is versioned.

Per `aws2azure`'s generated
[compatibility docs](https://github.com/pedrosakuma/aws2azure/blob/main/docs/site/secretsmanager.md),
all three operations are `implemented` and part of its
`secretsmanager-basic-lifecycle` workload profile (GA — see the
[upstream workload-GA page](https://github.com/pedrosakuma/aws2azure/blob/main/docs/site/workload-ga.md)
for the current live verdict).

### Known caveats (call these out if you extend this case study)

- `PutSecretValue` / `UpdateSecret` / `TagResource` / `UntagResource` are
  `partial` (🔵 by design) — Airflow's read path doesn't need them, but this
  case study's own setup script does fall back to `PutSecretValue` when a
  secret already exists (re-running the smoke test), so exercise that path
  carefully if you rely on it in your own automation.
- `RotateSecret` is `unsupported` (⚫ non-goal) and
  `UpdateSecretVersionStage` is `unsupported` (🔵 by design) — Airflow's
  `SecretsManagerBackend` read path never calls either, so this doesn't
  block the case study, but don't wire secret rotation through this
  backend expecting it to work.

## Stack

- `apache/airflow` (official image) — webserver + scheduler, `LocalExecutor`,
  Postgres metadata DB (no Celery/Redis needed — the secrets backend
  doesn't depend on the executor).
- A real **Azure Key Vault** instance — no local Blob-Storage-style
  emulator exists for Key Vault, so `docker-compose.yml` does not bundle
  one; see [Prerequisites](#prerequisites).
- `aws2azure` — no published image yet; built directly from the main repo
  via a remote git build context in `docker-compose.yml`.

## Prerequisites

You need a real Azure subscription with:

1. A Key Vault instance (any SKU) — `az keyvault create --name <name>
   --resource-group <rg>`.
2. An Entra app registration (client-secret credential) granted
   `get`/`list`/`set` secret permissions on that vault (Access Policy or
   RBAC `Key Vault Secrets Officer`) — `aws2azure` needs `get`+`list` for
   Airflow's read path, `set` only if you exercise this case study's own
   setup/teardown steps.

Fill these into `config/aws2azure-config.json`
(`target.vaultUrl`, `azureIdentities.showcase-key-vault-identity.tenantId`
/`clientId`/`clientSecret`) before starting the stack. This incurs real
Azure costs (Key Vault operations are cheap but not free) — clean up
the vault/app registration when done.

## Run it

```bash
cd case-studies/airflow-secrets-manager
# edit config/aws2azure-config.json first (see Prerequisites)
docker compose up --build -d
```

Wait for the webserver health check, then seed a connection secret and
trigger a DAG that uses it:

```bash
python3 -m venv .venv && .venv/bin/pip install boto3
.venv/bin/python scripts/verify_secrets_roundtrip.py
```

`scripts/verify_secrets_roundtrip.py` replays
`SecretsManagerBackend`'s exact `GetSecretValue` call directly (plus its own
`CreateSecret`/`DescribeSecret`/`DeleteSecret` setup/teardown) against
`aws2azure` → your real Key Vault, without needing the full Airflow UI —
useful for fast, Airflow-less verification of the wiring before trusting
the full stack.

To see Airflow itself resolve a connection through the backend, seed the
secret with the script above (leave the created connection in place — skip
its `DeleteSecret` cleanup step or re-run just the `CreateSecret` portion),
then:

```bash
docker compose exec airflow-webserver airflow connections get aws_s3_showcase
```

This should print the connection Airflow read back from
`airflow/connections/aws_s3_showcase` in your Key Vault, through
`aws2azure` — not from Airflow's own metadata DB.

## Configuration

`config/aws2azure-config.json` — the proxy binding used by this case study:

```jsonc
{
  "services": { "secretsmanager": { "enabled": true } },
  "azureIdentities": {
    "showcase-key-vault-identity": {
      "authMode": "clientSecret",
      "tenantId": "<your Entra tenant id>",
      "clientId": "<your app registration client id>",
      "clientSecret": "<your app registration client secret>"
    }
  },
  "bindings": [
    {
      "aws": {
        "accessKeyId": "AKIA-AIRFLOW-SECRETS-SHOWCASE",
        "secretAccessKey": "showcase-local-dev-secret-not-real"
      },
      "azure": {
        "secretsmanager": {
          "kind": "keyVault",
          "target": { "vaultUrl": "https://<your-vault>.vault.azure.net/" },
          "auth": { "mode": "reference", "identity": "showcase-key-vault-identity" }
        }
      }
    }
  ]
}
```

Airflow's side (`docker-compose.yml` environment):
`AIRFLOW__SECRETS__BACKEND` set to `SecretsManagerBackend`, with
`AIRFLOW__SECRETS__BACKEND_KWARGS` pointing its boto3 client at
`http://secretsmanager.aws2azure:8080` — `secretsmanager.aws2azure` is a
network alias on the `aws2azure` service, needed because `aws2azure` routes
Secrets Manager requests by Host header (must start with
`secretsmanager.`/`secretsmanager-`, or equal `secretsmanager`), exactly
like real AWS endpoints. A real deployment resolves
`secretsmanager.<region>.amazonaws.com` (or any `secretsmanager.*` name) to
the sidecar via DNS/hosts, with zero Airflow code change.

## Going to a different Key Vault / production setup

Swap `target.vaultUrl` and the `azureIdentities` entry for your production
vault and identity (Managed Identity or Workload Identity are usually
preferable to a client secret outside of local/CI use — see
[`docs/configuration/examples`](https://github.com/pedrosakuma/aws2azure/tree/main/docs/configuration/examples)
in the main repo for both shapes), and re-run.

## Known limits (by design, not a bug)

- No automated CI smoke test — see [Status](#status) above for why.
- `LocalExecutor` is used to keep the compose stack small; a
  `CeleryExecutor` + SQS broker variant is out of scope for this case study.
- This case study only exercises the Connections read path
  (`connections_prefix`); Variables/Config prefixes work identically
  (`airflow/variables/<key>`, `airflow/config/<key>`) but aren't separately
  demonstrated here.

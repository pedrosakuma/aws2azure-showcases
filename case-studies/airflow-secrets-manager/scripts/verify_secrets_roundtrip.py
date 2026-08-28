"""
Standalone verification: exercises the exact boto3 call pattern Apache
Airflow's SecretsManagerBackend (apache-airflow-providers-amazon,
airflow.providers.amazon.aws.secrets.secrets_manager) uses to resolve
Connections/Variables, pointed at the aws2azure proxy from this case
study's docker-compose stack (backed by a real Azure Key Vault) instead of
real AWS Secrets Manager.

This does NOT drive Airflow itself -- it isolates and replays the call
sequence SecretsManagerBackend issues, so you can validate the operations
quickly without waiting on the full webserver/scheduler/Postgres stack. Run
`docker compose up -d aws2azure` first (with a real vaultUrl + Entra
identity filled into config/aws2azure-config.json), then run this script
from the host.

SecretsManagerBackend.get_conn_value()/get_variable() call, per lookup:
  client.get_secret_value(SecretId=secrets_path)   -> GetSecretValue

That's the *only* AWS call the backend itself makes at Airflow runtime --
CreateSecret/DescribeSecret below are this script's own setup/teardown
steps (seeding the secret the backend will then read), mirroring how an
operator would provision a connection/variable secret ahead of time.
"""
import sys

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# aws2azure routes Secrets Manager by the Host header (must start with
# "secretsmanager."/"secretsmanager-", or equal "secretsmanager") -- exactly
# like real AWS endpoints, so a real deployment just needs DNS/hosts
# pointing secretsmanager.<region>.amazonaws.com (or any secretsmanager.*
# name) at the sidecar. Locally we use nip.io wildcard DNS to get a
# "secretsmanager."-prefixed hostname that still resolves to 127.0.0.1.
ENDPOINT = "http://secretsmanager.127.0.0.1.nip.io:8080"
ACCESS_KEY = "AKIA-AIRFLOW-SECRETS-SHOWCASE"
SECRET_KEY = "showcase-local-dev-secret-not-real"

# Airflow's default connections_prefix is "airflow/connections"; the secret
# id below is what SecretsManagerBackend.get_conn_value("aws_s3_showcase")
# would build: f"{connections_prefix}/{conn_id}".
CONNECTIONS_PREFIX = "airflow/connections"
CONN_ID = "aws_s3_showcase"
SECRET_ID = f"{CONNECTIONS_PREFIX}/{CONN_ID}"
# Airflow 2.3+ JSON-shaped connection secret (also accepts a bare conn URI).
SECRET_VALUE = (
    '{"conn_type": "aws", "login": "AKIA-AIRFLOW-SHOWCASE", '
    '"password": "showcase-local-dev-secret-not-real", '
    '"extra": "{\\"endpoint_url\\": \\"http://s3.aws2azure:8080\\"}"}'
)

client = boto3.client(
    "secretsmanager",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)


def step(name):
    print(f"\n=== {name} ===")


def fail(msg):
    print(f"FAIL: {msg}")
    sys.exit(1)


# 1. Setup: seed the secret an operator would have provisioned ahead of
#    time (not part of SecretsManagerBackend's own runtime call pattern).
step("CreateSecret (test setup: seeding the connection secret)")
try:
    client.create_secret(Name=SECRET_ID, SecretString=SECRET_VALUE)
    print(f"created secret '{SECRET_ID}'")
except ClientError as exc:
    code = exc.response.get("Error", {}).get("Code")
    if code == "ResourceExistsException":
        client.put_secret_value(SecretId=SECRET_ID, SecretString=SECRET_VALUE)
        print(f"secret '{SECRET_ID}' already existed, updated its value")
    else:
        raise

# 2. DescribeSecret -- sanity check the secret is visible/versioned before
#    exercising the backend's actual read path.
step("DescribeSecret (test setup: confirming the secret is visible)")
desc = client.describe_secret(SecretId=SECRET_ID)
print(f"ARN={desc.get('ARN')!r} Name={desc.get('Name')!r}")

# 3. SecretsManagerBackend._get_secret_value() -> GetSecretValue. This is
#    the one and only call Airflow's backend makes at runtime to resolve
#    `aws_s3_showcase` when it looks up a Connection.
step("GetSecretValue (SecretsManagerBackend resolving the connection)")
resp = client.get_secret_value(SecretId=SECRET_ID)
got = resp.get("SecretString")
if got != SECRET_VALUE:
    fail(f"round-trip mismatch: wrote {SECRET_VALUE!r}, got {got!r}")
print("round-trip OK, SecretsManagerBackend would resolve:")
print(got)

# 4. Cleanup.
step("DeleteSecret (test cleanup)")
# ForceDeleteWithoutRecovery maps to a Key Vault *purge*, which many tenants
# restrict via policy (Key Vault soft-delete is mandatory on modern vaults,
# and purge is a separate, often-locked-down permission). The soft-delete
# itself still succeeds even when the follow-up purge is denied, so on
# AccessDeniedException the secret is already gone -- no need to retry the
# delete, just confirm it's unresolvable (still enough to prove
# SecretsManagerBackend's actual read path, which never deletes).
try:
    client.delete_secret(SecretId=SECRET_ID, ForceDeleteWithoutRecovery=True)
except ClientError as exc:
    if exc.response.get("Error", {}).get("Code") != "AccessDeniedException":
        raise
    print("purge (hard-delete) denied by Key Vault policy -- secret is still soft-deleted")
try:
    client.describe_secret(SecretId=SECRET_ID)
    fail("expected secret to be gone after DeleteSecret")
except ClientError as exc:
    if exc.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
        raise
    print("delete confirmed, secret no longer resolvable")

print(
    "\nALL STEPS PASSED — Airflow SecretsManagerBackend's GetSecretValue call "
    "pattern round-trips cleanly through aws2azure -> Key Vault."
)

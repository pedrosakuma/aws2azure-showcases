"""
Standalone verification: exercises the exact boto3 call pattern Apache
Airflow's S3 remote-logging path uses (apache-airflow-providers-amazon
S3Hook / S3TaskHandler), pointed at the aws2azure proxy from this case
study's docker-compose stack (backed by Azurite Blob) instead of real AWS.

This does NOT drive Airflow itself -- it isolates and replays the S3 call
sequence Airflow issues, so you can validate the operations quickly without
waiting on the full webserver/scheduler/Postgres stack. Run
`docker compose up -d aws2azure azurite` first (or the full stack), then run
this script from the host.

Airflow's S3TaskHandler (airflow/providers/amazon/aws/log/s3_task_handler.py)
roughly does, per task attempt:
  1. S3Hook.check_for_bucket(bucket)              -> HeadBucket
  2. S3Hook.load_string(log, key, bucket)         -> PutObject
  3. (UI log view) S3Hook.list_keys(bucket, prefix=task_prefix) -> ListObjectsV2
  4. (UI log view) S3Hook.read_key(key, bucket)   -> GetObject
  5. (log cleanup / task retry housekeeping)      -> DeleteObject
"""
import sys
import time
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# aws2azure routes S3 by the Host header (must start with "s3." or contain
# ".s3." for virtual-hosted style) -- exactly like real AWS endpoints, so a
# real deployment just needs DNS/hosts pointing s3.<region>.amazonaws.com (or
# any s3.* name) at the sidecar. Locally we use nip.io wildcard DNS to get an
# "s3."-prefixed hostname that still resolves to 127.0.0.1.
ENDPOINT = "http://s3.127.0.0.1.nip.io:8080"
ACCESS_KEY = "AKIA-AIRFLOW-SHOWCASE"
SECRET_KEY = "showcase-local-dev-secret-not-real"
BUCKET = "airflow-logs"

# Airflow's default remote log key template (Airflow 2.x):
# "dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log"
DAG_ID = "example_bash_operator"
RUN_ID = "scheduled__2026-08-27T00:00:00+00:00"
TASK_ID = "runme_0"
ATTEMPT = 1
LOG_KEY = f"dag_id={DAG_ID}/run_id={RUN_ID}/task_id={TASK_ID}/attempt={ATTEMPT}.log"
LOG_BODY = (
    "[2026-08-27T20:38:00.123+0000] {taskinstance.py:1157} INFO - "
    "Starting attempt 1 of 1\n"
    "[2026-08-27T20:38:01.456+0000] {bash.py:190} INFO - Running: echo hello\n"
    "[2026-08-27T20:38:01.789+0000] {taskinstance.py:1401} INFO - "
    "Marking task as SUCCESS.\n"
).encode("utf-8")

client = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
)

def step(name):
    print(f"\n=== {name} ===")

def fail(msg):
    print(f"FAIL: {msg}")
    sys.exit(1)

# 1. S3Hook.check_for_bucket() -> HeadBucket (create if missing, like a
#    first-run operator/admin would via S3Hook.create_bucket()).
step("HeadBucket / CreateBucket (S3Hook.check_for_bucket)")
try:
    client.head_bucket(Bucket=BUCKET)
    print(f"bucket '{BUCKET}' already exists")
except ClientError:
    client.create_bucket(Bucket=BUCKET)
    print(f"created bucket '{BUCKET}'")

# 2. S3Hook.load_string(log_text, key, bucket, replace=True) -> PutObject
step("PutObject (S3Hook.load_string writing task attempt log)")
client.put_object(Bucket=BUCKET, Key=LOG_KEY, Body=LOG_BODY)
print(f"wrote {len(LOG_BODY)} bytes to s3://{BUCKET}/{LOG_KEY}")

# 3. S3Hook.list_keys(bucket, prefix=f"dag_id={DAG_ID}/...") -> ListObjectsV2
#    (this is how the Airflow webserver enumerates attempts for a task)
step("ListObjectsV2 (S3Hook.list_keys enumerating attempts for the task)")
prefix = f"dag_id={DAG_ID}/run_id={RUN_ID}/task_id={TASK_ID}/"
resp = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
keys = [obj["Key"] for obj in resp.get("Contents", [])]
print(f"prefix={prefix!r} -> {keys}")
if LOG_KEY not in keys:
    fail(f"expected {LOG_KEY} in listing, got {keys}")

# 4. S3Hook.read_key(key, bucket) -> GetObject (webserver rendering the log)
step("GetObject (S3Hook.read_key rendering the log in the UI)")
got = client.get_object(Bucket=BUCKET, Key=LOG_KEY)
body = got["Body"].read()
if body != LOG_BODY:
    fail(f"round-trip mismatch: wrote {LOG_BODY!r}, got {body!r}")
print(f"round-trip OK, {len(body)} bytes match exactly")
print(body.decode("utf-8"))

# 5. DeleteObject (log retention / cleanup housekeeping)
step("DeleteObject (log retention cleanup)")
client.delete_object(Bucket=BUCKET, Key=LOG_KEY)
resp = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
remaining = [obj["Key"] for obj in resp.get("Contents", [])]
if remaining:
    fail(f"expected empty listing after delete, got {remaining}")
print("delete confirmed, listing now empty")

print("\nALL STEPS PASSED — Airflow's S3 remote-logging call pattern round-trips "
      "cleanly through aws2azure -> Azurite Blob.")

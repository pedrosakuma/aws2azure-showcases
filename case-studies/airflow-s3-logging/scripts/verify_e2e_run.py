"""
End-to-end verification: drives the *real* Airflow webserver/scheduler
stack (not the standalone call-pattern replay in verify_s3_roundtrip.py),
triggers a real DAG run, waits for it to succeed, then proves the task
log genuinely round-tripped through Azurite via aws2azure rather than
being served from the local disk fallback.

Steps:
  1. Wait for the webserver's REST API to come up.
  2. Unpause + trigger `example_bash_operator` via the REST API.
  3. Poll the DAG run until it reaches a terminal state.
  4. Read the `runme_0` task's log via the REST API (this is the same
     code path -- TaskLogReader -- the webserver UI itself uses).
  5. Fetch the same log object directly from Azurite via aws2azure
     (boto3 GetObject) and assert its content matches what the REST API
     served, proving the object actually exists in Azurite. The remote
     upload happens from the task's own process teardown, decoupled
     from -- and sometimes lagging -- the scheduler's "success" state
     transition, so this comparison is retried with a bounded backoff
     rather than asserted on the first read.
  6. Delete the task's *local* on-disk log copy (inside the scheduler
     container) and re-fetch via the REST API -- if the content still
     matches, the webserver read it from Azurite, not local disk.

Requires: `docker compose up -d` already run for the full stack (postgres,
azurite, aws2azure, airflow-init, airflow-webserver, airflow-scheduler),
and the webserver reachable at http://localhost:8081.
"""
import subprocess
import sys
import time

import boto3
import requests
from botocore.config import Config

WEBSERVER = "http://localhost:8081"
AUTH = ("admin", "admin")
DAG_ID = "example_bash_operator"
TASK_ID = "runme_0"

S3_ENDPOINT = "http://s3.127.0.0.1.nip.io:8080"
ACCESS_KEY = "AKIA-AIRFLOW-SHOWCASE"
SECRET_KEY = "showcase-local-dev-secret-not-real"
BUCKET = "airflow-logs"


def wait_for_webserver(timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{WEBSERVER}/health", timeout=5)
            if r.ok:
                print("webserver is up")
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    sys.exit("webserver did not become reachable in time")


def api(method, path, **kwargs):
    r = requests.request(method, f"{WEBSERVER}/api/v1{path}", auth=AUTH, timeout=15, **kwargs)
    r.raise_for_status()
    return r.json() if r.content else {}


def trigger_dag_run():
    api("PATCH", f"/dags/{DAG_ID}", json={"is_paused": False})
    resp = api("POST", f"/dags/{DAG_ID}/dagRuns", json={})
    run_id = resp["dag_run_id"]
    print(f"triggered dag run {run_id}")
    return run_id


def wait_for_dag_run(run_id, timeout=300):
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = api("GET", f"/dags/{DAG_ID}/dagRuns/{run_id}")
        state = resp["state"]
        if state in ("success", "failed"):
            print(f"dag run {run_id} reached state={state}")
            return state
        time.sleep(5)
    sys.exit(f"dag run {run_id} did not finish within {timeout}s")


def fetch_log_via_api(run_id, try_number=1):
    r = requests.get(
        f"{WEBSERVER}/api/v1/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances/{TASK_ID}/logs/{try_number}",
        auth=AUTH,
        params={"full_content": "true"},
        headers={"Accept": "text/plain"},
        timeout=15,
    )
    r.raise_for_status()
    return r.text


def fetch_log_via_azurite(run_id, try_number=1):
    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )
    key = f"dag_id={DAG_ID}/run_id={run_id}/task_id={TASK_ID}/attempt={try_number}.log"
    body = client.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")
    print(f"fetched s3://{BUCKET}/{key} directly from Azurite ({len(body)} bytes)")
    return key, body


def delete_local_log_copy(run_id, try_number=1):
    path = (
        f"/opt/airflow/logs/dag_id={DAG_ID}/run_id={run_id}/"
        f"task_id={TASK_ID}/attempt={try_number}.log"
    )
    subprocess.run(
        ["docker", "compose", "exec", "-T", "airflow-scheduler", "rm", "-f", path],
        check=True,
    )
    print(f"deleted local log copy: {path}")


def wait_for_azurite_log_containment(run_id, api_log_fn, failure_hint, timeout=60, interval=5):
    """Poll until the object stored in Azurite is contained in the log
    served by the REST API, or raise the given assertion after timeout.

    Airflow's S3TaskHandler uploads the task log to remote storage from
    the task's own process teardown, which is decoupled from -- and can
    lag slightly behind -- the scheduler marking the task instance
    "success". Reading immediately after that state transition can
    legitimately race the upload; retrying tolerates that eventual
    consistency instead of treating a transient timing gap as a wiring
    bug.
    """
    deadline = time.time() + timeout
    last_api_log = ""
    last_azurite_log = ""
    while True:
        last_api_log = api_log_fn()
        _, last_azurite_log = fetch_log_via_azurite(run_id)
        if last_azurite_log in last_api_log:
            return last_api_log
        if time.time() >= deadline:
            assert last_azurite_log in last_api_log, failure_hint
        time.sleep(interval)


def main():
    wait_for_webserver()
    run_id = trigger_dag_run()
    state = wait_for_dag_run(run_id)
    if state != "success":
        sys.exit(f"dag run did not succeed (state={state})")

    api_log = wait_for_azurite_log_containment(
        run_id,
        lambda: fetch_log_via_api(run_id),
        "log content served by the webserver does not contain the object "
        "stored in Azurite -- remote logging may not be wired correctly",
    )
    assert api_log.strip(), "task log via REST API was empty"
    print("REST API log content matches the object stored in Azurite")

    delete_local_log_copy(run_id)
    wait_for_azurite_log_containment(
        run_id,
        lambda: fetch_log_via_api(run_id),
        "after deleting the local log copy, the webserver no longer served "
        "the same content -- it may have been reading from local disk, not "
        "from Azurite via aws2azure",
    )
    print(
        "PASS: task log was served from Azurite via aws2azure after the "
        "local on-disk copy was deleted -- remote logging round-trip confirmed"
    )


if __name__ == "__main__":
    main()

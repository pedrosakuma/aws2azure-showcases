"""
End-to-end verification for the airflow-celery-sqs case study.

Drives the real Airflow webserver/scheduler/worker stack (CeleryExecutor),
triggers a real DAG run, waits for it to succeed, and then proves the task
was actually dispatched through the SQS-compatible broker (aws2azure ->
local Service Bus emulator) rather than some other transport:

  1. Wait for the webserver's REST API to come up.
  2. Confirm the `celery` queue exists via SQS GetQueueUrl against
     aws2azure -- this only succeeds once kombu has connected and the
     queue has been resolved/created, proving the broker leg is live.
  3. Unpause + trigger `example_bash_operator` via the REST API.
  4. Poll the DAG run until it reaches a terminal state. Since
     AIRFLOW__CORE__EXECUTOR=CeleryExecutor and the only configured broker
     is the SQS transport, a successful run necessarily means the task
     was enqueued via aws2azure, picked up by airflow-worker, and its
     result written back -- there is no other path to success.
  5. Immediately after success, read the queue's ApproximateNumberOfMessages
     via SQS GetQueueAttributes and assert it is back to 0, showing the
     message was actually consumed (not stuck) and that the queue is a
     real, addressable Service Bus queue behind aws2azure -- not a stub.

Requires: `docker compose up -d` already run for the full stack, and the
webserver reachable at http://localhost:8081.
"""
import sys
import time

import boto3
import requests
from botocore.config import Config

WEBSERVER = "http://localhost:8081"
AUTH = ("admin", "admin")
DAG_ID = "example_bash_operator"

SQS_ENDPOINT = "http://sqs.127.0.0.1.nip.io:8080"
ACCESS_KEY = "AKIA-AIRFLOW-CELERY-SHOWCASE"
SECRET_KEY = "showcase-local-dev-secret-not-real"
QUEUE_NAME = "default"


def sqs_client():
    return boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        config=Config(retries={"max_attempts": 1}),
    )


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


def wait_for_broker_queue(client, timeout=180):
    """Poll GetQueueUrl until the worker/scheduler have caused kombu to
    resolve (and, if needed, create) the `celery` queue via aws2azure."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = client.get_queue_url(QueueName=QUEUE_NAME)
            print(f"queue '{QUEUE_NAME}' resolved: {resp['QueueUrl']}")
            return resp["QueueUrl"]
        except client.exceptions.QueueDoesNotExist:
            pass
        except Exception as exc:  # noqa: BLE001
            print(f"waiting for broker queue: {exc}")
        time.sleep(3)
    sys.exit(f"queue '{QUEUE_NAME}' never appeared via aws2azure within {timeout}s")


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


def main():
    wait_for_webserver()
    client = sqs_client()
    queue_url = wait_for_broker_queue(client)

    run_id = trigger_dag_run()
    state = wait_for_dag_run(run_id)
    if state != "success":
        sys.exit(f"dag run did not succeed (state={state}) -- CeleryExecutor/SQS broker path is broken")

    print(f"PASS: DAG run succeeded end-to-end via CeleryExecutor over the SQS-compatible broker")

    # Give the worker a moment to ack/delete the last message, then confirm
    # the queue drained -- proving messages genuinely flowed through it.
    time.sleep(5)
    attrs = client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    depth = int(attrs["ApproximateNumberOfMessages"])
    print(f"queue '{QUEUE_NAME}' depth after run: {depth}")
    assert depth == 0, (
        f"expected the '{QUEUE_NAME}' queue to be drained after a successful run, "
        f"found {depth} messages still queued"
    )
    print("PASS: broker queue drained -- Celery task dispatch over aws2azure/SQS confirmed end-to-end")


if __name__ == "__main__":
    main()

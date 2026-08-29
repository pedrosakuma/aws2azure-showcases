"""
End-to-end verification: drives the *real* Airflow webserver/scheduler
stack, triggers the sns_publish_showcase DAG (which uses Airflow's
unmodified SnsPublishOperator), waits for it to succeed, then proves the
message was genuinely published through aws2azure to Azure Service Bus
Topics by reading it directly off the backing Service Bus topic
subscription with the native azure-servicebus SDK.

Why not just read it back via SQS/boto3? aws2azure's SNS Subscribe is
publish-side-only by design: it records a Service Bus topic subscription
but never forwards published messages out to an SQS queue or HTTP(S)
endpoint (see docs/gaps/sns/Subscribe.yaml "Subscriber delivery forwarder"
in the aws2azure repo -- explicitly WON'T IMPLEMENT / non_goal, since active
push delivery would require a stateful, always-on dispatcher outside this
stateless proxy's scope). The documented supported way to consume these
messages is a native Azure Service Bus consumer, which is exactly what this
script does on the read side -- aws2azure is only exercised on the write
(publish) side, via Airflow's real SnsPublishOperator.

Requires: `docker compose up -d` already run for the full stack (postgres,
mssql, servicebus-emulator, aws2azure, airflow-init, airflow-webserver,
airflow-scheduler), and the webserver reachable at http://localhost:8081
and the Service Bus emulator's AMQP/management ports reachable at
localhost:5672/5300.
"""
import hashlib
import sys
import time

import requests
from azure.servicebus import ServiceBusClient

WEBSERVER = "http://localhost:8081"
AUTH = ("admin", "admin")
DAG_ID = "sns_publish_showcase"

REGION = "us-east-1"
TOPIC_NAME = "airflow-sns-showcase"
TOPIC_ARN = f"arn:aws:sns:{REGION}:000000000000:{TOPIC_NAME}"
SQS_ENDPOINT_ARN = f"arn:aws:sqs:{REGION}:000000000000:showcase-fanout-queue"
MESSAGE_BODY = (
    "Hello from an unmodified Airflow SnsPublishOperator, routed through "
    "aws2azure to Azure Service Bus Topics."
)
MESSAGE_SUBJECT = "aws2azure SNS showcase"

SERVICE_BUS_CONNECTION_STRING = (
    "Endpoint=sb://localhost:5672;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;"
    "UseDevelopmentEmulator=true;"
)


def compute_subscription_id(topic_arn: str, protocol: str, endpoint: str) -> str:
    """Mirrors aws2azure's SnsSubscriptionSupport.CreateSubscriptionId:
    the first 20 lowercase hex characters of SHA-256("{topicArn}\\n{protocol}\\n{endpoint}").
    This lets the script address the exact Service Bus subscription name
    aws2azure's Subscribe call deterministically derives, without needing
    to round-trip aws2azure's own SubscriptionArn response.
    """
    payload = f"{topic_arn}\n{protocol}\n{endpoint}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:20]


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


def dump_task_logs(run_id):
    """Prints every task instance's log for this DAG run -- `docker compose
    logs` doesn't capture task logs (LocalExecutor writes them to files
    inside the container, not to the scheduler's stdout), so this is the
    only way to see *why* a task failed from outside the container.
    """
    try:
        task_instances = api("GET", f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances")["task_instances"]
    except requests.RequestException as exc:
        print(f"could not list task instances for {run_id}: {exc}")
        return
    for ti in task_instances:
        task_id = ti["task_id"]
        print(f"--- log for task {task_id} (state={ti['state']}) ---")
        try:
            r = requests.get(
                f"{WEBSERVER}/api/v1/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1",
                auth=AUTH,
                params={"full_content": "true"},
                headers={"Accept": "text/plain"},
                timeout=15,
            )
            print(r.text)
        except requests.RequestException as exc:
            print(f"could not fetch log for task {task_id}: {exc}")


def wait_for_servicebus_emulator(timeout=120):
    """Waits for the emulator's AMQP port to accept TCP connections. A
    plain socket probe (rather than an SDK-level operation) avoids the
    azure-servicebus SDK's own internal connection retry/backoff muddying
    this wait loop's timing.
    """
    import socket

    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            with socket.create_connection(("localhost", 5672), timeout=3):
                print("service bus emulator AMQP port is reachable")
                return
        except OSError as exc:
            last_error = exc
        time.sleep(3)
    sys.exit(f"service bus emulator did not become reachable in time: {last_error}")


def receive_published_message(subscription_id: str, timeout=60):
    with ServiceBusClient.from_connection_string(SERVICE_BUS_CONNECTION_STRING) as client:
        with client.get_subscription_receiver(
            topic_name=TOPIC_NAME, subscription_name=subscription_id, max_wait_time=timeout
        ) as receiver:
            messages = receiver.receive_messages(max_message_count=1, max_wait_time=timeout)
            if not messages:
                sys.exit(
                    f"no message received from Service Bus topic '{TOPIC_NAME}' "
                    f"subscription '{subscription_id}' within {timeout}s -- publish "
                    "may not have reached the backing Service Bus topic subscription"
                )
            message = messages[0]
            receiver.complete_message(message)
            return message


def main():
    wait_for_webserver()
    wait_for_servicebus_emulator()

    subscription_id = compute_subscription_id(TOPIC_ARN, "sqs", SQS_ENDPOINT_ARN)
    print(f"expected Service Bus subscription name: {subscription_id}")

    run_id = trigger_dag_run()
    state = wait_for_dag_run(run_id)
    if state != "success":
        dump_task_logs(run_id)
        sys.exit(f"dag run did not succeed (state={state})")

    message = receive_published_message(subscription_id)
    body = str(message)
    assert body == MESSAGE_BODY, (
        f"message body read from the Service Bus topic subscription does not match "
        f"what SnsPublishOperator sent: got {body!r}"
    )

    def _prop(props, key):
        # application_properties keys are documented as always `str`, but
        # some SDK/broker version combinations have been observed to hand
        # back bytes-encoded AMQP symbols instead -- check both to stay
        # robust across azure-servicebus SDK versions.
        value = props.get(key, props.get(key.encode("utf-8")))
        return value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else value

    props = message.application_properties or {}
    subject = _prop(props, "aws.sns.Subject")
    assert subject == MESSAGE_SUBJECT, f"unexpected Subject application property: {subject!r}"

    attr_value = _prop(props, "showcase")
    assert attr_value == "airflow-sns-fanout", f"unexpected 'showcase' message attribute: {attr_value!r}"

    print(
        "PASS: message published by Airflow's unmodified SnsPublishOperator via "
        "aws2azure was read back directly from the backing Azure Service Bus "
        "topic subscription, with Subject and MessageAttributes intact"
    )


if __name__ == "__main__":
    main()

"""
Publishes a message to an SNS topic using Airflow's unmodified
SnsPublishOperator, routed entirely through aws2azure to Azure Service Bus
Topics -- zero Airflow/provider code changes, only the `aws_sns_showcase`
connection's endpoint_url pointed at the proxy.

IMPORTANT: aws2azure's SNS Subscribe is publish-side-only by design (see
docs/gaps/sns/Subscribe.yaml "Subscriber delivery forwarder" in the
aws2azure repo -- explicitly WON'T IMPLEMENT / non_goal). It records a
Subscribe call as an Azure Service Bus topic subscription but never
actively forwards published messages out to an SQS queue or HTTP(S)
endpoint. The `bootstrap_topic_and_subscription` task below still calls
Subscribe with Protocol="sqs" to demonstrate the real call shape a fanout
config would use, but the SQS endpoint ARN is symbolic -- nothing is ever
delivered to it. scripts/verify_e2e_run.py proves the publish actually
worked by reading the message directly off the backing Service Bus topic
subscription with the native azure-servicebus SDK, which is the documented
supported way to consume these messages.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.sns import SnsHook
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator

AWS_CONN_ID = "aws_sns_showcase"
TOPIC_NAME = "airflow-sns-showcase"
REGION = "us-east-1"
# Matches aws2azure's synthetic ARN shape: arn:aws:sns:{region}:000000000000:{topicName}
TOPIC_ARN = f"arn:aws:sns:{REGION}:000000000000:{TOPIC_NAME}"
# Symbolic fanout target -- see module docstring: aws2azure never actually
# delivers to this endpoint, it only proves Subscribe accepts the
# protocol=sqs call shape a real SNS->SQS fanout configuration would use.
SQS_ENDPOINT_ARN = f"arn:aws:sqs:{REGION}:000000000000:showcase-fanout-queue"
MESSAGE_BODY = "Hello from an unmodified Airflow SnsPublishOperator, routed through aws2azure to Azure Service Bus Topics."
MESSAGE_SUBJECT = "aws2azure SNS showcase"


def _bootstrap_topic_and_subscription(**_context):
    hook = SnsHook(aws_conn_id=AWS_CONN_ID)
    client = hook.get_conn()
    client.create_topic(Name=TOPIC_NAME)
    client.subscribe(TopicArn=TOPIC_ARN, Protocol="sqs", Endpoint=SQS_ENDPOINT_ARN)


with DAG(
    dag_id="sns_publish_showcase",
    description="Publishes to SNS via aws2azure -> Azure Service Bus Topics",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["showcase", "sns"],
) as dag:
    bootstrap_topic_and_subscription = PythonOperator(
        task_id="bootstrap_topic_and_subscription",
        python_callable=_bootstrap_topic_and_subscription,
    )

    publish_message = SnsPublishOperator(
        task_id="publish_message",
        aws_conn_id=AWS_CONN_ID,
        target_arn=TOPIC_ARN,
        message=MESSAGE_BODY,
        subject=MESSAGE_SUBJECT,
        message_attributes={"showcase": {"DataType": "String", "StringValue": "airflow-sns-fanout"}},
    )

    bootstrap_topic_and_subscription >> publish_message

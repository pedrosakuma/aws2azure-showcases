# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
App-level AWS-vs-Azure differential DAG (issue #10).

This DAG's *task sequence* is adapted from Apache Airflow's own official
system test for the S3 provider:
  https://github.com/apache/airflow/blob/main/providers/amazon/tests/system/amazon/aws/example_s3.py
(operators: S3CreateBucketOperator, S3PutBucketTaggingOperator,
S3GetBucketTaggingOperator, S3CreateObjectOperator, S3ListOperator,
S3DeleteObjectsOperator, S3DeleteBucketOperator.)

It is **not** the file vendored verbatim: the upstream file depends on
Airflow's internal system-test harness (`SystemTestContextBuilder`, the
`watcher()` pattern, SSM-backed `Variable` fetching) which only exists
inside Airflow's own test suite, not as an importable package in a plain
Airflow deployment -- so it cannot run unmodified in a regular DAGs folder.
This DAG uses the *same operators, in the same order, against the same
bucket lifecycle* as that system test, adapted to run as an ordinary DAG
via the REST API in `scripts/run_differential.py`, against either backend:

  - LocalStack S3 directly (no aws2azure) -- the "ground truth" AWS-shaped run.
  - aws2azure + Azurite -- the "translated" run this repo's case studies use.

The `summarize` task's return value (an XCom) is what the runner script
diffs between the two runs.

`S3ReadObjectOperator` (used by the upstream example) isn't available in
the amazon provider version bundled with `apache/airflow:2.10.4`
(introduced later) -- `read_object` below calls the same
`S3Hook.read_key` it wraps.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
    S3DeleteObjectsOperator,
    S3GetBucketTaggingOperator,
    S3ListOperator,
    S3PutBucketTaggingOperator,
)

DAG_ID = "differential_s3"
BUCKET = "differential-s3-bucket"
KEY = "differential-key.txt"
DATA = "apple,0.5\nmilk,2.5\nbread,4.0\n"
TAG_KEY = "differential-test-key"
TAG_VALUE = "differential-test-value"
CONN_ID = "aws_target"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["differential"],
) as dag:
    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET,
        aws_conn_id=CONN_ID,
    )

    put_tagging = S3PutBucketTaggingOperator(
        task_id="put_tagging",
        bucket_name=BUCKET,
        key=TAG_KEY,
        value=TAG_VALUE,
        aws_conn_id=CONN_ID,
    )

    get_tagging = S3GetBucketTaggingOperator(
        task_id="get_tagging",
        bucket_name=BUCKET,
        aws_conn_id=CONN_ID,
    )

    create_object = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=BUCKET,
        s3_key=KEY,
        data=DATA,
        replace=True,
        aws_conn_id=CONN_ID,
    )

    @task(task_id="read_object")
    def read_object() -> str:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id=CONN_ID)
        return hook.read_key(key=KEY, bucket_name=BUCKET)

    list_keys = S3ListOperator(
        task_id="list_keys",
        bucket=BUCKET,
        prefix="",
        aws_conn_id=CONN_ID,
    )

    delete_objects = S3DeleteObjectsOperator(
        task_id="delete_objects",
        bucket=BUCKET,
        keys=[KEY],
        aws_conn_id=CONN_ID,
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET,
        aws_conn_id=CONN_ID,
    )

    @task(task_id="summarize")
    def summarize(tagging: list, content: str, keys: list) -> dict:
        # get_bucket_tagging already returns a clean `[{"Key": ..., "Value": ...}]`
        # list (via S3Hook.get_bucket_tagging), so no response-metadata
        # normalization is needed here.
        return {
            "tagging": sorted(tagging, key=lambda t: t["Key"]),
            "object_content": content,
            "listed_keys": sorted(keys),
        }

    read_object_result = read_object()
    summary = summarize(
        tagging=get_tagging.output,
        content=read_object_result,
        keys=list_keys.output,
    )

    (
        create_bucket
        >> put_tagging
        >> get_tagging
        >> create_object
        >> read_object_result
        >> list_keys
        >> summary
        >> delete_objects
        >> delete_bucket
    )

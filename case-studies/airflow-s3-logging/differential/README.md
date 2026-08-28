# App-level differential: LocalStack vs. aws2azure+Azurite (issue #10)

This complements `aws2azure`'s own wire-protocol conformance suite
(`tests/Aws2Azure.Conformance`, Tier 2 — proxy-over-Azurite vs LocalStack S3
at the HTTP response level) with an **application-level** check: does the
exact same Airflow DAG produce an identical final state whether Airflow talks
to a real-AWS-shaped backend (LocalStack S3 directly) or to Azure (via
aws2azure + Azurite)?

Before this, nothing in this repo (or upstream) actually proved that — we
could show "this app works against Azure via aws2azure" and, separately,
"the wire protocol matches AWS," but never "the same official-style Airflow
test produces an equivalent outcome on both backends."

## What it runs

`dags/differential_s3.py` adapts the **operator sequence** from Apache
Airflow's own official system test for the S3 provider:
<https://github.com/apache/airflow/blob/main/providers/amazon/tests/system/amazon/aws/example_s3.py>
(`S3CreateBucketOperator` → `S3PutBucketTaggingOperator` →
`S3GetBucketTaggingOperator` → `S3CreateObjectOperator` → read → `S3ListOperator`
→ `S3DeleteObjectsOperator` → `S3DeleteBucketOperator`), ending in a
`summarize` task whose XCom (bucket tagging, object content read back,
listed keys) is what gets diffed between backends.

It is **not** that file vendored verbatim: the upstream file depends on
Airflow's internal system-test harness (`SystemTestContextBuilder`, the
`watcher()` pattern, SSM-backed `Variable` fetching), which only exists
inside Airflow's own test suite, not as an importable package in a plain
Airflow deployment. This DAG uses the same operators, in the same order,
against the same bucket lifecycle, adapted to run as an ordinary DAG.

## How it runs

`scripts/run_differential.py` runs the DAG twice, sequentially (so both get
the same host ports), tearing each stack fully down before the next starts:

1. **`docker-compose.localstack.yml`** — Airflow's `aws_target` connection
   points directly at a LocalStack S3 container. No aws2azure in the path.
   This is the "ground truth" AWS-shaped run.
2. **`docker-compose.aws2azure.yml`** — Airflow's `aws_target` connection
   points at aws2azure, backed by Azurite. This is the "translated" run —
   the same setup the `airflow-s3-logging` case study uses.

For each run it triggers the DAG via the REST API, waits for `success`,
and fetches the `summarize` task's XCom. The final step diffs the two
summaries and fails on any mismatch.

## Running it locally

```bash
cd case-studies/airflow-s3-logging/differential
python3 -m venv .venv && .venv/bin/pip install requests
.venv/bin/python scripts/run_differential.py
```

Requires Docker. Verified locally: both backends produce byte-identical
`tagging`, `object_content`, and `listed_keys` in the final summary.

## Known limits

- Covers only the S3 operator surface exercised by `differential_s3.py`
  (bucket create/tag/get-tag, object put/read/list, object/bucket delete).
  Extending to Secrets Manager, SQS, etc. would need analogous DAGs per
  service.
- `S3ReadObjectOperator` (used by the upstream example) isn't available in
  the amazon provider version bundled with `apache/airflow:2.10.4`
  (introduced in a later provider release) — `read_object` here calls the
  same `S3Hook.read_key` it wraps under the hood.
- Runs sequentially, not in parallel, to avoid host port/network conflicts
  between the two stacks; a full run takes a few minutes.

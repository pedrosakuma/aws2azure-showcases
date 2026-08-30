# fluentbit-kinesis: Fluent Bit's real `kinesis_streams` output plugin, via aws2azure, over Azure Event Hubs

## What this demonstrates

[Fluent Bit](https://fluentbit.io/)'s core `kinesis_streams` output plugin
(the official AWS-maintained plugin, written in C -- a completely different
SDK implementation from the Python/JVM clients this repo's other case
studies use) batches log records into a Kinesis Data Stream via the
`PutRecords` API. It is configured entirely through its standard `endpoint`
/ `port` / `tls` options pointed at aws2azure, and standard AWS credential
environment variables -- zero code changes, config only, the same pattern
proven by every other case study in this repo.

This case study runs Fluent Bit with its own real, unmodified `dummy` input
plugin generating five deterministic synthetic log records, ships them
through `kinesis_streams` -> aws2azure -> an Azure Event Hub, then a
verification script reads them back via plain boto3
(`DescribeStream` / `GetShardIterator` / `GetRecords`) against the *same*
aws2azure endpoint -- proving the full write path with a real client, and
exercising the read path (`GetShardIterator`/`GetRecords`/`DescribeStream`)
directly, since aws2azure's Kinesis implementation has no analogous
"publish-side only" gap the way SNS Subscribe does.

## What this does NOT prove -- read this before drawing conclusions

aws2azure's Kinesis module (backed by Azure Event Hubs) has real, documented
design gaps around sequence numbers and iterator continuity
(`docs/gaps/kinesis/_design.yaml` in the aws2azure repo):

- **Sequence numbers are synthetic**, minted by the proxy as
  `(unixMs << 20) | counter`, not true AWS-style monotonic sequence numbers.
  Multiple records published within the same millisecond can share ordering
  ambiguity at the boundary.
- **`TRIM_HORIZON` (read from the beginning) is what this script uses**,
  specifically because it is the one iterator type unaffected by that
  synthetic-sequence-number caveat. This case study does **not** exercise
  `AT_SEQUENCE_NUMBER` / `AFTER_SEQUENCE_NUMBER` replay-from-a-specific-point
  semantics, which the gap doc calls out as best-effort only.
- **Regional failover / Geo-DR continuity is not exercised.** This is a
  single-emulator, single-region local stack.
- **Shard-level throughput throttling is not simulated locally.** aws2azure
  does not enforce AWS-style per-shard TPS/byte budgets; only Azure-side
  throttles (if any) would surface as `ProvisionedThroughputExceededException`.

This case study therefore validates aws2azure's Kinesis **write path**
(`PutRecords`, exercised by a real, unmodified Fluent Bit plugin) and a
**simple, from-the-beginning read path**
(`DescribeStream` / `GetShardIterator(TRIM_HORIZON)` / `GetRecords`), but
does not validate exact-replay / sequence-number-boundary semantics or
regional failover.

## Stack

- **Azure Event Hubs emulator** (`mcr.microsoft.com/azure-messaging/eventhubs-emulator`,
  the same official image and `Config.json` shape aws2azure's own Kinesis
  integration tests use) + **Azurite**, providing the emulator's blob/metadata
  backend. The Event Hubs emulator does not support dynamic entity creation
  (unlike the Service Bus emulator the SQS/SNS case studies use), so the
  namespace (`showcaseNs1`) and event hub (`fluentbit-logs`, 1 partition) are
  declared statically in `deploy/eventhubs/Config.json` and mirrored in
  `config/aws2azure-config.json`'s `streams` map.
- **aws2azure**, built directly from the `main` branch of the upstream repo
  via a remote git build context (no published image yet), configured with
  a single Kinesis binding (`kind: eventHubs`) routed via the
  `kinesis.aws2azure` network alias.
- **Fluent Bit** (`fluent/fluent-bit`), configured with its real `dummy`
  input (5 deterministic synthetic records) and real `kinesis_streams`
  output pointed at aws2azure.

## Running it locally

```bash
cd case-studies/fluentbit-kinesis
docker compose up --build -d

# aws2azure routes by Host header (must start with "kinesis."); map a
# hostname the verification script uses to localhost so the Authorization
# header aws2azure receives on the read side matches what Fluent Bit's
# writes were routed by inside the docker network.
echo "127.0.0.1 kinesis.aws2azure.fluentbit-showcase.local" | sudo tee -a /etc/hosts

python3 -m venv .venv
.venv/bin/pip install --quiet boto3
.venv/bin/python scripts/verify_e2e_run.py

docker compose down -v
```

## How verification works

1. `scripts/verify_e2e_run.py` polls `DescribeStream` until aws2azure reports
   the expected single-shard topology (`shardId-000000000000`) -- this also
   acts as an aws2azure-and-Event-Hubs-emulator readiness check, since
   `DescribeStream` only succeeds once the proxy can reach the backing
   Event Hub.
2. It then calls `GetShardIterator` with `ShardIteratorType=TRIM_HORIZON` and
   loops `GetRecords` until it collects the 5 records Fluent Bit's `dummy`
   input was configured to generate (`Samples=5`), or a 90-second timeout
   elapses.
3. Each record's `Data` is base64-decoded (defensively, since some botocore
   versions hand back an already-decoded `bytes` value for blob shapes and
   others hand back a base64 `str`) and parsed as JSON, then asserted to
   match the exact `message` and `showcase` fields Fluent Bit's `dummy`
   input plugin was configured to emit.
4. Success requires the write path (`kinesis_streams` -> aws2azure ->
   Event Hubs) and the read path (`DescribeStream` /
   `GetShardIterator` / `GetRecords` -> aws2azure -> Event Hubs) to both
   have worked correctly end-to-end, with a real, unmodified,
   non-Python/non-JVM AWS SDK client on the write side.

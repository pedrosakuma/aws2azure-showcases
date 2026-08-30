# fluentbit-kinesis: Fluent Bit's real `kinesis_streams` output plugin, via aws2azure, over Azure Event Hubs

## What this demonstrates

[Fluent Bit](https://fluentbit.io/)'s core `kinesis_streams` output plugin
(the official AWS-maintained plugin, written in C -- a completely different
SDK implementation from the Python/JVM clients this repo's other case
studies use) batches log records into a Kinesis Data Stream via the
`PutRecords` API. It is configured entirely through its standard `endpoint`
/ `port` options pointed at aws2azure, and standard AWS credential
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
  namespace (`emulatorNs1`, a fixed name the emulator requires) and event hub (`fluentbit-logs`, 1 partition) are
  declared statically in `deploy/eventhubs/Config.json` and mirrored in
  `config/aws2azure-config.json`'s `streams` map.
- **aws2azure**, built directly from the `main` branch of the upstream repo
  via a remote git build context (no published image yet), configured with
  a single Kinesis binding (`kind: eventHubs`) routed via the
  `kinesis.aws2azure` network alias.
- **kinesis-tls**: a small self-signed-cert nginx sidecar terminating TLS
  for Fluent Bit's write path only (see "A real Fluent Bit constraint"
  below) and forwarding to aws2azure in plaintext with the Host header
  intact.
- **Fluent Bit** (`fluent/fluent-bit`, 3.2.2+ -- see below for why), configured
  with its real `dummy` input (5 deterministic synthetic records) and real
  `kinesis_streams` output pointed at the `kinesis-tls` sidecar.

## A real Fluent Bit constraint this surfaced

Every released version of Fluent Bit's `kinesis_streams` output plugin
(checked through 3.2.x, source: `plugins/out_kinesis_streams/kinesis.c`)
unconditionally wraps its upstream connection in TLS -- it passes
`FLB_IO_TLS` to `flb_upstream_create()` with no config option to disable
it. Versions before 3.2.0 additionally hardcode port 443 regardless of any
`Port` setting (`a90daaf0` added custom-port support on 2024-08-30, first
in the 3.2.0 release); this case study therefore requires Fluent Bit
**3.2.2**, not the 3.1.9 pinned by the other case studies' baseline choices.
Because aws2azure only serves plain HTTP (by design -- TLS termination is
expected to happen at the infrastructure layer or not be needed on a
private network), this case study runs a small self-signed-cert nginx
sidecar (`deploy/tls-proxy/`) purely to satisfy this plugin's hardcoded TLS
requirement; a real deployment fronting aws2azure with real TLS
termination (a service mesh sidecar, an ALB/Envoy, etc.) would not need
anything special here. The plugin also hardcodes cert verification on for
that same connection (`flb_tls_create(..., verify=TRUE, ...)`, ignoring the
generic `tls.verify` output property entirely), so a self-signed cert can't
be accepted by turning verification off -- `tls.ca_file` (which the plugin
does forward) points Fluent Bit at the sidecar's own checked-in dev cert
instead, a fixed throwaway pair scoped to this ephemeral stack, the same
convention as the other case studies' well-known emulator dev keys.

## Running it locally

```bash
cd case-studies/fluentbit-kinesis

# Starts azurite + eventhubs-emulator + aws2azure, waits for the emulator's
# AMQP port and aws2azure's health endpoint (with a one-shot restart
# mitigation for a known emulator readiness flake, see the script), then
# starts fluent-bit last so its startup burst isn't dropped.
./scripts/wait_for_stack.sh

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

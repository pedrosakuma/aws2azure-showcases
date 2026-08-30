#!/usr/bin/env bash
# Brings the stack up in two stages so Fluent Bit only starts shipping its
# dummy records once the Event Hubs emulator is genuinely ready to accept
# AMQP connections and aws2azure is up.
#
# Why not just `docker compose up -d` + a depends_on healthcheck? The
# emulator's own internal MetadataStore readiness probe has a known,
# officially-acknowledged upstream bug where it can permanently get stuck
# reporting Unhealthy under load without ever recovering or exiting
# (Azure/azure-event-hubs-emulator-installer#70) -- when that happens, its
# AMQP listener never opens, so gating on Docker's HEALTHCHECK (which
# mirrors that internal probe) can hang forever. Polling the AMQP TCP port
# directly, with a one-shot container restart if it doesn't come up
# reasonably quickly, is the documented community mitigation for that bug.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

wait_for_tcp() {
  local host="$1" port="$2" timeout_s="$3"
  local waited=0
  until (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; do
    exec 3<&- 2>/dev/null || true
    if [ "$waited" -ge "$timeout_s" ]; then
      return 1
    fi
    sleep 3
    waited=$((waited + 3))
  done
  exec 3<&- 2>/dev/null || true
  return 0
}

wait_for_http_ok() {
  local url="$1" timeout_s="$2"
  local waited=0
  until curl --silent --fail --output /dev/null "$url"; do
    if [ "$waited" -ge "$timeout_s" ]; then
      return 1
    fi
    sleep 3
    waited=$((waited + 3))
  done
  return 0
}

echo "==> Starting azurite, eventhubs-emulator, aws2azure"
docker compose up --build -d azurite eventhubs-emulator aws2azure

echo "==> Waiting for the Event Hubs emulator's AMQP port (up to 75s)"
if ! wait_for_tcp 127.0.0.1 5672 75; then
  echo "==> AMQP port not up yet; applying known mitigation for" \
       "Azure/azure-event-hubs-emulator-installer#70 (restart once)"
  docker compose restart eventhubs-emulator
  wait_for_tcp 127.0.0.1 5672 90 \
    || { echo "==> Event Hubs emulator never became reachable"; docker compose logs eventhubs-emulator; exit 1; }
fi
echo "==> Event Hubs emulator AMQP port is open"

echo "==> Waiting for aws2azure health endpoint (up to 60s)"
wait_for_http_ok http://127.0.0.1:8080/_aws2azure/health 60 \
  || { echo "==> aws2azure never became healthy"; docker compose logs aws2azure; exit 1; }

# Matches the extra warm-up margin aws2azure's own Kinesis integration test
# fixture (KinesisEmulatorProxyFixture.cs) applies after the AMQP port opens
# -- the port accepts connections slightly before the emulator is actually
# ready to serve AMQP traffic.
echo "==> Warm-up delay"
sleep 15

echo "==> Starting fluent-bit"
docker compose up -d fluent-bit

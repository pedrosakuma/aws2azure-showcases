#!/usr/bin/env bash
# Renders config/aws2azure-config.json.template -> config/aws2azure-config.json
# by substituting AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET /
# AZURE_VAULT_URL from a local .env file (never committed -- see .gitignore).
#
# This exists so the real client secret only ever has to be typed by a
# human into .env; it's never something an assistant/CI job driving this
# repo needs to read, print, or handle in order to stand the stack up.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo "Missing .env -- copy .env.example to .env and fill in the real values first." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

for var in AZURE_TENANT_ID AZURE_CLIENT_ID AZURE_CLIENT_SECRET AZURE_VAULT_URL; do
  if [ -z "${!var:-}" ]; then
    echo "Missing required variable: $var (set it in .env)" >&2
    exit 1
  fi
done

envsubst '${AZURE_TENANT_ID} ${AZURE_CLIENT_ID} ${AZURE_CLIENT_SECRET} ${AZURE_VAULT_URL}' \
  < config/aws2azure-config.json.template > config/aws2azure-config.json

echo "Rendered config/aws2azure-config.json from .env + template."

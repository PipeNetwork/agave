#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  cluster-health.sh [--rpc-url URL] [--metrics-url URL] [--require-fair] [--require-solanacdn-connected]

Defaults:
  --rpc-url     http://127.0.0.1:8899
  --metrics-url disabled
EOF
}

rpc_url="http://127.0.0.1:8899"
metrics_url=""
require_fair=false
require_connected=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rpc-url)
      rpc_url="$2"
      shift 2
      ;;
    --metrics-url)
      metrics_url="$2"
      shift 2
      ;;
    --require-fair)
      require_fair=true
      shift
      ;;
    --require-solanacdn-connected)
      require_connected=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

health_body="$(curl -fsS "$rpc_url/health")"
[[ "$health_body" == "ok" ]] || {
  echo "rpc health check failed: $health_body" >&2
  exit 1
}

epoch_info="$(curl -fsS "$rpc_url" \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getEpochInfo"}')"
if ! grep -q '"result"' <<< "$epoch_info"; then
  echo "rpc getEpochInfo failed: $epoch_info" >&2
  exit 1
fi

fair_status="unknown"
connected_status="unknown"

if [[ -n "$metrics_url" ]]; then
  status_body="$(curl -fsS "$metrics_url/solanacdn/status")"
  if ! grep -q '"tx_fair_ordering"' <<< "$status_body"; then
    echo "solanacdn status endpoint did not return tx_fair_ordering" >&2
    exit 1
  fi

  fair_status="$(grep -o '"tx_fair_ordering":[[:space:]]*\(true\|false\)' <<< "$status_body" | head -n1 | cut -d: -f2 | tr -d ' ')"
  connected_status="$(grep -o '"connected":[[:space:]]*\(true\|false\)' <<< "$status_body" | head -n1 | cut -d: -f2 | tr -d ' ')"
elif [[ "$require_fair" == "true" || "$require_connected" == "true" ]]; then
  echo "metrics URL is required for fair-ordering or SolanaCDN connectivity checks" >&2
  exit 1
fi

if [[ "$require_fair" == "true" && "$fair_status" != "true" ]]; then
  echo "fair ordering is not enabled according to solanacdn status" >&2
  exit 1
fi

if [[ "$require_connected" == "true" && "$connected_status" != "true" ]]; then
  echo "solanacdn is not connected according to status endpoint" >&2
  exit 1
fi

printf 'rpc=%s health=ok fair=%s connected=%s\n' \
  "$rpc_url" \
  "$fair_status" \
  "$connected_status"

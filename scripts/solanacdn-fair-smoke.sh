#!/usr/bin/env bash
#
# Local SolanaCDN fair smoke test using the POP stub + scripts/run.sh.
#
# Environment overrides:
#   POP_ADDR=127.0.0.1:9002
#   METRICS_ADDR=127.0.0.1:9100
#   RPC_URL=http://127.0.0.1:8899
#   BATCHES=1            # 0 = continuous
#   TXS_PER_BATCH=1
#   INTERVAL_MS=200
#   EXIT_AFTER_MS=1000   # 0 = keep running
#   FAIR_SLASHING=0
#   FAIR_SLASHING_ENFORCE=0
#   TARGET_SLOT=         # if empty and slashing enabled, auto-fetch current slot
#   ECHO_COMMITS=        # unset = auto (enabled for slashing)
#
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "${script_dir}/.." && pwd)"

POP_ADDR="${POP_ADDR:-127.0.0.1:9002}"
METRICS_ADDR="${METRICS_ADDR:-127.0.0.1:9100}"
RPC_URL="${RPC_URL:-http://127.0.0.1:8899}"
BATCHES="${BATCHES:-1}"
TXS_PER_BATCH="${TXS_PER_BATCH:-1}"
INTERVAL_MS="${INTERVAL_MS:-200}"
EXIT_AFTER_MS="${EXIT_AFTER_MS:-1000}"
FAIR_SLASHING="${FAIR_SLASHING:-0}"
FAIR_SLASHING_ENFORCE="${FAIR_SLASHING_ENFORCE:-0}"
TARGET_SLOT="${TARGET_SLOT:-}"

if [[ -z "${ECHO_COMMITS+x}" ]]; then
  if [[ "${FAIR_SLASHING}" -eq 1 || "${FAIR_SLASHING_ENFORCE}" -eq 1 ]]; then
    ECHO_COMMITS=1
  else
    ECHO_COMMITS=0
  fi
fi

validator_args=(
  --fair
  --solanacdn-pop "${POP_ADDR}"
  --solanacdn-tls-insecure-skip-verify
  --solanacdn-metrics-addr "${METRICS_ADDR}"
)
if [[ "${FAIR_SLASHING_ENFORCE}" -eq 1 ]]; then
  validator_args+=(--fair-slashing-enforce)
elif [[ "${FAIR_SLASHING}" -eq 1 ]]; then
  validator_args+=(--fair-slashing)
fi

export SOLANA_RUN_SH_VALIDATOR_ARGS="${validator_args[*]}"

init_file="${root_dir}/config/run/init-completed"

run_pid=""
cleanup() {
  if [[ -n "${run_pid}" ]]; then
    kill -INT "${run_pid}" 2>/dev/null || true
    wait "${run_pid}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

echo "Starting local validator..."
"${root_dir}/scripts/run.sh" &
run_pid=$!

echo "Waiting for validator init (${init_file})..."
for _ in $(seq 1 60); do
  if [[ -f "${init_file}" ]]; then
    break
  fi
  sleep 1
done
if [[ ! -f "${init_file}" ]]; then
  echo "Timed out waiting for validator init file: ${init_file}" >&2
  exit 1
fi

if [[ -z "${TARGET_SLOT}" && ( "${FAIR_SLASHING}" -eq 1 || "${FAIR_SLASHING_ENFORCE}" -eq 1 ) ]]; then
  echo "Fetching current slot from ${RPC_URL}..."
  TARGET_SLOT="$(curl -s "${RPC_URL}" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}' \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("result",""))')"
fi

stub_args=(
  --listen "${POP_ADDR}"
  --batches "${BATCHES}"
  --txs-per-batch "${TXS_PER_BATCH}"
  --interval-ms "${INTERVAL_MS}"
  --exit-after-ms "${EXIT_AFTER_MS}"
)
if [[ -n "${TARGET_SLOT}" ]]; then
  stub_args+=(--target-slot "${TARGET_SLOT}")
fi
if [[ -n "${RPC_URL}" ]]; then
  stub_args+=(--rpc-url "${RPC_URL}")
fi
if [[ "${ECHO_COMMITS}" -eq 1 ]]; then
  stub_args+=(--echo-commits)
fi

echo "Running POP stub..."
cargo run -p solana-core --bin solanacdn-pop-stub -- "${stub_args[@]}"

metrics_url="http://${METRICS_ADDR}/metrics"
echo "Checking metrics at ${metrics_url}..."
curl -s "${metrics_url}" | grep -E 'solanacdn_tx_fair_batch_(received|injected)_total' || true

if [[ "${FAIR_SLASHING}" -eq 1 || "${FAIR_SLASHING_ENFORCE}" -eq 1 ]]; then
  curl -s "${metrics_url}" | grep -E 'solanacdn_fair_(commits_rx_total|ledger_commits_seen_total|ledger_audit_checked_total)' || true
fi

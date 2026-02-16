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
#   WAIT_FOR_METRICS_SECS=15
#   EXPECT_FAIR_RECEIVED_MIN=1
#   EXPECT_FAIR_INJECTED_MIN=1
#   EXPECT_FAIR_COMMITS_RX_MIN=1
#   EXPECT_FAIR_LEDGER_COMMITS_SEEN_MIN=0
#   EXPECT_FAIR_LEDGER_AUDIT_CHECKED_MIN=0
#   STRICT_SLASHING_METRICS=0
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
WAIT_FOR_METRICS_SECS="${WAIT_FOR_METRICS_SECS:-15}"
STRICT_SLASHING_METRICS="${STRICT_SLASHING_METRICS:-0}"

if [[ -z "${EXPECT_FAIR_RECEIVED_MIN+x}" ]]; then
  EXPECT_FAIR_RECEIVED_MIN=1
fi
if [[ -z "${EXPECT_FAIR_INJECTED_MIN+x}" ]]; then
  EXPECT_FAIR_INJECTED_MIN=1
fi
if [[ -z "${EXPECT_FAIR_COMMITS_RX_MIN+x}" ]]; then
  if [[ "${FAIR_SLASHING}" -eq 1 || "${FAIR_SLASHING_ENFORCE}" -eq 1 ]]; then
    EXPECT_FAIR_COMMITS_RX_MIN=1
  else
    EXPECT_FAIR_COMMITS_RX_MIN=0
  fi
fi
if [[ -z "${EXPECT_FAIR_LEDGER_COMMITS_SEEN_MIN+x}" ]]; then
  if [[ "${STRICT_SLASHING_METRICS}" -eq 1 && ( "${FAIR_SLASHING}" -eq 1 || "${FAIR_SLASHING_ENFORCE}" -eq 1 ) ]]; then
    EXPECT_FAIR_LEDGER_COMMITS_SEEN_MIN=1
  else
    EXPECT_FAIR_LEDGER_COMMITS_SEEN_MIN=0
  fi
fi
if [[ -z "${EXPECT_FAIR_LEDGER_AUDIT_CHECKED_MIN+x}" ]]; then
  if [[ "${STRICT_SLASHING_METRICS}" -eq 1 && ( "${FAIR_SLASHING}" -eq 1 || "${FAIR_SLASHING_ENFORCE}" -eq 1 ) ]]; then
    EXPECT_FAIR_LEDGER_AUDIT_CHECKED_MIN=1
  else
    EXPECT_FAIR_LEDGER_AUDIT_CHECKED_MIN=0
  fi
fi

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

metric_value_from() {
  local metrics="$1"
  local name="$2"
  local value
  value="$(awk -v n="${name}" '$1==n {print $2; exit}' <<<"${metrics}")"
  if [[ -z "${value}" ]]; then
    echo 0
  else
    echo "${value}"
  fi
}

assert_min() {
  local name="$1"
  local value="$2"
  local min="$3"
  if (( value < min )); then
    echo "FAIL: ${name}=${value} < ${min}"
    return 1
  fi
  return 0
}

deadline=$((SECONDS + WAIT_FOR_METRICS_SECS))
while true; do
  metrics="$(curl -s "${metrics_url}" || true)"
  fair_received="$(metric_value_from "${metrics}" "solanacdn_tx_fair_batch_received_total")"
  fair_injected="$(metric_value_from "${metrics}" "solanacdn_tx_fair_batch_injected_total")"
  fair_commits_rx="$(metric_value_from "${metrics}" "solanacdn_fair_commits_rx_total")"
  fair_commits_seen="$(metric_value_from "${metrics}" "solanacdn_fair_ledger_commits_seen_total")"
  fair_audit_checked="$(metric_value_from "${metrics}" "solanacdn_fair_ledger_audit_checked_total")"

  ok=1
  assert_min "solanacdn_tx_fair_batch_received_total" "${fair_received}" "${EXPECT_FAIR_RECEIVED_MIN}" || ok=0
  assert_min "solanacdn_tx_fair_batch_injected_total" "${fair_injected}" "${EXPECT_FAIR_INJECTED_MIN}" || ok=0

  if [[ "${FAIR_SLASHING}" -eq 1 || "${FAIR_SLASHING_ENFORCE}" -eq 1 ]]; then
    assert_min "solanacdn_fair_commits_rx_total" "${fair_commits_rx}" "${EXPECT_FAIR_COMMITS_RX_MIN}" || ok=0
    assert_min "solanacdn_fair_ledger_commits_seen_total" "${fair_commits_seen}" "${EXPECT_FAIR_LEDGER_COMMITS_SEEN_MIN}" || ok=0
    assert_min "solanacdn_fair_ledger_audit_checked_total" "${fair_audit_checked}" "${EXPECT_FAIR_LEDGER_AUDIT_CHECKED_MIN}" || ok=0
  fi

  if [[ "${ok}" -eq 1 ]]; then
    echo "Metrics thresholds satisfied."
    break
  fi

  if (( SECONDS >= deadline )); then
    echo "Metrics thresholds not met before timeout (${WAIT_FOR_METRICS_SECS}s)."
    echo "Last values:"
    echo "  solanacdn_tx_fair_batch_received_total=${fair_received}"
    echo "  solanacdn_tx_fair_batch_injected_total=${fair_injected}"
    echo "  solanacdn_fair_commits_rx_total=${fair_commits_rx}"
    echo "  solanacdn_fair_ledger_commits_seen_total=${fair_commits_seen}"
    echo "  solanacdn_fair_ledger_audit_checked_total=${fair_audit_checked}"
    exit 1
  fi

  sleep 1
done

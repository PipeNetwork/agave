#!/usr/bin/env bash
#
# Local multinode SolanaCDN `--fair` smoke + (optional) load test.
#
# Starts a small local cluster as separate OS processes (bootstrap validator + N auditors),
# runs a multi-POP stub that emits fair batches + POP witness receipts, broadcasts leader
# ACK/COMMIT/REJECT evidence to all connected validators, optionally runs bench-tps load, and
# asserts no fair vote withholding occurred.
#
# Environment overrides:
#   POP0=127.0.0.1:9002
#   POP1=127.0.0.1:9003
#   RPC_URL=http://127.0.0.1:8899
#   METRICS0=127.0.0.1:9100   # bootstrap SolanaCDN metrics
#   METRICS1=127.0.0.1:9101   # auditor1 SolanaCDN metrics
#   METRICS2=127.0.0.1:9102   # auditor2 SolanaCDN metrics
#   AUDITORS=1                # number of non-staked auditors (1..2)
#   BENCH_DURATION=30         # 0 disables bench-tps load
#   BATCHES=0                 # 0 = continuous fair batches
#   TXS_PER_BATCH=8
#   INTERVAL_MS=200
#   TARGET_SLOT_OFFSET=20
#   TOTAL_TIMEOUT_SECS=240
#
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "${script_dir}/.." && pwd)"

POP0="${POP0:-127.0.0.1:9002}"
POP1="${POP1:-127.0.0.1:9003}"
RPC_URL="${RPC_URL:-http://127.0.0.1:8899}"
METRICS0="${METRICS0:-127.0.0.1:9100}"
METRICS1="${METRICS1:-127.0.0.1:9101}"
METRICS2="${METRICS2:-127.0.0.1:9102}"
AUDITORS="${AUDITORS:-1}"
BENCH_DURATION="${BENCH_DURATION:-30}"
BATCHES="${BATCHES:-0}"
TXS_PER_BATCH="${TXS_PER_BATCH:-8}"
INTERVAL_MS="${INTERVAL_MS:-200}"
TARGET_SLOT_OFFSET="${TARGET_SLOT_OFFSET:-20}"
TOTAL_TIMEOUT_SECS="${TOTAL_TIMEOUT_SECS:-240}"

if [[ -z "${LIBCLANG_PATH:-}" && -d /opt/homebrew/opt/llvm/lib ]]; then
  export LIBCLANG_PATH="/opt/homebrew/opt/llvm/lib"
  export DYLD_LIBRARY_PATH="/opt/homebrew/opt/llvm/lib${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}"
  export PATH="/opt/homebrew/opt/llvm/bin:${PATH}"
fi

stub_bin="${root_dir}/target/debug/solanacdn-pop-stub"

faucet_pid=""
bootstrap_pid=""
auditor1_pid=""
auditor2_pid=""
stub_pid=""
watchdog_pid=""
main_pid="$$"

cleanup() {
  set +e
  for pid in "${watchdog_pid}" "${stub_pid}" "${auditor2_pid}" "${auditor1_pid}" "${bootstrap_pid}" "${faucet_pid}"; do
    [[ -n "${pid}" ]] || continue
    kill -INT "${pid}" 2>/dev/null || true
  done
  sleep 1
  for pid in "${stub_pid}" "${auditor2_pid}" "${auditor1_pid}" "${bootstrap_pid}" "${faucet_pid}"; do
    [[ -n "${pid}" ]] || continue
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

if [[ "${TOTAL_TIMEOUT_SECS}" -gt 0 ]]; then
  (
    sleep "${TOTAL_TIMEOUT_SECS}"
    echo "Overall timeout (${TOTAL_TIMEOUT_SECS}s) reached; shutting down." >&2
    kill -TERM "${main_pid}" 2>/dev/null || true
  ) &
  watchdog_pid=$!
fi

echo "Building POP stub (if needed)..."
if [[ ! -x "${stub_bin}" ]]; then
  cargo build -p solana-core --bin solanacdn-pop-stub
fi

echo "Setting up local multinode cluster..."
"${root_dir}/multinode-demo/setup.sh" >"${root_dir}/config/solanacdn-fair-multinode-setup.log" 2>&1

echo "Starting faucet..."
"${root_dir}/multinode-demo/faucet.sh" --bind-address 127.0.0.1 --port 9900 >"${root_dir}/config/solanacdn-fair-multinode-faucet.log" 2>&1 &
faucet_pid=$!

echo "Starting bootstrap validator (leader)..."
"${root_dir}/multinode-demo/bootstrap-validator.sh" --no-restart --log - \
  --fair \
  --solanacdn-pop "${POP0}" \
  --solanacdn-pop "${POP1}" \
  --solanacdn-tls-insecure-skip-verify \
  --solanacdn-metrics-addr "${METRICS0}" \
  >"${root_dir}/config/solanacdn-fair-multinode-bootstrap.log" 2>&1 &
bootstrap_pid=$!

if [[ "${AUDITORS}" -ge 1 ]]; then
  echo "Starting auditor1..."
  "${root_dir}/multinode-demo/validator-x.sh" --no-restart --log - \
    --rpc-port 8891 \
    --gossip-port 8002 \
    --fair \
    --solanacdn-pop "${POP0}" \
    --solanacdn-pop "${POP1}" \
    --solanacdn-tls-insecure-skip-verify \
    --solanacdn-metrics-addr "${METRICS1}" \
    >"${root_dir}/config/solanacdn-fair-multinode-auditor1.log" 2>&1 &
  auditor1_pid=$!
fi

if [[ "${AUDITORS}" -ge 2 ]]; then
  echo "Starting auditor2..."
  "${root_dir}/multinode-demo/validator-x.sh" --no-restart --log - \
    --rpc-port 8892 \
    --gossip-port 8003 \
    --fair \
    --solanacdn-pop "${POP0}" \
    --solanacdn-pop "${POP1}" \
    --solanacdn-tls-insecure-skip-verify \
    --solanacdn-metrics-addr "${METRICS2}" \
    >"${root_dir}/config/solanacdn-fair-multinode-auditor2.log" 2>&1 &
  auditor2_pid=$!
fi

echo "Waiting for RPC (${RPC_URL})..."
deadline=$((SECONDS + 90))
while true; do
  if curl -s "${RPC_URL}" -H 'Content-Type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}' >/dev/null 2>&1; then
    break
  fi
  if (( SECONDS >= deadline )); then
    echo "Timed out waiting for RPC readiness." >&2
    exit 1
  fi
  sleep 1
done

echo "Starting multi-POP fair stub..."
"${stub_bin}" \
  --listen "${POP0}" \
  --listen "${POP1}" \
  --rpc-url "${RPC_URL}" \
  --batches "${BATCHES}" \
  --txs-per-batch "${TXS_PER_BATCH}" \
  --interval-ms "${INTERVAL_MS}" \
  --exit-after-ms 0 \
  --target-slot-offset "${TARGET_SLOT_OFFSET}" \
  --broadcast-evidence \
  --emit-witnesses \
  >"${root_dir}/config/solanacdn-fair-multinode-pop-stub.log" 2>&1 &
stub_pid=$!

if [[ "${BENCH_DURATION}" -gt 0 ]]; then
  echo "Running bench-tps for ${BENCH_DURATION}s (background load)..."
  "${root_dir}/multinode-demo/bench-tps.sh" --duration "${BENCH_DURATION}" \
    >"${root_dir}/config/solanacdn-fair-multinode-bench-tps.log" 2>&1 || true
fi

echo "Waiting for fair evidence/audits to settle..."
sleep 10

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

check_metrics() {
  local addr="$1"
  local url="http://${addr}/metrics"
  local metrics
  metrics="$(curl -s "${url}" || true)"
  local withheld failed equiv
  withheld="$(metric_value_from "${metrics}" "solanacdn_fair_votes_withheld_total")"
  failed="$(metric_value_from "${metrics}" "solanacdn_fair_ledger_audit_failed_total")"
  equiv="$(metric_value_from "${metrics}" "solanacdn_fair_equivocations_total")"

  echo "Metrics ${addr}: votes_withheld=${withheld} audit_failed=${failed} equivocations=${equiv}"
  if (( withheld > 0 )) || (( failed > 0 )) || (( equiv > 0 )); then
    return 1
  fi
  return 0
}

ok=1
check_metrics "${METRICS0}" || ok=0
if [[ "${AUDITORS}" -ge 1 ]]; then
  check_metrics "${METRICS1}" || ok=0
fi
if [[ "${AUDITORS}" -ge 2 ]]; then
  check_metrics "${METRICS2}" || ok=0
fi

if [[ "${ok}" -ne 1 ]]; then
  echo "FAIL: detected fair vote withholding or audit failures. Logs:"
  echo "  ${root_dir}/config/solanacdn-fair-multinode-bootstrap.log"
  echo "  ${root_dir}/config/solanacdn-fair-multinode-auditor1.log"
  echo "  ${root_dir}/config/solanacdn-fair-multinode-auditor2.log"
  echo "  ${root_dir}/config/solanacdn-fair-multinode-pop-stub.log"
  exit 1
fi

echo "PASS: no fair vote withholding detected."

#!/usr/bin/env bash
#
# Local multinode negative test for SolanaCDN `--fair` enforcement.
#
# Starts a small local cluster (bootstrap leader + 1 validator), runs a multi-POP stub that emits
# fair batches + witnesses, enables a DEV fault on the leader (ACK without commit), and asserts
# the auditor detects the violation and withholds votes.
#
# Environment overrides:
#   POP0=127.0.0.1:9002
#   POP1=127.0.0.1:9003
#   POP2=127.0.0.1:9004
#   RPC_URL=http://127.0.0.1:8899
#   METRICS0=127.0.0.1:9100   # bootstrap SolanaCDN metrics
#   METRICS1=127.0.0.1:9101   # auditor1 SolanaCDN metrics
#   BATCHES=0                 # 0 = continuous fair batches
#   TXS_PER_BATCH=8
#   INTERVAL_MS=200
#   TARGET_SLOT_OFFSET=20
#   TOTAL_TIMEOUT_SECS=240
#   WAIT_FOR_METRICS_SECS=120
#   EXPECT_EQUIVOCATIONS_MIN=1
#   EXPECT_VOTES_WITHHELD_MIN=1
#   EXPECT_WITNESS_QUORUM_MET=1
#
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "${script_dir}/.." && pwd)"

POP0="${POP0:-127.0.0.1:9002}"
POP1="${POP1:-127.0.0.1:9003}"
POP2="${POP2:-127.0.0.1:9004}"
RPC_URL="${RPC_URL:-http://127.0.0.1:8899}"
METRICS0="${METRICS0:-127.0.0.1:9100}"
METRICS1="${METRICS1:-127.0.0.1:9101}"
BATCHES="${BATCHES:-0}"
TXS_PER_BATCH="${TXS_PER_BATCH:-8}"
INTERVAL_MS="${INTERVAL_MS:-200}"
TARGET_SLOT_OFFSET="${TARGET_SLOT_OFFSET:-20}"
TOTAL_TIMEOUT_SECS="${TOTAL_TIMEOUT_SECS:-240}"
WAIT_FOR_METRICS_SECS="${WAIT_FOR_METRICS_SECS:-120}"

EXPECT_EQUIVOCATIONS_MIN="${EXPECT_EQUIVOCATIONS_MIN:-1}"
EXPECT_VOTES_WITHHELD_MIN="${EXPECT_VOTES_WITHHELD_MIN:-1}"
EXPECT_WITNESS_QUORUM_MET="${EXPECT_WITNESS_QUORUM_MET:-1}"

if [[ -d /opt/homebrew/opt/llvm/lib ]]; then
  export LIBCLANG_PATH="/opt/homebrew/opt/llvm/lib"
  export DYLD_LIBRARY_PATH="/opt/homebrew/opt/llvm/lib${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}"
  export PATH="/opt/homebrew/opt/llvm/bin:${PATH}"
fi

profile="${CARGO_BUILD_PROFILE:-debug}"
export PATH="${root_dir}/target/${profile}:${PATH}"
export USE_INSTALL=1

stub_bin="${root_dir}/target/${profile}/solanacdn-pop-stub"

faucet_pid=""
bootstrap_pid=""
auditor1_pid=""
stub_pid=""
watchdog_pid=""
main_pid="$$"

cleanup() {
  set +e
  for pid in "${watchdog_pid}" "${stub_pid}" "${auditor1_pid}" "${bootstrap_pid}" "${faucet_pid}"; do
    [[ -n "${pid}" ]] || continue
    kill -INT "${pid}" 2>/dev/null || true
  done
  sleep 1
  for pid in "${stub_pid}" "${auditor1_pid}" "${bootstrap_pid}" "${faucet_pid}"; do
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

echo "Building required binaries..."
cargo build -p solana-core --bin solanacdn-pop-stub
cargo build --bin solana-keygen
cargo build --bin solana-genesis
cargo build --bin solana-faucet
cargo build --bin solana-gossip
cargo build --bin solana
cargo build --bin agave-validator

echo "Setting up local multinode cluster..."
"${root_dir}/multinode-demo/setup.sh" >"${root_dir}/config/solanacdn-fair-malicious-setup.log" 2>&1

bootstrap_ledger="${root_dir}/config/bootstrap-validator"
auditor1_ledger="${root_dir}/config/solanacdn-fair-malicious-auditor1-ledger"

seed_ledger_from_bootstrap() {
  local dest="$1"
  rm -rf "${dest}"
  mkdir -p "${dest}"
  cp -p "${bootstrap_ledger}/genesis.bin" "${dest}/"
  cp -R "${bootstrap_ledger}/rocksdb" "${dest}/"
  rm -f "${dest}/identity.json" "${dest}/vote-account.json" "${dest}/stake-account.json"
}

echo "Seeding auditor1 ledger from bootstrap genesis..."
seed_ledger_from_bootstrap "${auditor1_ledger}"

echo "Starting faucet..."
"${root_dir}/multinode-demo/faucet.sh" >"${root_dir}/config/solanacdn-fair-malicious-faucet.log" 2>&1 &
faucet_pid=$!

echo "Starting bootstrap validator (malicious leader)..."
"${root_dir}/multinode-demo/bootstrap-validator.sh" --no-restart --log - \
  --fair \
  --fair-dev-fault ack-no-commit \
  --solanacdn-pop "${POP0}" \
  --solanacdn-pop "${POP1}" \
  --solanacdn-pop "${POP2}" \
  --solanacdn-tls-insecure-skip-verify \
  --solanacdn-metrics-addr "${METRICS0}" \
  >"${root_dir}/config/solanacdn-fair-malicious-bootstrap.log" 2>&1 &
bootstrap_pid=$!

echo "Starting auditor1 (voting enabled; should withhold votes)..."
"${root_dir}/multinode-demo/validator-x.sh" --no-restart --log - \
  --rpc-port 8891 \
  --gossip-port 8002 \
  --no-snapshot-fetch \
  --ledger "${auditor1_ledger}" \
  --skip-require-tower \
  --fair \
  --solanacdn-pop "${POP0}" \
  --solanacdn-pop "${POP1}" \
  --solanacdn-pop "${POP2}" \
  --solanacdn-tls-insecure-skip-verify \
  --solanacdn-metrics-addr "${METRICS1}" \
  >"${root_dir}/config/solanacdn-fair-malicious-auditor1.log" 2>&1 &
auditor1_pid=$!

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
  --listen "${POP2}" \
  --rpc-url "${RPC_URL}" \
  --batches "${BATCHES}" \
  --txs-per-batch "${TXS_PER_BATCH}" \
  --interval-ms "${INTERVAL_MS}" \
  --exit-after-ms 0 \
  --target-slot-offset "${TARGET_SLOT_OFFSET}" \
  --broadcast-evidence \
  --emit-witnesses \
  >"${root_dir}/config/solanacdn-fair-malicious-pop-stub.log" 2>&1 &
stub_pid=$!

echo "Waiting for slashing evidence..."
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

has_witness_quorum_met() {
  local addr="$1"
  curl -sf "http://${addr}/solanacdn/fair-evidence" | python3 -c '
import json,sys
expect=int(sys.argv[1])
try:
  data=json.load(sys.stdin)
except Exception:
  sys.exit(1)
recent=data.get("recent_witnesses") or []
met=any(bool(w.get("witness_quorum_met")) for w in recent)
sys.exit(0 if (not expect) or met else 1)
' "${EXPECT_WITNESS_QUORUM_MET}"
}

check_metrics() {
  local addr="$1"
  local role="$2"
  local url="http://${addr}/metrics"
  local metrics=""
  if ! metrics="$(curl -sf "${url}")"; then
    echo "FAIL: could not fetch metrics from ${url}" >&2
    return 1
  fi

  local withheld equiv failed checked
  withheld="$(metric_value_from "${metrics}" "solanacdn_fair_votes_withheld_total")"
  equiv="$(metric_value_from "${metrics}" "solanacdn_fair_equivocations_total")"
  failed="$(metric_value_from "${metrics}" "solanacdn_fair_ledger_audit_failed_total")"
  checked="$(metric_value_from "${metrics}" "solanacdn_fair_ledger_audit_checked_total")"

  echo "Metrics ${addr} (${role}): audit_checked=${checked} audit_failed=${failed} equivocations=${equiv} votes_withheld=${withheld}"

  local ok=1
  assert_min "solanacdn_fair_equivocations_total" "${equiv}" "${EXPECT_EQUIVOCATIONS_MIN}" || ok=0
  assert_min "solanacdn_fair_votes_withheld_total" "${withheld}" "${EXPECT_VOTES_WITHHELD_MIN}" || ok=0
  if [[ "${EXPECT_WITNESS_QUORUM_MET}" -eq 1 ]]; then
    has_witness_quorum_met "${addr}" >/dev/null || ok=0
  fi

  if [[ "${ok}" -ne 1 ]]; then
    return 1
  fi
  return 0
}

echo "Checking metrics (timeout ${WAIT_FOR_METRICS_SECS}s)..."
deadline=$((SECONDS + WAIT_FOR_METRICS_SECS))
while true; do
  if check_metrics "${METRICS1}" "auditor1"; then
    break
  fi
  if (( SECONDS >= deadline )); then
    echo "FAIL: slashing thresholds not satisfied before timeout. Logs:"
    echo "  ${root_dir}/config/solanacdn-fair-malicious-setup.log"
    echo "  ${root_dir}/config/solanacdn-fair-malicious-faucet.log"
    echo "  ${root_dir}/config/solanacdn-fair-malicious-bootstrap.log"
    echo "  ${root_dir}/config/solanacdn-fair-malicious-auditor1.log"
    echo "  ${root_dir}/config/solanacdn-fair-malicious-pop-stub.log"
    exit 1
  fi
  sleep 1
done

echo "PASS: malicious leader detected and votes withheld."


#!/usr/bin/env bash
#
# Spin up a small local multinode cluster with MCP enabled.
#
# Example:
#   multinode-demo/mcp-demo.sh
#   multinode-demo/bench-tps.sh
#
set -euo pipefail

demo_dir=$(dirname "$0")
# shellcheck source=multinode-demo/common.sh
source "$demo_dir"/common.sh

num_validators=3
num_pops=1
lanes=2
mcp_da_threshold_bps=1
mcp_microblock_max_refs=64
reset=false
activate_vote_withholding_feature=false
use_pop_sim=true
use_full_pop=false
duration_secs=0
bench_tps_secs=0
bench_tps_tx_count=5000
pop_sim_listen="127.0.0.1:10020"
bind_address="127.0.0.1"
solanacdn_repo_default="$demo_dir/../../solanaCdn"
run_tests=false
config_dir=""
teardown_mode="off" # off | on-success | always
teardown_allow_any_config_dir=false
keep_config=false
test_timeout_secs=90
tx_test_workers=4
tx_test_count=200
solanacdn_metrics_base_port=18990
enable_solanacdn_metrics=false
local_snapshot_service_enabled=false
snapshot_service_port=""
snapshot_manifest_url=""
bootstrap_snapshot_interval_slots=200

# Keep MCP vote-withholding enforcement feature inactive by default for local testing.
# This matches mainnet behavior (feature must be explicitly activated on-chain).
mcp_vote_withholding_feature="Dnscgakm5WULSoZq8hbBHcnfd3xTwZ4VDmGtmn1DijTR"

usage() {
  if [[ -n ${1-} ]]; then
    echo "$1"
    echo
  fi
  cat <<EOF

usage: $0 [OPTIONS] [extra validator args...]

OPTIONS:
  --num-validators N          Total validators including bootstrap (default: $num_validators)
  --num-pops N                Local SolanaCDN POP count (requires --full-pop; default: $num_pops)
  --lanes K                   MCP lanes per slot (default: $lanes)
  --mcp-da-threshold-bps BPS  MCP DA threshold (default: $mcp_da_threshold_bps)
  --mcp-microblock-max-refs N MCP microblock max refs (default: $mcp_microblock_max_refs)
  --no-pop-sim                Do not start the local SolanaCDN POP simulator
  --full-pop                  Start local SolanaCDN POP(s) (solanacdn-pop) from ../solanaCdn
  --pop-sim-listen HOST:PORT  POP listen addr base (default: $pop_sim_listen). With --num-pops>1, ports increment from base.
  --bind-address HOST         Bind validators to this address (default: $bind_address)
  --duration-secs N           Exit after N seconds (default: wait forever). Useful for smoke tests.
  --bench-tps-secs N          Run bench-tps for N seconds once the cluster is healthy (default: $bench_tps_secs)
  --bench-tps-tx-count N      Total bench-tps tx-count when --bench-tps-secs is set (default: $bench_tps_tx_count)
  --run-tests                Start cluster, run MCP+fair self-tests, then exit with non-zero on failure.
                             Defaults: --reset, temp --config-dir under /tmp, SOLANA_SKIP_PROGRAM_FETCH=1 (if unset),
                             auto-enables block production forwarding (via --staked-nodes-overrides), and tears down
                             the temp config dir on success.
                             Note: requires --num-validators >= 2 to exercise the MCP forwarding path.
  --config-dir DIR           Override SOLANA_CONFIG_DIR for this run (recommended for test isolation).
  --teardown                 Remove --config-dir after the run (or always remove the temp dir created by --run-tests).
  --keep-config              Keep --config-dir on success (default behavior on failure).
  --test-timeout-secs N       Timeout waiting for MCP memo kinds in --run-tests mode (default: $test_timeout_secs)
  --tx-test-workers N         Parallelism for --run-tests tx burst (default: $tx_test_workers)
  --tx-test-count N           Total txs to send in --run-tests tx burst (default: $tx_test_count)
  --reset                     Regenerate genesis/ledger before starting (recommended if you
                              previously ran with a different genesis config)
  --activate-vote-withholding-feature
                              Activate the on-chain MCP vote-withholding feature after the
                              cluster becomes healthy. To actually withhold votes, also pass
                              --mcp-enforce via extra validator args.

Any remaining args are forwarded to all validators (bootstrap + non-bootstrap), e.g.:
  $0 --solanacdn-api-token pk_test_dummy   # --mcp implies --fair and MCP DA over SolanaCDN when configured

EOF
  exit 1
}

extra_validator_args=()
while [[ -n ${1-} ]]; do
  case "$1" in
    --num-validators)
      num_validators="${2:?missing value for --num-validators}"
      shift 2
      ;;
    --num-pops)
      num_pops="${2:?missing value for --num-pops}"
      shift 2
      ;;
    --lanes)
      lanes="${2:?missing value for --lanes}"
      shift 2
      ;;
    --mcp-da-threshold-bps)
      mcp_da_threshold_bps="${2:?missing value for --mcp-da-threshold-bps}"
      shift 2
      ;;
    --mcp-microblock-max-refs)
      mcp_microblock_max_refs="${2:?missing value for --mcp-microblock-max-refs}"
      shift 2
      ;;
    --no-pop-sim)
      use_pop_sim=false
      shift
      ;;
    --full-pop)
      use_full_pop=true
      use_pop_sim=false
      shift
      ;;
    --pop-sim-listen)
      pop_sim_listen="${2:?missing value for --pop-sim-listen}"
      shift 2
      ;;
    --bind-address)
      bind_address="${2:?missing value for --bind-address}"
      shift 2
      ;;
    --duration-secs)
      duration_secs="${2:?missing value for --duration-secs}"
      shift 2
      ;;
    --bench-tps-secs)
      bench_tps_secs="${2:?missing value for --bench-tps-secs}"
      shift 2
      ;;
    --bench-tps-tx-count)
      bench_tps_tx_count="${2:?missing value for --bench-tps-tx-count}"
      shift 2
      ;;
    --run-tests)
      run_tests=true
      shift
      ;;
    --config-dir)
      config_dir="${2:?missing value for --config-dir}"
      shift 2
      ;;
    --teardown)
      teardown_mode="always"
      shift
      ;;
    --keep-config)
      keep_config=true
      teardown_mode="off"
      shift
      ;;
    --test-timeout-secs)
      test_timeout_secs="${2:?missing value for --test-timeout-secs}"
      shift 2
      ;;
    --tx-test-workers)
      tx_test_workers="${2:?missing value for --tx-test-workers}"
      shift 2
      ;;
    --tx-test-count)
      tx_test_count="${2:?missing value for --tx-test-count}"
      shift 2
      ;;
    --reset)
      reset=true
      shift
      ;;
    --activate-vote-withholding-feature)
      activate_vote_withholding_feature=true
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      extra_validator_args+=("$1")
      shift
      ;;
  esac
done

config_dir_created=false
if [[ $run_tests == true ]]; then
  reset=true
  enable_solanacdn_metrics=true
  if [[ -z ${SOLANA_SKIP_PROGRAM_FETCH-} ]]; then
    export SOLANA_SKIP_PROGRAM_FETCH=1
    echo "SOLANA_SKIP_PROGRAM_FETCH not set; defaulting to SOLANA_SKIP_PROGRAM_FETCH=1 for --run-tests"
  fi
  if [[ -z $config_dir ]]; then
    tmp_base="${TMPDIR:-/tmp}"
    if [[ -d /tmp && -w /tmp ]]; then
      tmp_base="/tmp"
    fi
    config_dir="$(mktemp -d "${tmp_base%/}/agave-mcp-demo.XXXXXX")"
    config_dir_created=true
    if [[ $teardown_mode == "off" && $keep_config == false ]]; then
      teardown_mode="on-success"
    fi
  fi
fi

if [[ $teardown_mode == "always" && -n $config_dir ]]; then
  # User provided a config dir and explicitly asked us to delete it at the end.
  teardown_allow_any_config_dir=true
fi

if [[ -n $config_dir ]]; then
  mkdir -p "$config_dir"
  export SOLANA_CONFIG_DIR="$config_dir"
  echo "Using SOLANA_CONFIG_DIR=$SOLANA_CONFIG_DIR"
fi

# Ensure RPC history is enabled in --run-tests mode so we can inspect MCP memo transactions.
has_flag() {
  local needle=$1
  shift
  local arg
  for arg in "$@"; do
    if [[ $arg == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}
append_flag_if_missing() {
  local flag=$1
  if ! has_flag "$flag" "${extra_validator_args[@]}"; then
    extra_validator_args+=("$flag")
  fi
}
append_kv_if_missing() {
  local flag=$1
  local value=$2
  if ! has_flag "$flag" "${extra_validator_args[@]}"; then
    extra_validator_args+=("$flag" "$value")
  fi
}
if [[ $run_tests == true ]]; then
  append_flag_if_missing --enable-rpc-transaction-history
  append_flag_if_missing --enable-extended-tx-metadata-storage
  append_flag_if_missing --tpu-enable-udp
fi
if [[ $num_validators -gt 1 ]]; then
  # Avoid unintentionally downloading massive remote snapshots when joining a local cluster.
  # This demo script pre-populates each validator ledger with the genesis blockstore, so bootstrap
  # snapshot fetch is unnecessary.
  append_flag_if_missing --no-genesis-fetch
  append_flag_if_missing --no-snapshot-fetch
fi

# If the user explicitly provided SolanaCDN configuration flags, do not start the local POP sim.
for arg in "${extra_validator_args[@]}"; do
  case "$arg" in
    --solanacdn-pop|--solanacdn-control|--solanacdn-api-token)
      use_pop_sim=false
      use_full_pop=false
      if [[ $num_pops -ne 1 ]]; then
        echo "Error: --num-pops is only supported with --full-pop (local POPs)."
        echo "Remove --num-pops or remove explicit SolanaCDN flags and let this script start POPs."
        exit 1
      fi
      ;;
  esac
done

if [[ $num_validators -lt 1 ]]; then
  usage "Error: --num-validators must be >= 1"
fi

if [[ $run_tests == true && $num_validators -lt 2 ]]; then
  usage "Error: --run-tests requires --num-validators >= 2 (MCP memos are produced by the forwarding stage on non-leader nodes)"
fi

if [[ $num_pops -lt 1 ]]; then
  usage "Error: --num-pops must be >= 1"
fi

if [[ $use_pop_sim == true && $num_pops -ne 1 ]]; then
  usage "Error: --num-pops requires --full-pop (POP simulator does not support multi-POP mesh)"
fi

if [[ $use_full_pop == false && $num_pops -ne 1 ]]; then
  usage "Error: --num-pops requires --full-pop"
fi

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Error: missing required command: $1"
    exit 1
  }
}
require_cmd python3
require_cmd curl

echo "--- Preflight: bind sockets ($bind_address)"
if ! python3 - <<PY
import socket

addr="$bind_address"

def check(socktype):
  try:
    infos = socket.getaddrinfo(addr, 0, type=socktype)
  except Exception as e:
    raise SystemExit(f"getaddrinfo({addr!r}) failed: {e}")
  last = None
  for family, st, proto, _canon, sockaddr in infos:
    s = socket.socket(family, st, proto)
    try:
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      s.bind(sockaddr)
      if st == socket.SOCK_STREAM:
        s.listen(1)
      return
    except OSError as e:
      last = e
    finally:
      try:
        s.close()
      except Exception:
        pass
  raise SystemExit(f"cannot bind {'tcp' if socktype==socket.SOCK_STREAM else 'udp'} on {addr!r}: {last}")

check(socket.SOCK_STREAM)
check(socket.SOCK_DGRAM)
print("ok")
PY
then
  echo "Preflight failed: unable to bind TCP/UDP sockets on $bind_address."
  echo "Running a local cluster requires binding many ports; if you're in a sandboxed environment, run on your host machine."
  exit 1
fi

if [[ $num_validators -gt 1 ]]; then
  # By default the validator bootstrapping flow consults a Pipe snapshot manifest URL to locate
  # snapshots. For local clusters this can unintentionally trigger very large remote snapshot
  # downloads. If the user didn't explicitly configure a manifest URL, prefer a local snapshot
  # manifest served from this machine.
  if ! has_flag --snapshot-manifest-url "${extra_validator_args[@]}" && ! has_flag --no-snapshot-fetch "${extra_validator_args[@]}"; then
    local_snapshot_service_enabled=true
    snapshot_service_port="$(python3 - <<'PY'
import socket
s=socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
    snapshot_manifest_url="http://127.0.0.1:${snapshot_service_port}/snapshot-manifest.json"
    append_kv_if_missing --snapshot-manifest-url "$snapshot_manifest_url"
    bootstrap_snapshot_interval_slots=10
    echo "Using local snapshot manifest URL: $snapshot_manifest_url"
	fi
fi

genesis_extra_args=()
if [[ $num_validators -gt 1 ]]; then
  # In scheduled MCP mode, lane leaders are selected from the leader schedule, which is derived
  # from stake. Ensure all validators are present (with stake) in genesis so scheduled MCP memos
  # can be produced in small local clusters.
  keygen_bin="$SOLANA_ROOT/target/debug/solana-keygen"
  keygen_cmd() {
    if [[ -x "$keygen_bin" ]]; then
      "$keygen_bin" "$@"
    else
      # shellcheck disable=SC2086 # $solana_keygen is a command string with args
      $solana_keygen "$@"
    fi
  }
  for i in $(seq 1 $((num_validators - 1))); do
    vdir="$SOLANA_CONFIG_DIR/validator--mcp-$i"
    mkdir -p "$vdir"
    [[ -r "$vdir/identity.json" ]] || keygen_cmd new --no-passphrase -so "$vdir/identity.json" >/dev/null
    [[ -r "$vdir/vote-account.json" ]] || keygen_cmd new --no-passphrase -so "$vdir/vote-account.json" >/dev/null
    [[ -r "$vdir/stake-account.json" ]] || keygen_cmd new --no-passphrase -so "$vdir/stake-account.json" >/dev/null
	    genesis_extra_args+=(--bootstrap-validator "$vdir/identity.json" "$vdir/vote-account.json" "$vdir/stake-account.json")
	  done
	fi

setup_extra_args=()
if [[ $run_tests == true ]]; then
  # Warmup epochs are convenient for devnet-style workflows, but in tiny local clusters they
  # advance epochs quickly. If the cluster doesn't root votes fast enough, validators can spam
  # "No next leader found" once they enter an unconfirmed epoch, which breaks the MCP forwarding
  # path tests. Disable warmup epochs for deterministic local MCP self-tests.
  setup_extra_args+=(--no-warmup-epochs)
fi

	if [[ $reset == true ]]; then
	  "$demo_dir"/setup.sh "${setup_extra_args[@]}" --deactivate-feature "$mcp_vote_withholding_feature" "${genesis_extra_args[@]}"
	elif [[ ! -f "$SOLANA_CONFIG_DIR/bootstrap-validator/genesis.bin" && ! -f "$SOLANA_CONFIG_DIR/bootstrap-validator/genesis.tar.bz2" ]]; then
	  "$demo_dir"/setup.sh "${setup_extra_args[@]}" --deactivate-feature "$mcp_vote_withholding_feature" "${genesis_extra_args[@]}"
	fi

	if [[ $num_validators -gt 1 ]]; then
	  # Validators started with `--no-genesis-fetch --no-snapshot-fetch` still need a local genesis
	  # blockstore to replay bank0. Seed each validator ledger dir with the genesis ledger created by
	  # `setup.sh` (without overwriting identity/vote keypairs).
	  genesis_ledger_src="$SOLANA_CONFIG_DIR/bootstrap-validator"
  if [[ -d "$genesis_ledger_src/rocksdb" ]]; then
    for i in $(seq 1 $((num_validators - 1))); do
      vdir="$SOLANA_CONFIG_DIR/validator--mcp-$i"
      mkdir -p "$vdir"
      for f in genesis.bin genesis.tar.bz2; do
        if [[ -f "$genesis_ledger_src/$f" && ! -f "$vdir/$f" ]]; then
          cp -f "$genesis_ledger_src/$f" "$vdir/$f"
        fi
      done
      for d in rocksdb accounts snapshots; do
        if [[ -d "$genesis_ledger_src/$d" && ! -d "$vdir/$d" ]]; then
          cp -R "$genesis_ledger_src/$d" "$vdir/$d"
        fi
      done
	    done
	  fi
	fi

if [[ $run_tests == true ]]; then
  # Enable non-vote forwarding (forwarding stage) so scheduled MCP memos can be produced.
  # agave-validator only enables block production forwarding when --staked-nodes-overrides is set.
  if ! has_flag --staked-nodes-overrides "${extra_validator_args[@]}"; then
    echo "--- Test setup: enabling block production forwarding (--staked-nodes-overrides)"
    staked_nodes_overrides_file="$SOLANA_CONFIG_DIR/staked-nodes-overrides.yaml"

    pks=()
    # Bootstrap identity is created by setup.sh
    if [[ -r "$SOLANA_CONFIG_DIR/bootstrap-validator/identity.json" ]]; then
      pks+=("$(keygen_cmd pubkey "$SOLANA_CONFIG_DIR/bootstrap-validator/identity.json")")
    fi
    # Extra validators (created above)
    if [[ $num_validators -gt 1 ]]; then
      for i in $(seq 1 $((num_validators - 1))); do
        vdir="$SOLANA_CONFIG_DIR/validator--mcp-$i"
        if [[ -r "$vdir/identity.json" ]]; then
          pks+=("$(keygen_cmd pubkey "$vdir/identity.json")")
        fi
      done
    fi

    if [[ ${#pks[@]} -eq 0 ]]; then
      echo "staked_map_id: {}" >"$staked_nodes_overrides_file"
    else
      echo "staked_map_id:" >"$staked_nodes_overrides_file"
      for pk in "${pks[@]}"; do
        echo "  $pk: 1000000000" >>"$staked_nodes_overrides_file"
      done
    fi
    append_kv_if_missing --staked-nodes-overrides "$staked_nodes_overrides_file"
  fi
fi

pids=()
cleanup_pids() {
  set +euo pipefail
  for pid in "${pids[@]}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  for pid in "${pids[@]}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
}
maybe_teardown() {
  local exit_code=$1
  local should_delete=false
  case "$teardown_mode" in
    always)
      should_delete=true
      ;;
    on-success)
      if [[ $exit_code -eq 0 ]]; then
        should_delete=true
      fi
      ;;
    off)
      should_delete=false
      ;;
    *)
      should_delete=false
      ;;
  esac

  if [[ $should_delete == true && -n ${SOLANA_CONFIG_DIR-} ]]; then
    if [[ $config_dir_created == true || $teardown_allow_any_config_dir == true ]]; then
      rm -rf "${SOLANA_CONFIG_DIR:?}"
    else
      echo "Refusing to remove SOLANA_CONFIG_DIR=$SOLANA_CONFIG_DIR (not created by this script). Use --config-dir with --teardown to allow cleanup."
    fi
  fi
}
on_exit() {
  local exit_code=$?
  trap - EXIT INT TERM
  cleanup_pids
  if [[ $run_tests == true && $exit_code -ne 0 && -n ${SOLANA_CONFIG_DIR-} ]]; then
    local will_delete=false
    case "$teardown_mode" in
      always)
        will_delete=true
        ;;
      on-success)
        will_delete=false
        ;;
      off|*)
        will_delete=false
        ;;
    esac
    if [[ $will_delete != true || ( $config_dir_created != true && $teardown_allow_any_config_dir != true ) ]]; then
      echo "Self tests failed; keeping SOLANA_CONFIG_DIR=$SOLANA_CONFIG_DIR"
    fi
  fi
  maybe_teardown "$exit_code"
  # Preserve original exit code
  return
}
on_int() {
  trap - EXIT INT TERM
  cleanup_pids
  maybe_teardown 130
  exit 130
}
on_term() {
  trap - EXIT INT TERM
  cleanup_pids
  maybe_teardown 143
  exit 143
}
trap on_exit EXIT
trap on_int INT
trap on_term TERM

run_self_tests() {
  local rpc_host="$bind_address"
  if [[ $rpc_host == "0.0.0.0" || $rpc_host == "::" ]]; then
    rpc_host="127.0.0.1"
  fi
  local rpc_url="http://${rpc_host}:8899"
  local bootstrap_metrics_port=$solanacdn_metrics_base_port
  local solana_bin="$SOLANA_ROOT/target/debug/solana"
  local keygen_bin="$SOLANA_ROOT/target/debug/solana-keygen"

  solana_cmd() {
    if [[ -x "$solana_bin" ]]; then
      "$solana_bin" "$@"
    else
      # shellcheck disable=SC2086 # $solana_cli is a command string with args
      $solana_cli "$@"
    fi
  }
  keygen_cmd() {
    if [[ -x "$keygen_bin" ]]; then
      "$keygen_bin" "$@"
    else
      # shellcheck disable=SC2086 # $solana_keygen is a command string with args
      $solana_keygen "$@"
    fi
  }

  echo "--- Self test: cluster nodes"
  python3 - <<PY
import json, subprocess, sys, time
rpc="$rpc_url"
expected=$num_validators
deadline=time.time()+360
def call(method, params):
  payload={"jsonrpc":"2.0","id":1,"method":method,"params":params}
  out=subprocess.check_output(["curl","-sS",rpc,"-X","POST","-H","Content-Type: application/json","-d",json.dumps(payload)])
  j=json.loads(out)
  if "error" in j:
    raise SystemExit(f"RPC error: {j['error']}")
  return j["result"]
while True:
  nodes=call("getClusterNodes", [])
  if isinstance(nodes, list):
    nodes_with_tpu=sum(1 for n in nodes if isinstance(n, dict) and n.get("tpu"))
  else:
    nodes_with_tpu=0
  if isinstance(nodes, list) and len(nodes) >= expected and nodes_with_tpu >= expected:
    print(f"cluster_nodes={len(nodes)} (expected>={expected})")
    print(f"nodes_with_tpu={nodes_with_tpu} (expected>={expected})")
    break
  if time.time() > deadline:
    if isinstance(nodes, list):
      summary=[{k:n.get(k) for k in ("pubkey","gossip","tpu","tpuQuic","rpc")} for n in nodes if isinstance(n, dict)]
    else:
      summary=nodes
    raise SystemExit(f"Timed out waiting for validators to be ready (got_nodes={len(nodes) if isinstance(nodes,list) else nodes}, nodes={summary})")
  time.sleep(1)
PY

  echo "--- Self test: program binaries present"
  python3 - <<PY
import json, subprocess, sys
rpc="$rpc_url"
programs={
  "alt":"AddressLookupTab1e1111111111111111111111111",
  "config":"Config1111111111111111111111111111111111111",
  "feature":"Feature111111111111111111111111111111111111",
  "stake":"Stake11111111111111111111111111111111111111",
  "token":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
  "token2022":"TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
  "memo1":"Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo",
  "memo3":"MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",
  "ata":"ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
}
def call(method, params):
  payload={"jsonrpc":"2.0","id":1,"method":method,"params":params}
  out=subprocess.check_output(["curl","-sS",rpc,"-X","POST","-H","Content-Type: application/json","-d",json.dumps(payload)])
  j=json.loads(out)
  if "error" in j:
    raise SystemExit(f"RPC error: {j['error']}")
  return j["result"]
for name,addr in programs.items():
  v=call("getAccountInfo",[addr,{"encoding":"base64"}]).get("value")
  if not v:
    raise SystemExit(f"Missing program account: {name}={addr}")
  if not v.get("executable"):
    raise SystemExit(f"Program not executable: {name}={addr}")
print("ok")
PY

  if [[ $enable_solanacdn_metrics == true ]]; then
    echo "--- Self test: solanacdn metrics"
    # The validator logs the bound addr, but in this script we pin it. Retry briefly to allow the
    # metrics server to come up.
    local metrics_ok=false
    for _ in {1..30}; do
      if curl -fsS "http://127.0.0.1:${bootstrap_metrics_port}/metrics" \
        | grep -qF "solanacdn_tx_fair_ordering_enabled 1"; then
        metrics_ok=true
        break
      fi
      sleep 1
    done
    if [[ $metrics_ok != true ]]; then
      echo "Expected fair ordering enabled in solanacdn metrics on port ${bootstrap_metrics_port}."
      echo "Hint: check $SOLANA_CONFIG_DIR/mcp-bootstrap.log for solanacdn metrics bind errors."
      return 1
    fi
  fi

  echo "--- Self test: basic tx (memo3)"
  local tmp_keys
  local tmp_base="${TMPDIR:-/tmp}"
  if [[ -d /tmp && -w /tmp ]]; then
    tmp_base="/tmp"
  fi
  tmp_keys="$(mktemp -d "${tmp_base%/}/agave-mcp-demo-keys.XXXXXX")"
  local alice="$tmp_keys/alice.json"
  local bob="$tmp_keys/bob.json"
  keygen_cmd new --no-passphrase -so "$alice" >/dev/null
  keygen_cmd new --no-passphrase -so "$bob" >/dev/null
  local bob_pubkey
  bob_pubkey="$(keygen_cmd pubkey "$bob")"
  solana_cmd --url "$rpc_url" --keypair "$alice" airdrop 2 >/dev/null
  local sig
  sig="$(solana_cmd --url "$rpc_url" --keypair "$alice" transfer --allow-unfunded-recipient "$bob_pubkey" 0.1 --with-memo "mcp-demo-smoke" --output json | python3 -c 'import sys,json; print(json.load(sys.stdin)["signature"])')"
  python3 - <<PY
import json, subprocess, sys
import time
rpc="$rpc_url"
sig="$sig"
for _ in range(20):
  payload={"jsonrpc":"2.0","id":1,"method":"getTransaction","params":[sig,{"encoding":"jsonParsed","maxSupportedTransactionVersion":0,"commitment":"confirmed"}]}
  out=subprocess.check_output(["curl","-sS",rpc,"-X","POST","-H","Content-Type: application/json","-d",json.dumps(payload)])
  j=json.loads(out)
  r=j.get("result")
  if r:
    break
  time.sleep(1)
if not r:
  raise SystemExit("getTransaction returned null (ensure --enable-rpc-transaction-history is set and try again)")
ixs=r.get("transaction",{}).get("message",{}).get("instructions",[]) or []
prog_ids=[ix.get("programId") for ix in ixs]
if "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr" not in prog_ids:
  raise SystemExit(f"expected memo3 instruction, got programIds={prog_ids}")
print("ok")
PY

  echo "--- Self test: MCP memos (memo1) include microblock+checkpoint+da-cert"
  local rcpt="$tmp_keys/rcpt.json"
  keygen_cmd new --no-passphrase -so "$rcpt" >/dev/null
  local rcpt_pubkey
  rcpt_pubkey="$(keygen_cmd pubkey "$rcpt")"

	  # Send signed tx packets to TPU(s) (UDP). Under normal operation, some of this traffic will
	  # traverse the forwarding stage (non-leader nodes), which is where scheduled MCP memos are
	  # produced.
  local count="$tx_test_count"
  if [[ $count -lt 1 ]]; then
    count=1
  fi

  local tpu_targets_json
  tpu_targets_json="$(python3 - <<PY
import json, subprocess
rpc="$rpc_url"
payload={"jsonrpc":"2.0","id":1,"method":"getClusterNodes","params":[]}
out=subprocess.check_output(["curl","-sS",rpc,"-X","POST","-H","Content-Type: application/json","-d",json.dumps(payload)])
j=json.loads(out)
nodes=j.get("result") or []
targets=[]
for n in nodes:
  tpu=n.get("tpu")
  if tpu:
    targets.append(tpu)
print(json.dumps(targets))
PY
		  )"
		  if [[ $tpu_targets_json == "[]" ]]; then
		    echo "No TPU(UDP) addresses found in getClusterNodes."
		    echo "Hint: ensure the cluster is started with --tpu-enable-udp (added automatically in --run-tests mode)."
		    echo "--- getClusterNodes summary:"
		    python3 - <<PY || true
	import json, subprocess
rpc="$rpc_url"
payload={"jsonrpc":"2.0","id":1,"method":"getClusterNodes","params":[]}
out=subprocess.check_output(["curl","-sS",rpc,"-X","POST","-H","Content-Type: application/json","-d",json.dumps(payload)])
j=json.loads(out)
nodes=j.get("result") or []
for n in nodes:
  if not isinstance(n, dict):
    continue
  print({k:n.get(k) for k in ("pubkey","gossip","tpu","tpuQuic","rpc")})
PY
	    return 1
	  fi

  local blockhash
  blockhash="$(python3 - <<PY
import json, subprocess
rpc="$rpc_url"
payload={"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash","params":[{"commitment":"confirmed"}]}
out=subprocess.check_output(["curl","-sS",rpc,"-X","POST","-H","Content-Type: application/json","-d",json.dumps(payload)])
j=json.loads(out)
print(j["result"]["value"]["blockhash"])
PY
)"

  local workers="$tx_test_workers"
  if [[ $workers -lt 1 ]]; then
    workers=1
  fi
  if [[ ! -x "$solana_bin" ]]; then
    echo "Warning: $solana_bin not found; falling back to cargo-run solana CLI for tx burst. For speed, forcing --tx-test-workers=1."
    workers=1
  fi

  local per_worker=$((count / workers))
  local remainder=$((count % workers))

	  echo "Sending ${count} signed transfers to TPU(s) (UDP) with ${workers} worker(s)..."
  local worker_pids=()
  local w
  for w in $(seq 1 "$workers"); do
    local n="$per_worker"
    if [[ $w -le $remainder ]]; then
      n=$((n + 1))
    fi
    if [[ $n -le 0 ]]; then
      continue
    fi

    (
      set -euo pipefail
	      for j in $(seq 1 "$n"); do
	        # Vary CU price to exercise bid hint parsing paths.
	        price=$((1000 + (w * 100) + (j % 50)))
	        solana_cmd --url "$rpc_url" --keypair "$alice" transfer \
	          --sign-only --dump-transaction-message --blockhash "$blockhash" \
	          --with-compute-unit-price "$price" --output json-compact \
	          "$rcpt_pubkey" 0.000001
	      done | TPU_TARGETS_JSON="$tpu_targets_json" python3 -u /dev/fd/3 3<<-'PY'
	import base64
	import json
	import os
	import socket
	import sys

alphabet="123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
idx={c:i for i,c in enumerate(alphabet)}
def b58decode(s: str) -> bytes:
  n=0
  for ch in s:
    n=n*58+idx[ch]
  b=n.to_bytes((n.bit_length()+7)//8, "big") if n else b""
  pad=0
  for ch in s:
    if ch=="1":
      pad+=1
    else:
      break
  return b"\x00"*pad+b

def shortvec_u16(n: int) -> bytes:
  # Solana shortvec length encoding (little-endian 7-bit groups).
  if n < 0:
    raise ValueError("shortvec length must be >=0")
  out=bytearray()
  while True:
    elem = n & 0x7f
    n >>= 7
    if n:
      out.append(elem | 0x80)
    else:
      out.append(elem)
      break
  return bytes(out)

targets=json.loads(os.environ.get("TPU_TARGETS_JSON","[]"))
if not targets:
  raise SystemExit("TPU_TARGETS_JSON is empty")

sock=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sent=0
for line in sys.stdin:
  line=line.strip()
  if not line:
    continue
  j=json.loads(line)
  msg_b64=j.get("message")
  if not isinstance(msg_b64, str):
    raise SystemExit(f"missing message in sign-only output: keys={list(j.keys())}")
  msg=base64.b64decode(msg_b64)
  if not msg:
    raise SystemExit("message empty")
  # Legacy message: first byte is MessageHeader.num_required_signatures.
  # Versioned message (v0+): first byte is 0x80|version, then header starts at offset 1.
  if msg[0] & 0x80:
    if len(msg) < 4:
      raise SystemExit("versioned message too short")
    required_sigs=msg[1]
  else:
    if len(msg) < 3:
      raise SystemExit("legacy message too short")
    required_sigs=msg[0]
  signers=j.get("signers") or []
  sigs=[]
  for s in signers:
    if not isinstance(s, str) or "=" not in s:
      continue
    sig_b58=s.split("=",1)[1]
    sig=b58decode(sig_b58)
    if len(sig) != 64:
      raise SystemExit(f"unexpected signature length: {len(sig)}")
    sigs.append(sig)
  if len(sigs) < required_sigs:
    raise SystemExit(f"missing signatures: required={required_sigs} got={len(sigs)}")
  wire=shortvec_u16(required_sigs) + b"".join(sigs[:required_sigs]) + msg
  host,port=targets[sent % len(targets)].rsplit(":",1)
  sock.sendto(wire, (host, int(port)))
  sent += 1
print(f"sent={sent}", file=sys.stderr)
PY
    ) &
    worker_pids+=("$!")
  done

  local pid
  for pid in "${worker_pids[@]}"; do
    wait "$pid"
  done

  python3 - <<PY
import json, subprocess, sys, time
rpc="$rpc_url"
memo1="Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo"
deadline=time.time()+$test_timeout_secs

alphabet="123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
idx={c:i for i,c in enumerate(alphabet)}
def b58decode(s: str) -> bytes:
  n=0
  for ch in s:
    n=n*58+idx[ch]
  b=n.to_bytes((n.bit_length()+7)//8, "big") if n else b""
  pad=0
  for ch in s:
    if ch=="1":
      pad+=1
    else:
      break
  return b"\\x00"*pad+b

MAGIC=b"SCDNMCP\\x00"
def parse_kind(data: bytes):
  if len(data) < 16:
    return None
  if not data.startswith(MAGIC):
    return None
  version=data[8]
  if version != 1:
    return None

  def try_parse(kind_encoding: str):
    i=9
    if kind_encoding=="u32":
      kind=int.from_bytes(data[i:i+4], "little"); i+=4
      kind_map={0:"Microblock",1:"Checkpoint",2:"DaCert"}
      if kind not in kind_map:
        return None
    else:
      kind=data[i]; i+=1
      kind_map={1:"Microblock",2:"Checkpoint",3:"DaCert"}
      if kind not in kind_map:
        return None
    # slot
    if len(data) < i+8:
      return None
    slot=int.from_bytes(data[i:i+8],"little"); i+=8
    if slot > 10**12:
      return None
    # lane_id
    if len(data) < i+1:
      return None
    lane=data[i]; i+=1
    if lane > 255:
      return None
    # object_id + chunk_index + chunk_total
    need=i+32+2+2
    if len(data) < need:
      return None
    i+=32
    i+=2
    i+=2
    # object_chunk vec
    if len(data) < i+8:
      return None
    obj_len=int.from_bytes(data[i:i+8],"little"); i+=8
    if obj_len > len(data)-i:
      return None
    i+=obj_len
    # leader_pubkey + leader_time_ms
    if len(data) < i+32+8:
      return None
    i+=32
    i+=8
    # signature vec
    if len(data) < i+8:
      return None
    sig_len=int.from_bytes(data[i:i+8],"little"); i+=8
    if sig_len > len(data)-i:
      return None
    i+=sig_len
    if i != len(data):
      return None
    return kind_map[kind]

  return try_parse("u32") or try_parse("u8")

def call(method, params):
  payload={"jsonrpc":"2.0","id":1,"method":method,"params":params}
  out=subprocess.check_output(["curl","-sS",rpc,"-X","POST","-H","Content-Type: application/json","-d",json.dumps(payload)])
  j=json.loads(out)
  if "error" in j:
    raise RuntimeError(j["error"])
  return j["result"]

want={"Microblock","Checkpoint","DaCert"}
seen=set()
counts={}
while time.time() < deadline:
  sigs=call("getSignaturesForAddress",[memo1,{"limit":100}]) or []
  for ent in sigs:
    sig=ent.get("signature")
    if not sig:
      continue
    tx=call("getTransaction",[sig,{"encoding":"json","maxSupportedTransactionVersion":0,"commitment":"confirmed"}])
    if not tx:
      continue
    msg=(tx.get("transaction") or {}).get("message") or {}
    keys=msg.get("accountKeys") or []
    if keys and isinstance(keys[0], dict):
      keys=[k.get("pubkey") for k in keys]
    for ix in msg.get("instructions") or []:
      prog=ix.get("programId")
      if not prog:
        prog=keys[ix.get("programIdIndex")]
      if prog != memo1:
        continue
      data_b58=ix.get("data")
      if not isinstance(data_b58,str):
        continue
      kind=parse_kind(b58decode(data_b58))
      if not kind:
        continue
      counts[kind]=counts.get(kind,0)+1
      seen.add(kind)
  if want.issubset(seen):
    break
  time.sleep(1)

missing=sorted(want-seen)
print("mcp_memo_counts=",counts)
if missing:
  raise SystemExit(f"Timed out waiting for MCP memo kinds: missing={missing}")
print("ok")
PY
}

start_local_snapshot_service() {
  local ledger_dir="$SOLANA_CONFIG_DIR/bootstrap-validator"
  local service_dir="$SOLANA_CONFIG_DIR/snapshot-service"
  local manifest_path="$service_dir/snapshot-manifest.json"

  mkdir -p "$service_dir"

  echo "--- Local snapshot: waiting for bootstrap snapshot archive"
  local snapshot_path=""
  local stable_path=""
  local deadline=$((SECONDS + 180))
  while [[ $SECONDS -lt $deadline ]]; do
    snapshot_path="$(ls -1t "$ledger_dir"/snapshot-*.tar.zst "$ledger_dir"/snapshot-*.tar.bz2 2>/dev/null | head -n 1 || true)"
    if [[ -n $snapshot_path ]]; then
      # Wait for the snapshot file size to stabilize to avoid serving a partially-written archive.
      local s1 s2
      s1="$(SNAPSHOT_PATH="$snapshot_path" python3 - <<'PY'
import os
import os.path
import sys

p=os.environ.get("SNAPSHOT_PATH","")
try:
  print(os.path.getsize(p))
except FileNotFoundError:
  print("")
PY
)"
      sleep 1
      s2="$(SNAPSHOT_PATH="$snapshot_path" python3 - <<'PY'
import os
import os.path
import sys

p=os.environ.get("SNAPSHOT_PATH","")
try:
  print(os.path.getsize(p))
except FileNotFoundError:
  print("")
PY
)"
      if [[ -n $s1 && $s1 == "$s2" ]]; then
        stable_path="$snapshot_path"
        break
      fi
    fi
    sleep 1
  done

  if [[ -z $stable_path ]]; then
    echo "Timed out waiting for a bootstrap snapshot archive under: $ledger_dir"
    echo "Hint: check $SOLANA_CONFIG_DIR/mcp-bootstrap.log for snapshot generation errors."
    return 1
  fi

	  local snapshot_file
	  snapshot_file="$(basename "$stable_path")"
	  # Do not symlink the snapshot archive: the bootstrap validator may delete old snapshots as
	  # new ones are produced, which would break late joiners. Prefer a hard link (no data copy),
	  # falling back to a copy if the filesystem doesn't support linking.
	  rm -f "$service_dir/$snapshot_file"
	  if ! ln "$stable_path" "$service_dir/$snapshot_file" 2>/dev/null; then
	    cp -f "$stable_path" "$service_dir/$snapshot_file"
	  fi

	  SNAPSHOT_PATH="$stable_path" MANIFEST_PATH="$manifest_path" python3 - <<'PY'
import json, os

snapshot_path=os.environ["SNAPSHOT_PATH"]
manifest_path=os.environ["MANIFEST_PATH"]
fname=os.path.basename(snapshot_path)
parts=fname.split("-", 2)
if len(parts) < 3 or parts[0] != "snapshot":
  raise SystemExit(f"unexpected snapshot filename: {fname}")
slot=int(parts[1])
size=os.path.getsize(snapshot_path)
manifest={
  "updated_at": None,
  "full_snapshot": {"filename": fname, "slot": slot, "size_bytes": size},
  "incremental_snapshots": [],
}
with open(manifest_path, "w") as f:
  json.dump(manifest, f)
PY

  echo "--- Local snapshot: serving $snapshot_file via range HTTP on 127.0.0.1:${snapshot_service_port}"
  SNAPSHOT_SERVICE_DIR="$service_dir" SNAPSHOT_SERVICE_PORT="$snapshot_service_port" python3 -u - <<'PY' >"$SOLANA_CONFIG_DIR/mcp-snapshot-service.log" 2>&1 &
import os
import socketserver
from http.server import BaseHTTPRequestHandler
from urllib.parse import unquote

root = os.environ["SNAPSHOT_SERVICE_DIR"]
port = int(os.environ["SNAPSHOT_SERVICE_PORT"])

def safe_join(base: str, rel: str) -> str:
  rel = rel.lstrip("/")
  path = os.path.normpath(os.path.join(base, rel))
  if not path.startswith(os.path.abspath(base) + os.sep) and path != os.path.abspath(base):
    raise ValueError("path traversal")
  return path

class Handler(BaseHTTPRequestHandler):
  protocol_version = "HTTP/1.1"

  def log_message(self, fmt, *args):
    return

  def do_GET(self):
    path = unquote(self.path.split("?", 1)[0])
    if path == "/snapshot-manifest.json":
      manifest_path = os.path.join(root, "snapshot-manifest.json")
      try:
        with open(manifest_path, "rb") as f:
          body = f.read()
      except FileNotFoundError:
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()
        return
      self.send_response(200)
      self.send_header("Content-Type", "application/json")
      self.send_header("Content-Length", str(len(body)))
      self.end_headers()
      self.wfile.write(body)
      return

    try:
      file_path = safe_join(root, path)
    except Exception:
      self.send_response(400)
      self.send_header("Content-Length", "0")
      self.end_headers()
      return

    if not os.path.isfile(file_path):
      self.send_response(404)
      self.send_header("Content-Length", "0")
      self.end_headers()
      return

    rng = self.headers.get("Range", "")
    if not rng.startswith("bytes=") or "-" not in rng:
      self.send_response(416)
      self.send_header("Content-Length", "0")
      self.end_headers()
      return
    start_s, end_s = rng[len("bytes="):].split("-", 1)
    try:
      start = int(start_s)
      end = int(end_s)
    except Exception:
      self.send_response(416)
      self.send_header("Content-Length", "0")
      self.end_headers()
      return

    size = os.path.getsize(file_path)
    if start < 0 or end < start or end >= size:
      self.send_response(416)
      self.send_header("Content-Length", "0")
      self.end_headers()
      return

    length = end - start + 1
    self.send_response(206)
    self.send_header("Content-Type", "application/octet-stream")
    self.send_header("Content-Length", str(length))
    self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
    self.send_header("Connection", "close")
    self.end_headers()
    with open(file_path, "rb") as f:
      f.seek(start)
      self.wfile.write(f.read(length))

class ThreadingHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
  daemon_threads = True
  allow_reuse_address = True

with ThreadingHTTPServer(("127.0.0.1", port), Handler) as httpd:
  httpd.serve_forever()
PY
  pids+=("$!")
}

"$demo_dir"/faucet.sh >"$SOLANA_CONFIG_DIR/mcp-faucet.log" 2>&1 &
pids+=("$!")

pop_endpoints=()
solanacdn_common_validator_args=()

if [[ $use_pop_sim == true ]]; then
  echo "Starting SolanaCDN POP simulator on $pop_sim_listen"
  profile_args=()
  if [[ -n ${CARGO_BUILD_PROFILE-} ]]; then
    profile_args+=(--profile "$CARGO_BUILD_PROFILE")
  fi
  cargo ${CARGO_TOOLCHAIN-} run "${profile_args[@]}" --bin solanacdn-pop-sim -- --listen "$pop_sim_listen" \
    >"$SOLANA_CONFIG_DIR/mcp-pop-sim.log" 2>&1 &
  pids+=("$!")

  # Configure validators to use SolanaCDN control-plane only (no shreds/vote tunnel) and enable MCP
  # DA over SolanaCDN.
  pop_endpoints+=("$pop_sim_listen")
  solanacdn_common_validator_args+=(
    --solanacdn-tls-insecure-skip-verify
    --solanacdn-udp off
    --solanacdn-no-shreds
    --solanacdn-no-subscribe
    --solanacdn-no-inject
    --solanacdn-no-direct-shreds
    --solanacdn-no-vote-tunnel
  )
fi

if [[ $use_full_pop == true ]]; then
  solanacdn_repo="${SOLANACDN_REPO:-$solanacdn_repo_default}"
  if [[ ! -f "$solanacdn_repo/Cargo.toml" ]]; then
    echo "Expected SolanaCDN repo at: $solanacdn_repo"
    echo "Set SOLANACDN_REPO=/path/to/solanaCdn or run without --full-pop"
    exit 1
  fi

  pop_host="${pop_sim_listen%:*}"
  pop_port="${pop_sim_listen##*:}"
  mesh_port_base=$((pop_port + 1000))

  echo "Building SolanaCDN POP (solanacdn-pop) from $solanacdn_repo"
  solanacdn_cargo_toolchain="${SOLANACDN_CARGO_TOOLCHAIN:-+nightly}"
  profile_args=()
  solanacdn_target_dir="${SOLANACDN_CARGO_TARGET_DIR:-$solanacdn_repo/target}"
  if [[ -n ${CARGO_BUILD_PROFILE-} ]]; then
    profile_args+=(--profile "$CARGO_BUILD_PROFILE")
    pop_bin="$solanacdn_target_dir/$CARGO_BUILD_PROFILE/solanacdn-pop"
  else
    pop_bin="$solanacdn_target_dir/debug/solanacdn-pop"
  fi
  CARGO_TARGET_DIR="$solanacdn_target_dir" cargo $solanacdn_cargo_toolchain build "${profile_args[@]}" --manifest-path "$solanacdn_repo/Cargo.toml" -p solanacdn-pop >/dev/null

  for i in $(seq 0 $((num_pops - 1))); do
    listen_quic="$pop_host:$((pop_port + i))"
    mesh_listen="$pop_host:$((mesh_port_base + i))"
    pop_endpoints+=("$listen_quic")

    pop_cfg="$SOLANA_CONFIG_DIR/mcp-solanacdn-pop-$i.toml"

    peers=()
    if [[ $num_pops -gt 1 ]]; then
      for j in $(seq 0 $((num_pops - 1))); do
        if [[ $j -eq $i ]]; then
          continue
        fi
        peers+=("$pop_host:$((mesh_port_base + j))")
      done
    fi

    peers_toml=""
    if [[ ${#peers[@]} -gt 0 ]]; then
      for p in "${peers[@]}"; do
        if [[ -n $peers_toml ]]; then
          peers_toml+=", "
        fi
        peers_toml+="\"$p\""
      done
    fi

    mcp_da_mesh=false
    if [[ $num_pops -gt 1 ]]; then
      mcp_da_mesh=true
    fi

    cat >"$pop_cfg" <<EOF
listen_quic = "$listen_quic"
public_addr = "$listen_quic"
mcp_da_mesh = $mcp_da_mesh

[pipe_api]
required = false

[tls]
server_name = "solanacdn-pop"
insecure_skip_verify = true

[cache]
max_batches = 256

[metrics]
listen = "127.0.0.1:0"
EOF

    if [[ $num_pops -gt 1 ]]; then
      cat >>"$pop_cfg" <<EOF

[mesh]
listen = "$mesh_listen"
peers = [$peers_toml]
EOF
    fi

    echo "Starting SolanaCDN POP $i on $listen_quic"
    "$pop_bin" --config "$pop_cfg" >"$SOLANA_CONFIG_DIR/mcp-solanacdn-pop-$i.log" 2>&1 &
    pids+=("$!")
  done

  # Configure validators to use SolanaCDN control-plane only (no shreds/vote tunnel) and enable MCP
  # DA over SolanaCDN.
  solanacdn_common_validator_args+=(
    --solanacdn-tls-insecure-skip-verify
    --solanacdn-udp off
    --solanacdn-no-shreds
    --solanacdn-no-subscribe
    --solanacdn-no-inject
    --solanacdn-no-direct-shreds
    --solanacdn-no-vote-tunnel
  )
fi

bootstrap_extra_args=("${extra_validator_args[@]}")
if [[ ${#pop_endpoints[@]} -gt 0 ]]; then
  bootstrap_extra_args=(--solanacdn-pop "${pop_endpoints[0]}" "${solanacdn_common_validator_args[@]}" "${extra_validator_args[@]}")
fi
if [[ $enable_solanacdn_metrics == true ]]; then
  bootstrap_extra_args=(--solanacdn-metrics-addr "127.0.0.1:${solanacdn_metrics_base_port}" "${bootstrap_extra_args[@]}")
fi

bootstrap_snapshot_interval_arg=()
if [[ $local_snapshot_service_enabled == true ]]; then
  if ! has_flag --snapshot-interval-slots "${extra_validator_args[@]}"; then
    bootstrap_snapshot_interval_arg=(--snapshot-interval-slots "$bootstrap_snapshot_interval_slots")
  fi
fi

"$demo_dir"/bootstrap-validator.sh \
  --no-restart \
  --bind-address "$bind_address" \
  --gossip-port 8001 \
  --dynamic-port-range 8002-8027 \
  "${bootstrap_snapshot_interval_arg[@]}" \
  --log "$SOLANA_CONFIG_DIR/mcp-bootstrap.log" \
  --mcp \
  --mcp-scheduled \
  --mcp-lanes "$lanes" \
  --mcp-da-threshold-bps "$mcp_da_threshold_bps" \
  --mcp-microblock-max-refs "$mcp_microblock_max_refs" \
  "${bootstrap_extra_args[@]}" \
  >"$SOLANA_CONFIG_DIR/mcp-bootstrap-wrapper.log" 2>&1 &
pids+=("$!")

# Wait for bootstrap RPC health.
rpc_health_host="$bind_address"
if [[ $rpc_health_host == "0.0.0.0" || $rpc_health_host == "::" ]]; then
  rpc_health_host="127.0.0.1"
fi
for _ in {1..60}; do
  if curl -fsS "http://${rpc_health_host}:8899/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl -fsS "http://${rpc_health_host}:8899/health" >/dev/null 2>&1; then
  echo "Bootstrap RPC did not become healthy. Check logs under: $SOLANA_CONFIG_DIR"
  exit 1
fi

if [[ $activate_vote_withholding_feature == true ]]; then
  echo "Activating MCP vote-withholding feature: $mcp_vote_withholding_feature"
  # Use the faucet keypair as fee payer.
  $solana_cli --url "http://${rpc_health_host}:8899" --keypair "$SOLANA_CONFIG_DIR/faucet.json" \
    feature activate "$mcp_vote_withholding_feature" \
    >"$SOLANA_CONFIG_DIR/mcp-feature-activate.log" 2>&1 || true
fi

if [[ $local_snapshot_service_enabled == true ]]; then
  start_local_snapshot_service
fi

if [[ $num_validators -gt 1 ]]; then
  for i in $(seq 1 $((num_validators - 1))); do
    gossip_port=$((8101 + (i - 1) * 100))
    dyn_start=$((8102 + (i - 1) * 100))
    dyn_end=$((8127 + (i - 1) * 100))
    rpc_port=$((8899 + i * 10))

    node_extra_args=("${extra_validator_args[@]}")
    if [[ ${#pop_endpoints[@]} -gt 0 ]]; then
      pop_index=$((i % ${#pop_endpoints[@]}))
      node_extra_args=(--solanacdn-pop "${pop_endpoints[$pop_index]}" "${solanacdn_common_validator_args[@]}" "${extra_validator_args[@]}")
    fi
    if [[ $enable_solanacdn_metrics == true ]]; then
      node_extra_args=(--solanacdn-metrics-addr "127.0.0.1:$((solanacdn_metrics_base_port + i))" "${node_extra_args[@]}")
    fi

    "$demo_dir"/validator.sh \
      --no-restart \
      --label "-mcp-$i" \
      --bind-address "$bind_address" \
      --gossip-port "$gossip_port" \
      --dynamic-port-range "${dyn_start}-${dyn_end}" \
      --rpc-port "$rpc_port" \
      --log "$SOLANA_CONFIG_DIR/mcp-validator-$i.log" \
      --mcp \
      --mcp-scheduled \
      --mcp-lanes "$lanes" \
      --mcp-da-threshold-bps "$mcp_da_threshold_bps" \
      --mcp-microblock-max-refs "$mcp_microblock_max_refs" \
      "${node_extra_args[@]}" \
      >"$SOLANA_CONFIG_DIR/mcp-validator-$i-wrapper.log" 2>&1 &
    pids+=("$!")
  done
fi

echo "MCP cluster running:"
echo "  RPC: http://${rpc_health_host}:8899"
echo "  Logs: $SOLANA_CONFIG_DIR/mcp-*.log"
echo "Suggested load test:"
echo "  $demo_dir/bench-tps.sh"

if [[ $run_tests == true ]]; then
  run_self_tests
  echo "--- Self test: OK"
  if [[ $bench_tps_secs -le 0 && $duration_secs -le 0 ]]; then
    exit 0
  fi
fi

if [[ $bench_tps_secs -gt 0 ]]; then
  # On macOS, some build scripts require DYLD_LIBRARY_PATH to locate libclang.dylib.
  if [[ $(uname) == Darwin && -z ${DYLD_LIBRARY_PATH-} && -r /Library/Developer/CommandLineTools/usr/lib/libclang.dylib ]]; then
    export DYLD_LIBRARY_PATH=/Library/Developer/CommandLineTools/usr/lib
  fi
  echo "Running bench-tps for ${bench_tps_secs}s (tx-count=$bench_tps_tx_count)..."
  "$demo_dir"/bench-tps.sh --url "http://${rpc_health_host}:8899" --faucet "${rpc_health_host}:9900" \
    --duration "$bench_tps_secs" --tx-count "$bench_tps_tx_count" \
    >"$SOLANA_CONFIG_DIR/mcp-bench-tps.log" 2>&1
  echo "bench-tps complete. Log: $SOLANA_CONFIG_DIR/mcp-bench-tps.log"
  if [[ $run_tests == true && $duration_secs -le 0 ]]; then
    exit 0
  fi
fi

if [[ $duration_secs -gt 0 ]]; then
  sleep "$duration_secs"
  exit 0
fi

wait

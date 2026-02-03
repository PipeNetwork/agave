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
solanacdn_repo_default="$demo_dir/../../solanaCdn"

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
  --duration-secs N           Exit after N seconds (default: wait forever). Useful for smoke tests.
  --bench-tps-secs N          Run bench-tps for N seconds once the cluster is healthy (default: $bench_tps_secs)
  --bench-tps-tx-count N      Total bench-tps tx-count when --bench-tps-secs is set (default: $bench_tps_tx_count)
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

if [[ $num_pops -lt 1 ]]; then
  usage "Error: --num-pops must be >= 1"
fi

if [[ $use_pop_sim == true && $num_pops -ne 1 ]]; then
  usage "Error: --num-pops requires --full-pop (POP simulator does not support multi-POP mesh)"
fi

if [[ $use_full_pop == false && $num_pops -ne 1 ]]; then
  usage "Error: --num-pops requires --full-pop"
fi

if [[ $reset == true ]]; then
  "$demo_dir"/setup.sh --deactivate-feature "$mcp_vote_withholding_feature"
elif [[ ! -f "$SOLANA_CONFIG_DIR/bootstrap-validator/genesis.bin" && ! -f "$SOLANA_CONFIG_DIR/bootstrap-validator/genesis.tar.bz2" ]]; then
  "$demo_dir"/setup.sh --deactivate-feature "$mcp_vote_withholding_feature"
fi

pids=()
cleanup() {
  set +euo pipefail
  for pid in "${pids[@]}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  for pid in "${pids[@]}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
}
trap cleanup INT TERM EXIT

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

"$demo_dir"/bootstrap-validator.sh \
  --no-restart \
  --gossip-port 8001 \
  --dynamic-port-range 8002-8027 \
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
for _ in {1..60}; do
  if curl -fsS "http://127.0.0.1:8899/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl -fsS "http://127.0.0.1:8899/health" >/dev/null 2>&1; then
  echo "Bootstrap RPC did not become healthy. Check logs under: $SOLANA_CONFIG_DIR"
  exit 1
fi

if [[ $activate_vote_withholding_feature == true ]]; then
  echo "Activating MCP vote-withholding feature: $mcp_vote_withholding_feature"
  # Use the faucet keypair as fee payer.
  $solana_cli --url "http://127.0.0.1:8899" --keypair "$SOLANA_CONFIG_DIR/faucet.json" \
    feature activate "$mcp_vote_withholding_feature" \
    >"$SOLANA_CONFIG_DIR/mcp-feature-activate.log" 2>&1 || true
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

    "$demo_dir"/validator.sh \
      --no-restart \
      --label "-mcp-$i" \
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
echo "  RPC: http://127.0.0.1:8899"
echo "  Logs: $SOLANA_CONFIG_DIR/mcp-*.log"
echo "Suggested load test:"
echo "  $demo_dir/bench-tps.sh"

if [[ $bench_tps_secs -gt 0 ]]; then
  echo "Running bench-tps for ${bench_tps_secs}s (tx-count=$bench_tps_tx_count)..."
  "$demo_dir"/bench-tps.sh --duration "$bench_tps_secs" --tx-count "$bench_tps_tx_count" \
    >"$SOLANA_CONFIG_DIR/mcp-bench-tps.log" 2>&1
  echo "bench-tps complete. Log: $SOLANA_CONFIG_DIR/mcp-bench-tps.log"
fi

if [[ $duration_secs -gt 0 ]]; then
  sleep "$duration_secs"
  exit 0
fi

wait

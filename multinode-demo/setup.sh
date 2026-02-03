#!/usr/bin/env bash

here=$(dirname "$0")
# shellcheck source=multinode-demo/common.sh
source "$here"/common.sh

set -e

enable_warmup_epochs=true
passthrough_args=()
while [[ -n ${1-} ]]; do
  case "$1" in
    --no-warmup-epochs)
      enable_warmup_epochs=false
      shift
      ;;
    *)
      passthrough_args+=("$1")
      shift
      ;;
  esac
done

rm -rf "$SOLANA_CONFIG_DIR"/bootstrap-validator
mkdir -p "$SOLANA_CONFIG_DIR"/bootstrap-validator

# Create genesis ledger
if [[ -r $FAUCET_KEYPAIR ]]; then
  cp -f "$FAUCET_KEYPAIR" "$SOLANA_CONFIG_DIR"/faucet.json
else
  $solana_keygen new --no-passphrase -fso "$SOLANA_CONFIG_DIR"/faucet.json
fi

if [[ -f $BOOTSTRAP_VALIDATOR_IDENTITY_KEYPAIR ]]; then
  cp -f "$BOOTSTRAP_VALIDATOR_IDENTITY_KEYPAIR" "$SOLANA_CONFIG_DIR"/bootstrap-validator/identity.json
else
  $solana_keygen new --no-passphrase -so "$SOLANA_CONFIG_DIR"/bootstrap-validator/identity.json
fi
if [[ -f $BOOTSTRAP_VALIDATOR_STAKE_KEYPAIR ]]; then
  cp -f "$BOOTSTRAP_VALIDATOR_STAKE_KEYPAIR" "$SOLANA_CONFIG_DIR"/bootstrap-validator/stake-account.json
else
  $solana_keygen new --no-passphrase -so "$SOLANA_CONFIG_DIR"/bootstrap-validator/stake-account.json
fi
if [[ -f $BOOTSTRAP_VALIDATOR_VOTE_KEYPAIR ]]; then
  cp -f "$BOOTSTRAP_VALIDATOR_VOTE_KEYPAIR" "$SOLANA_CONFIG_DIR"/bootstrap-validator/vote-account.json
else
  $solana_keygen new --no-passphrase -so "$SOLANA_CONFIG_DIR"/bootstrap-validator/vote-account.json
fi

args=(
  "${passthrough_args[@]}"
  --max-genesis-archive-unpacked-size 1073741824
  --bootstrap-validator "$SOLANA_CONFIG_DIR"/bootstrap-validator/identity.json
                        "$SOLANA_CONFIG_DIR"/bootstrap-validator/vote-account.json
                        "$SOLANA_CONFIG_DIR"/bootstrap-validator/stake-account.json
)

if [[ $enable_warmup_epochs == true ]]; then
  args+=(--enable-warmup-epochs)
fi

  if [[ -z ${SOLANA_SKIP_PROGRAM_FETCH-} ]]; then
    "$SOLANA_ROOT"/fetch-core-bpf.sh
    if [[ -r core-bpf-genesis-args.sh ]]; then
      CORE_BPF_GENESIS_ARGS=$(cat "$SOLANA_ROOT"/core-bpf-genesis-args.sh)
      #shellcheck disable=SC2207
      #shellcheck disable=SC2206
      args+=($CORE_BPF_GENESIS_ARGS)
    fi

    "$SOLANA_ROOT"/fetch-spl.sh
    if [[ -r spl-genesis-args.sh ]]; then
      SPL_GENESIS_ARGS=$(cat "$SOLANA_ROOT"/spl-genesis-args.sh)
      #shellcheck disable=SC2207
      #shellcheck disable=SC2206
      args+=($SPL_GENESIS_ARGS)
    fi
  else
    echo "Skipping fetch-core-bpf.sh and fetch-spl.sh (SOLANA_SKIP_PROGRAM_FETCH=1)"

    # For local/offline development, include the vendored program binaries shipped with this repo.
    # This enables features like MCP that rely on SPL Memo being present in the genesis config.
    programs_dir="${SOLANA_PROGRAM_BINARIES_DIR:-$SOLANA_ROOT/program-binaries/src/programs}"
    if [[ -d "$programs_dir" ]]; then
      echo "Using program binaries from: $programs_dir"

      # Core BPF programs (upgradeable loader v3)
      args+=(
        --upgradeable-program AddressLookupTab1e1111111111111111111111111 BPFLoaderUpgradeab1e11111111111111111111111 "$programs_dir/core_bpf_address_lookup_table-3.0.0.so" none
        --upgradeable-program Config1111111111111111111111111111111111111 BPFLoaderUpgradeab1e11111111111111111111111 "$programs_dir/core_bpf_config-3.0.0.so" none
        --upgradeable-program Feature111111111111111111111111111111111111 BPFLoaderUpgradeab1e11111111111111111111111 "$programs_dir/core_bpf_feature_gate-0.0.1.so" none
        --upgradeable-program Stake11111111111111111111111111111111111111 BPFLoaderUpgradeab1e11111111111111111111111 "$programs_dir/core_bpf_stake-1.0.1.so" none
      )

      # SPL programs
      args+=(
        --bpf-program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA BPFLoader2111111111111111111111111111111111 "$programs_dir/spl_token-3.5.0.so"
        --upgradeable-program TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb BPFLoaderUpgradeab1e11111111111111111111111 "$programs_dir/spl_token_2022-10.0.0.so" none
        --bpf-program Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo BPFLoader1111111111111111111111111111111111 "$programs_dir/spl_memo-1.0.0.so"
        --bpf-program MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr BPFLoader2111111111111111111111111111111111 "$programs_dir/spl_memo-3.0.0.so"
        --bpf-program ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL BPFLoader2111111111111111111111111111111111 "$programs_dir/spl_associated_token_account-1.1.1.so"
      )
    else
      echo "Warning: program binaries directory not found: $programs_dir"
      echo "Genesis will not include vended core/spl BPF programs."
    fi
  fi

default_arg --ledger "$SOLANA_CONFIG_DIR"/bootstrap-validator
default_arg --faucet-pubkey "$SOLANA_CONFIG_DIR"/faucet.json
default_arg --faucet-lamports 500000000000000000
default_arg --hashes-per-tick auto
default_arg --cluster-type development

$solana_genesis "${args[@]}"

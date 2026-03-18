#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  install-layout.sh [--prefix DIR] [--env-dir DIR] [--systemd-dir DIR]

Defaults:
  --prefix      /opt/agave/devnet
  --env-dir     /etc/agave/devnet
  --systemd-dir /etc/systemd/system

This installs the devnet helper scripts and systemd unit templates from the repo into standard
host paths. It does not install the agave-validator binary, create the agave user, or generate
all node keypairs for you.
EOF
}

prefix="/opt/agave/devnet"
env_dir="/etc/agave/devnet"
systemd_dir="/etc/systemd/system"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix)
      prefix="$2"
      shift 2
      ;;
    --env-dir)
      env_dir="$2"
      shift 2
      ;;
    --systemd-dir)
      systemd_dir="$2"
      shift 2
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

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
source_dir="$repo_root/scripts/devnet"

install -d -m 0755 "$prefix"
install -d -m 0755 "$env_dir"
install -d -m 0755 "$env_dir/examples"
install -d -m 0755 "$systemd_dir"

install -m 0755 "$source_dir/common.sh" "$prefix/common.sh"
install -m 0755 "$source_dir/create-genesis.sh" "$prefix/create-genesis.sh"
install -m 0755 "$source_dir/join-fair-validator.sh" "$prefix/join-fair-validator.sh"
install -m 0755 "$source_dir/start-validator.sh" "$prefix/start-validator.sh"
install -m 0755 "$source_dir/start-rpc.sh" "$prefix/start-rpc.sh"
install -m 0755 "$source_dir/start-faucet.sh" "$prefix/start-faucet.sh"
install -m 0755 "$source_dir/cluster-health.sh" "$prefix/cluster-health.sh"

install -m 0644 "$source_dir/systemd/agave-validator@.service" "$systemd_dir/agave-validator@.service"
install -m 0644 "$source_dir/systemd/agave-rpc@.service" "$systemd_dir/agave-rpc@.service"
install -m 0644 "$source_dir/systemd/agave-faucet@.service" "$systemd_dir/agave-faucet@.service"

install -m 0644 "$source_dir/env/bootstrap.env.example" "$env_dir/examples/bootstrap.env.example"
install -m 0644 "$source_dir/env/faucet.env.example" "$env_dir/examples/faucet.env.example"
install -m 0644 "$source_dir/env/public-fair-devnet.env.example" "$env_dir/examples/public-fair-devnet.env.example"
install -m 0644 "$source_dir/env/validator.env.example" "$env_dir/examples/validator.env.example"
install -m 0644 "$source_dir/env/validator.secrets.env.example" "$env_dir/examples/validator.secrets.env.example"
install -m 0644 "$source_dir/env/rpc.env.example" "$env_dir/examples/rpc.env.example"
install -m 0644 "$source_dir/env/rpc.secrets.env.example" "$env_dir/examples/rpc.secrets.env.example"

echo "installed scripts to $prefix"
echo "installed example env files to $env_dir/examples"
echo "installed systemd units to $systemd_dir"
echo "run: systemctl daemon-reload"

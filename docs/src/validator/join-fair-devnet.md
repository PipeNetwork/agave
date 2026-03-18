---
title: Join Public Fair Devnet
sidebar_position: 61
sidebar_label: Join Public Fair Devnet
pagination_label: Join Public Fair Devnet
---

This is the short path for joining an existing public devnet as a **voting validator** with
`--fair` enabled.

Use this when:

- the cluster already exists
- the operator has already published an entrypoint, known validators, genesis hash, and shred version
- the operator has given you SolanaCDN discovery details

## What the devnet operator must publish

At minimum, publish these values:

- `ENTRYPOINTS`
- `KNOWN_VALIDATORS`
- `EXPECTED_GENESIS_HASH`
- `EXPECTED_SHRED_VERSION`
- either:
  - `SOLANACDN_API_BASE` plus a per-validator `SOLANACDN_API_TOKEN`, or
  - explicit `SOLANACDN_POPS` / `SOLANACDN_CONTROL`

There is an example publishable shell file at `scripts/devnet/env/public-fair-devnet.env.example`.

## One-time install

On the validator host:

```bash
sudo scripts/devnet/install-layout.sh
sudo systemctl daemon-reload
```

You still need:

- `/opt/agave/bin/agave-validator`
- an `agave` user
- validator identity and vote keypairs

## Simplest join flow

Fastest path if the operator hosts a public network env file:

```bash
sudo scripts/devnet/install-layout.sh
sudo systemctl daemon-reload

SOLANACDN_API_TOKEN=<YOUR_SOLANACDN_TOKEN> /opt/agave/devnet/join-fair-validator.sh \
  --network-env-url https://devnet.example.org/public-fair-devnet.env \
  --node-name validator-7 \
  --bind-address <YOUR_PUBLIC_IP> \
  --identity /etc/agave/keys/validator-7-identity.json \
  --vote-account /etc/agave/keys/validator-7-vote-account.json \
  --print-command
```

If the printed command looks right, run it again without `--print-command`.

## Simplest `systemd` flow

If you want a persistent service instead of an interactive shell command:

```bash
sudo scripts/devnet/install-layout.sh
sudo systemctl daemon-reload

sudo SOLANACDN_API_TOKEN=<YOUR_SOLANACDN_TOKEN> /opt/agave/devnet/join-fair-validator.sh \
  --network-env-url https://devnet.example.org/public-fair-devnet.env \
  --node-name validator-7 \
  --bind-address <YOUR_PUBLIC_IP> \
  --identity /etc/agave/keys/validator-7-identity.json \
  --vote-account /etc/agave/keys/validator-7-vote-account.json \
  --write-env-file /etc/agave/devnet/validator-7.env

sudo systemctl enable --now agave-validator@validator-7
```

If a SolanaCDN token is present, the helper will also write
`/etc/agave/devnet/validator-7.secrets.env` automatically.

If you prefer a local file instead of a URL:

1. Save the network settings into a shell file, for example:

```bash
sudo cp /etc/agave/devnet/examples/public-fair-devnet.env.example /etc/agave/devnet/public-fair-devnet.env
sudo vi /etc/agave/devnet/public-fair-devnet.env
```

2. Use that file with the helper:

```bash
/opt/agave/devnet/join-fair-validator.sh \
  --network-env-file /etc/agave/devnet/public-fair-devnet.env \
  --node-name validator-7 \
  --bind-address <YOUR_PUBLIC_IP> \
  --identity /etc/agave/keys/validator-7-identity.json \
  --vote-account /etc/agave/keys/validator-7-vote-account.json \
  --solanacdn-api-token <YOUR_SOLANACDN_TOKEN> \
  --print-command
```

If the printed command looks right, run it again without `--print-command`.

## What this script does

`scripts/devnet/join-fair-validator.sh` is a small wrapper around
`scripts/devnet/start-validator.sh`. It starts a voting validator with:

- `--solanacdn-hybrid`
- `--fair-max-protection`

It also passes through:

- entrypoints
- known validators
- genesis hash
- shred version
- SolanaCDN discovery

## Public internet notes

For the simple case, run the validator on a host with a real public IP and pass that IP as
`--bind-address`.

Open these ports to the internet:

- `GOSSIP_PORT` (default `8001`)
- `DYNAMIC_PORT_RANGE` (default `8002-8020`)

If you run more than one validator on the same host, give each one unique values for:

- `RPC_PORT`
- `GOSSIP_PORT`
- `DYNAMIC_PORT_RANGE`
- `SOLANACDN_METRICS_ADDR`
- ledger / accounts / snapshots directories

If your host is behind NAT or needs more complex Agave networking flags, this helper may be too
simple. In that case, use `scripts/devnet/start-validator.sh` or pass extra Agave flags yourself.

## What `--fair` means here

In this fork, `--fair` is the maximum-protection mode. It only applies to transactions delivered
through the SolanaCDN fair-batch path.

That means:

- joining the cluster with this helper enables fair ordering on your leader slots
- normal public RPC `sendTransaction` is still not the fair path by itself

For the full fair-ordering contract, see [`solanacdn.md`](./solanacdn.md).

---
title: Devnet Deployment
sidebar_position: 60
sidebar_label: Devnet Deployment
pagination_label: Devnet Deployment
---

This repo includes a small `bash + systemd` bundle for running your patched validator in three
different ways:

- a brand new public devnet
- an additional validator or RPC node joining an existing devnet
- a small local multi-node cluster when you need this exact validator binary and startup path

This is an operator bundle, not a one-command localnet. If you only need a fast single-node program
dev environment, use `solana-test-validator` instead.

The bundle lives under `scripts/devnet/`:

- `create-genesis.sh` creates a brand new genesis ledger and generates missing keypairs.
- `join-fair-validator.sh` is the short path for joining an existing public fair devnet as a voting validator.
- `install-layout.sh` copies the helper scripts and `systemd` unit templates into standard host paths.
- `start-validator.sh` builds a readable `agave-validator` command line from env vars.
- `start-rpc.sh` is a thin wrapper around `start-validator.sh` with RPC defaults.
- `start-faucet.sh` runs the standard `solana-faucet` binary from an env file.
- `cluster-health.sh` checks local JSON-RPC health, and optionally SolanaCDN fair-ordering status.
- `env/*.example` files give you bootstrap, validator, RPC, and faucet templates.

## Host assumptions

These helpers assume:

- Linux with `systemd`
- an `agave` user and group already exist
- your patched `agave-validator` binary exists at `/opt/agave/bin/agave-validator`
- `solana-keygen`, `solana-genesis`, and `solana-ledger-tool` are installed if you will create a new cluster
- `solana-faucet` is installed if you will expose a faucet
- `curl` is installed if you will use `cluster-health.sh`

These helpers do **not**:

- install binaries
- create users
- open firewall ports
- provision keypairs for every node automatically beyond the bootstrap genesis flow

## First boot checklist

Before you try to start anything with `systemd`, confirm:

- `/opt/agave/bin/agave-validator` exists on that host
- `agave` user and group exist on that host
- `/etc/agave/devnet/` and `/etc/agave/keys/` exist on that host
- the node env file points at real keypair paths, not placeholder filenames
- voting validators that keep `FAIR_ENABLE=true` also have working SolanaCDN discovery configured
- each node on the same host has unique `RPC_PORT`, `GOSSIP_PORT`, `DYNAMIC_PORT_RANGE`, and storage directories
- if you enable SolanaCDN on multiple validators on one host, each has a unique `SOLANACDN_METRICS_ADDR`

Before enabling a service, render the command once and inspect it:

```bash
/opt/agave/devnet/start-validator.sh --env-file /etc/agave/devnet/validator-1.env --print-command
/opt/agave/devnet/start-rpc.sh --env-file /etc/agave/devnet/rpc-1.env --print-command
/opt/agave/devnet/start-faucet.sh --env-file /etc/agave/devnet/faucet-1.env --print-command
```

## Recommended layout

- Binaries: `/opt/agave/bin/`
- Devnet helper scripts: `/opt/agave/devnet/`
- Env files: `/etc/agave/devnet/<name>.env`
- Secret env files: `/etc/agave/devnet/<name>.secrets.env`
- Keypairs: `/etc/agave/keys/`
- State: `/var/lib/agave/<name>/`

## Role matrix

| Role | Example env files | Purpose | Fair / SolanaCDN default |
|---|---|---|---|
| Bootstrap genesis | `bootstrap.env` | Creates the first ledger and writes `cluster-info.env` | n/a |
| Bootstrap validator | `validator-1.env` | Starts the first voting validator on that genesis | on |
| Joining validator | `validator-N.env` | Adds another voting validator to an existing cluster | on |
| Public RPC | `rpc-1.env` | Serves public JSON-RPC without voting | off |
| Faucet | `faucet-1.env` | Serves `requestAirdrop` backing traffic | n/a |

The bootstrap machine usually needs **two** configs:

- `bootstrap.env` for `create-genesis.sh`
- `validator-1.env` for `agave-validator@validator-1`

## Install the helper bundle

Run this on every host that will run a validator, RPC, or faucet service:

```bash
sudo scripts/devnet/install-layout.sh
sudo systemctl daemon-reload
```

This copies scripts and unit files only. It does **not** install the validator binary, create the
`agave` user, or provision keys.

If you only want to join an existing public fair devnet, use
[`Join Public Fair Devnet`](./join-fair-devnet.md) instead of this longer operator guide.

## Scenario 1: Brand New Public Devnet

Use this path when you are creating a fresh cluster with your own genesis.

### 1. Copy the templates

On the bootstrap host:

```bash
sudo cp /etc/agave/devnet/examples/bootstrap.env.example /etc/agave/devnet/bootstrap.env
sudo cp /etc/agave/devnet/examples/faucet.env.example /etc/agave/devnet/faucet-1.env

sudo cp /etc/agave/devnet/examples/validator.env.example /etc/agave/devnet/validator-1.env
sudo cp /etc/agave/devnet/examples/validator.secrets.env.example /etc/agave/devnet/validator-1.secrets.env
```

On the other hosts:

```bash
sudo cp /etc/agave/devnet/examples/validator.env.example /etc/agave/devnet/validator-2.env
sudo cp /etc/agave/devnet/examples/validator.secrets.env.example /etc/agave/devnet/validator-2.secrets.env

sudo cp /etc/agave/devnet/examples/rpc.env.example /etc/agave/devnet/rpc-1.env
sudo cp /etc/agave/devnet/examples/rpc.secrets.env.example /etc/agave/devnet/rpc-1.secrets.env
```

### 2. Edit the bootstrap files

`bootstrap.env` controls genesis creation only. Edit:

- keypair paths
- `LEDGER_DIR`
- faucet and bootstrap lamports
- optional epoch / fee tuning

`validator-1.env` controls the first validator process. Edit:

- keypair paths
- ledger / accounts / snapshots directories
- gossip / RPC ports
- `KNOWN_VALIDATORS`
- SolanaCDN token or explicit POP/control discovery

For the **first** validator in a brand new cluster, leave these unset before first start:

- `ENTRYPOINTS`
- `EXPECTED_GENESIS_HASH`
- `EXPECTED_SHRED_VERSION`

### 3. Create the first ledger

Run once on the bootstrap host:

```bash
/opt/agave/devnet/create-genesis.sh --env-file /etc/agave/devnet/bootstrap.env
```

This writes `cluster-info.env` into the ledger directory. When `solana-ledger-tool` is available,
that file includes:

- `EXPECTED_GENESIS_HASH`
- `EXPECTED_SHRED_VERSION`
- bootstrap identity / vote / stake pubkeys
- faucet pubkey

### 4. Fill the joining-node configs

On `validator-2.env`, `validator-3.env`, and `rpc-1.env`, set:

- `ENTRYPOINTS` to one or more live gossip entrypoints, usually `validator-1` first
- `EXPECTED_GENESIS_HASH`
- `EXPECTED_SHRED_VERSION`
- `KNOWN_VALIDATORS`
- the correct keypair and storage paths for that node

For public devnet voting validators, keep:

- `FAIR_ENABLE=true`
- `SOLANACDN_ENABLE=true`

For public RPC nodes, the example leaves those off by default.

### 5. Start in order

```bash
sudo systemctl enable --now agave-validator@validator-1
sudo systemctl enable --now agave-validator@validator-2
sudo systemctl enable --now agave-rpc@rpc-1
sudo systemctl enable --now agave-faucet@faucet-1
```

Point `RPC_FAUCET_ADDRESS` on the public RPC nodes at the faucet, for example
`faucet.example.org:9900`.

## Scenario 2: Join An Existing Public Devnet

Use this path when genesis already exists and you only want to add one more node.

Do **not** run `create-genesis.sh`.

### Join as a voting validator

Copy the validator examples:

```bash
sudo cp /etc/agave/devnet/examples/validator.env.example /etc/agave/devnet/validator-3.env
sudo cp /etc/agave/devnet/examples/validator.secrets.env.example /etc/agave/devnet/validator-3.secrets.env
```

Edit these values before start:

- `IDENTITY_KEYPAIR`
- `VOTE_ACCOUNT_KEYPAIR`
- `LEDGER_DIR`, `ACCOUNTS_DIR`, `SNAPSHOTS_DIR`
- `ENTRYPOINTS`
- `KNOWN_VALIDATORS`
- `EXPECTED_GENESIS_HASH`
- `EXPECTED_SHRED_VERSION`
- `SOLANACDN_API_TOKEN` or explicit POP/control discovery

Then start:

```bash
sudo systemctl enable --now agave-validator@validator-3
```

### Join as a public RPC node

Copy the RPC examples:

```bash
sudo cp /etc/agave/devnet/examples/rpc.env.example /etc/agave/devnet/rpc-2.env
sudo cp /etc/agave/devnet/examples/rpc.secrets.env.example /etc/agave/devnet/rpc-2.secrets.env
```

Edit these values before start:

- `IDENTITY_KEYPAIR`
- `LEDGER_DIR`, `ACCOUNTS_DIR`, `SNAPSHOTS_DIR`
- `ENTRYPOINTS`
- `KNOWN_VALIDATORS`
- `EXPECTED_GENESIS_HASH`
- `EXPECTED_SHRED_VERSION`
- `RPC_FAUCET_ADDRESS` if this RPC should expose `requestAirdrop`

Then start:

```bash
sudo systemctl enable --now agave-rpc@rpc-2
```

## Scenario 3: Single-Host Local Multi-Node Cluster

This works for local testing when you specifically need your patched validator path. It is heavier
than `solana-test-validator`, but it does let you exercise the same scripts and systemd units.

### Local rules

- every node needs unique `LEDGER_DIR`, `ACCOUNTS_DIR`, and `SNAPSHOTS_DIR`
- every node needs unique `RPC_PORT`, `GOSSIP_PORT`, and `DYNAMIC_PORT_RANGE`
- every non-bootstrap node should point `ENTRYPOINTS` at the bootstrap validator, usually `127.0.0.1:8001`
- if you do **not** have SolanaCDN infrastructure locally, set `SOLANACDN_ENABLE=false` and `FAIR_ENABLE=false` on all local validators
- if you do enable SolanaCDN on multiple local validators, each validator needs a unique `SOLANACDN_METRICS_ADDR`
- the current `solana-faucet` binary listens on fixed TCP port `9900`, so you can run only one faucet per host

### Minimal local layout

One simple single-host layout is:

- `validator-1`: `RPC_PORT=8899`, `GOSSIP_PORT=8001`, `DYNAMIC_PORT_RANGE=8002-8020`
- `validator-2`: `RPC_PORT=8898`, `GOSSIP_PORT=8011`, `DYNAMIC_PORT_RANGE=8012-8030`
- `rpc-1`: `RPC_PORT=8897`, `GOSSIP_PORT=8021`, `DYNAMIC_PORT_RANGE=8022-8040`
- `faucet-1`: fixed `:9900`

For `validator-1`, create genesis with `bootstrap.env`, then start from `validator-1.env` with:

- `ENTRYPOINTS=`
- `EXPECTED_GENESIS_HASH=`
- `EXPECTED_SHRED_VERSION=`

For `validator-2` and `rpc-1`, use:

- `ENTRYPOINTS="127.0.0.1:8001"`
- `EXPECTED_GENESIS_HASH` from `cluster-info.env`
- `EXPECTED_SHRED_VERSION` from `cluster-info.env`

### Concrete single-host example

Use this as a starting point if everything is on one Linux machine:

| Node | RPC_PORT | GOSSIP_PORT | DYNAMIC_PORT_RANGE | ENTRYPOINTS | SOLANACDN / FAIR |
|---|---|---|---|---|---|
| `validator-1` | `8899` | `8001` | `8002-8020` | empty on first start | off for pure local testing |
| `validator-2` | `8898` | `8011` | `8012-8030` | `127.0.0.1:8001` | off for pure local testing |
| `rpc-1` | `8897` | `8021` | `8022-8040` | `127.0.0.1:8001` | off by default |
| `faucet-1` | fixed `9900` | n/a | n/a | n/a | n/a |

For that layout:

- set `SOLANACDN_ENABLE=false` and `FAIR_ENABLE=false` in `validator-1.env` and `validator-2.env`
- keep `RPC_FAUCET_ADDRESS=127.0.0.1:9900` in `rpc-1.env`
- leave `validator-1.env` `ENTRYPOINTS`, `EXPECTED_GENESIS_HASH`, and `EXPECTED_SHRED_VERSION` empty before the first start
- set `validator-2.env` and `rpc-1.env` `ENTRYPOINTS="127.0.0.1:8001"` after genesis is created
- copy `EXPECTED_GENESIS_HASH` and `EXPECTED_SHRED_VERSION` from `cluster-info.env` into `validator-2.env` and `rpc-1.env`

## Health checks

Local validator:

```bash
/opt/agave/devnet/cluster-health.sh --rpc-url http://127.0.0.1:8899 --metrics-url http://127.0.0.1:9100 --require-fair
```

Plain RPC only:

```bash
/opt/agave/devnet/cluster-health.sh --rpc-url http://127.0.0.1:8899
```

The health script always verifies:

- `/health` returns `ok`
- `getEpochInfo` succeeds over JSON-RPC

When `--metrics-url` is provided, it also verifies:

- `/solanacdn/status` is reachable
- `tx_fair_ordering=true` when `--require-fair` is set

## Placeholder addresses

The example `203.0.113.x` and `198.51.100.x` addresses are **RFC 5737 documentation-only
placeholder ranges**. They are intentionally fake and must be replaced before use.

The example hostnames such as `faucet.devnet.invalid` and `pop.devnet.example.com` are also
intentional placeholders.

## Operational notes

- Keep voting validators on private RPC (`PRIVATE_RPC=true`) and expose only dedicated RPC nodes publicly.
- Keep `FAIR_ENABLE=true` and `SOLANACDN_ENABLE=true` on every voting validator so scheduler and SolanaCDN stay aligned.
- The example public RPC config intentionally leaves `SOLANACDN_ENABLE=false` and `FAIR_ENABLE=false`; turn them on there only if you have a specific reason.
- Set `RPC_FAUCET_ADDRESS` only on the public RPC nodes that should expose `requestAirdrop`.
- Treat the example env files as templates, not production defaults. Adjust ports, disk paths, and `KNOWN_VALIDATORS` for your topology.
- For an early public devnet, publish a reset policy and expect to rotate the cluster regularly while the fairness path is still experimental.

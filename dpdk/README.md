# agave-dpdk (experimental)

This crate provides an **experimental** DPDK dataplane for Agave as an alternative to the existing
kernel UDP/XDP paths.

## What it supports (today)

- **TPU UDP RX** via DPDK (transaction / forwards / vote)
- **TPU QUIC RX/TX** via DPDK (Quinn abstract UDP socket)
- **TVU shred RX** via DPDK
- **Turbine shred TX** (broadcast + retransmit) via DPDK

IPv4-only for now.

## Build

DPDK is only required when the crate feature `dpdk` is enabled.

Ubuntu install example:

```bash
sudo apt-get update
sudo apt-get install -y libdpdk-dev dpdk dpdk-dev
```

Example:

```bash
cargo build -p agave-validator --features dpdk
```

## Probe (without running a validator)

You can verify that DPDK can initialize the port and read link state with:

```bash
cargo run -p agave-dpdk --features dpdk --bin agave-dpdk-probe -- --devargs 0000:01:00.0
```

To run the same fail-fast checks as the validator (link-up wait + gateway ARP probe), use
`--full-init` (requires `--ip`):

```bash
cargo run -p agave-dpdk --features dpdk --bin agave-dpdk-probe -- \
  --devargs 0000:01:00.0 \
  --full-init \
  --ip 203.0.113.10 --prefix-len 32 --gateway 203.0.113.1
```

Note: when probing a PCI device by BDF, it must already be bound to `vfio-pci` (or a supported UIO
PMD), otherwise probe will fail.

## Run (validator flags)

The validator has hidden/experimental flags to configure DPDK:

- `--experimental-dpdk`
- `--experimental-dpdk-dry-run` (initialize DPDK, print resolved port info, then exit)
- `--experimental-dpdk-devargs <dpdk_devargs>` (passed to EAL as `-a <devargs>`)
- `--experimental-dpdk-ip <ipv4>`
- `--experimental-dpdk-prefix-len <0-32>`
- `--experimental-dpdk-gateway <ipv4>` (required for `/31` and `/32`, and generally needed for off-subnet peers; may be inferred from the kernel default route for SR-IOV VFs)
- `--experimental-dpdk-gateway-mac <mac>` (optional: static gateway MAC; skips ARP probing)
- `--experimental-dpdk-eal-arg <arg>` (repeatable; additional EAL args)
- Tuning (all optional):
  - `--experimental-dpdk-link-up-timeout-secs <u64>`
  - `--experimental-dpdk-cpu-cores <CPU_LIST>`
  - `--experimental-dpdk-io-threads <u16>`
  - `--experimental-dpdk-rx-desc <u16>`
  - `--experimental-dpdk-tx-desc <u16>`
  - `--experimental-dpdk-mbuf-count <u32>`
  - `--experimental-dpdk-mbuf-data-size <u16>`
  - `--experimental-dpdk-shred-tx-channel-cap <usize>`
  - `--experimental-dpdk-quic-tx-channel-cap <usize>`
  - `--experimental-dpdk-quic-rx-channel-cap <usize>`

Note: these flags are hidden from `--help` by default. To display them, run:
`SOLANA_NO_HIDDEN_CLI_ARGS=1 agave-validator --help`.

## Operational notes

- A **full DPDK PMD** (vfio/uio) generally requires dedicating a **NIC/VF** to DPDK (the kernel
  cannot use that interface at the same time). Plan for a separate interface and/or IP for TPU/TVU.
- If you only have a single physical NIC, SR-IOV (PF in kernel + VF in vfio-pci) is the typical way
  to keep management/gossip/RPC on the kernel while running TPU/TVU on DPDK.
- Gossip/RPC still use kernel sockets, so plan for a kernel-managed interface that is reachable for
  the roles you enable.
- TPU/TVU/turbine traffic handled by DPDK bypasses the kernel network stack (host firewall,
  `tc`, and `tcpdump` on the kernel interface do not see/enforce it). Secure/observe at the
  upstream switch/router and via DPDK metrics.
- DPDK does **not** use the kernel routing table; configure routing via
  `--experimental-dpdk-prefix-len`/`--experimental-dpdk-gateway`.
- DPDK EAL/port initialization is **process-lifetime**. If you change DPDK settings (devargs/EAL
  args/queue counts), restart the process rather than trying to reinitialize within the same
  process.
- Ensure hugepages, IOMMU/vfio, and `libdpdk` are configured on the host before enabling this.
- Make DPDK host setup persistent across reboots (IOMMU, hugepages + hugetlbfs mount, vfio binding,
  memlock limits).
- If using `vfio-pci`, ensure all devices in the same IOMMU group are also bound to `vfio-pci`
  (helper: `scripts/dpdk-setup.sh --bind-iommu-group`).
- Ensure `memlock` is sufficiently high/unlimited (systemd: `LimitMEMLOCK=infinity`; shell:
  `ulimit -l unlimited`).
- If running as non-root, ensure `/dev/vfio/<group>` is accessible to the validator user (udev rule /
  group ownership) and that the hugepage mount is writable.
- When running as a non-root system service, DPDK EAL can fail if it cannot create a runtime
  directory (often due to `XDG_RUNTIME_DIR` not being set). If you hit EAL init failures related to
  runtime directories, pass `--experimental-dpdk-eal-arg --in-memory`.
- For a quick host sanity check, use `scripts/dpdk-status.sh` with `--iface` or `--pci`.
- `scripts/dpdk-status.sh --check` exits non-zero if the host/device is not ready for full DPDK
  (useful for `ExecStartPre=` in systemd).
- For optional host setup (hugepages, SR-IOV, vfio binding), use `scripts/dpdk-setup.sh` (defaults to
  printing actions unless `--apply` is provided; can be disruptive).
- DPDK I/O counters are reported under the `dpdk_io` datapoint (once/sec from queue 0).
- `--experimental-dpdk-io-threads > 1` enables multi-queue **RX via RSS**; TX uses up to
  `min(io_threads, NIC/PMD max_tx_queues)` queues (one TX queue per TX thread).
- DPDK I/O threads are registered with EAL via `rte_thread_register()`. If you override EAL lcore
  args (`-l` / `-c` / `--lcores`), ensure enough lcores are available for the main thread plus
  `--experimental-dpdk-io-threads` worker threads.
- TX assumes untagged Ethernet (no VLAN insertion). If your uplink uses VLAN tagging, configure the
  switchport/network as access/untagged for the DPDK NIC/VF.
- UDP payloads larger than Solana’s `PACKET_DATA_SIZE` are dropped (no benefit from jumbo payloads
  for TPU/TVU).

Startup checks (fail-fast):

- Errors if the DPDK `local_ip` is already configured on a kernel interface (ARP/route conflicts)
- Errors if a PCI device is not bound to `vfio-pci`/UIO (for PCI devargs)
- Errors if the DPDK link does not come up within `--experimental-dpdk-link-up-timeout-secs` (default: 10 seconds)
- Errors if the configured gateway cannot be resolved via ARP (unless `--experimental-dpdk-gateway-mac` is set)
- Errors if hugepages are not configured/available (ex: `HugePages_Total=0` or `HugePages_Free=0`)

DPDK is mutually exclusive with retransmit XDP (`--experimental-retransmit-xdp-cpu-cores`).

DPDK is not compatible with `--restricted-repair-only-mode` (which removes TPU/TVU ports).

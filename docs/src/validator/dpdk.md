---
title: DPDK Dataplane (Experimental)
sidebar_position: 20
sidebar_label: DPDK (Experimental)
---

Agave includes an **experimental** DPDK dataplane that can replace the kernel UDP path (and act as
an alternative to XDP) for validator-critical networking.

This is intended for deployments that can dedicate a **NIC/VF** to a full DPDK PMD (vfio/uio).

## What it supports

- **TPU UDP RX** (transactions / forwards / vote)
- **TPU QUIC RX/TX** via Quinn “abstract UDP socket”
- **TVU shred RX**
- **Turbine shred TX** (broadcast + retransmit)

IPv4-only for now.

## Hardware + host requirements

- Linux host with `libdpdk` installed
- Hugepages configured (typically 2MiB hugepages mounted at `/dev/hugepages`)
- Sufficient `memlock` limit (systemd: `LimitMEMLOCK=infinity`; shell: `ulimit -l unlimited`)
- Note: TPU/TVU/turbine traffic handled by DPDK bypasses the kernel network stack (host firewall,
  `tc`, and `tcpdump` on the kernel interface do not see/enforce it).
- If running as a non-root system service, ensure `/dev/hugepages` and `/dev/vfio/<group>` are
  readable/writable by the service user (or run as root)
- If running as a non-root system service and DPDK EAL fails due to runtime directory creation,
  add `--experimental-dpdk-eal-arg --in-memory`
- IOMMU enabled for vfio (recommended/required on most hosts)
- A **dedicated** NIC/VF bound to `vfio-pci` (or a supported UIO PMD)
- For `vfio-pci`, all devices in the same **IOMMU group** must also be bound to `vfio-pci`

### Installing DPDK (Ubuntu)

On Ubuntu, the following packages are typically sufficient for building/running `--features dpdk`:

```bash
sudo apt-get update
sudo apt-get install -y libdpdk-dev dpdk dpdk-dev
```

### Non-root (systemd) notes

When running the validator as a non-root systemd service user (common in production):

- Set `LimitMEMLOCK=infinity` in your unit file.
- Ensure the service user can open `/dev/vfio/<group>` (or run as root). `scripts/dpdk-status.sh`
  will warn if the current user cannot open the VFIO group device.
- Ensure the service user can create files under the hugepage mount (commonly `/dev/hugepages`).
  This is often done by mounting hugetlbfs with `mode=` and `gid=` options.

Example udev rule to grant group access to VFIO group devices:

```bash
# /etc/udev/rules.d/99-vfio.rules
SUBSYSTEM=="vfio", GROUP="vfio", MODE="0660"
```

Then:

- Add your validator user to the group (ex: `sudo usermod -aG vfio sol`)
- Reload udev rules (`sudo udevadm control --reload-rules && sudo udevadm trigger`)
- Re-check with `scripts/dpdk-status.sh --pci <BDF>` as the validator user

Use the helper script to inspect a candidate interface/device:

```bash
scripts/dpdk-status.sh --iface eno1
scripts/dpdk-status.sh --pci 0000:01:00.0
```

For automation/systemd gating, `--check` exits non-zero when the host/device is not ready for full
DPDK (vfio/uio):

```bash
scripts/dpdk-status.sh --pci 0000:01:00.0 --check
```

Example `ExecStartPre=` gate (recommended):

```ini
[Service]
LimitMEMLOCK=infinity
ExecStartPre=/path/to/scripts/dpdk-status.sh --pci 0000:01:00.0 --check
ExecStartPre=/path/to/agave-dpdk-probe --devargs 0000:01:00.0 --full-init --ip 203.0.113.10 --prefix-len 32 --gateway 203.0.113.1
```

### Persistence (after reboot)

Most DPDK prerequisites are **host configuration** and may revert after reboot unless you make them
persistent:

- **IOMMU:** enable in BIOS and ensure it is enabled in the kernel command line (ex:
  `intel_iommu=on iommu=pt` or `amd_iommu=on iommu=pt`).
- **Hugepages:** reserve hugepages and mount `hugetlbfs` (commonly `/dev/hugepages`) at boot.
- **VFIO binding:** ensure your dedicated NIC/VF is bound to `vfio-pci` on boot (udev
  `driver_override`, `dpdk-devbind.py`, or a boot-time service/`ExecStartPre=` for a dedicated
  dataplane device).
- **memlock:** set `LimitMEMLOCK=infinity` in the validator systemd unit.

If you use `ExecStartPre=` to bind devices, only do so for a **dedicated** dataplane NIC/VF; binding
a management NIC will drop network access.

For host setup (hugepages, SR-IOV VF creation, vfio binding), there is also a helper that defaults
to printing the actions it would take unless `--apply` is provided:

```bash
# Print what would happen (safe)
scripts/dpdk-setup.sh --iface eno1 --show-vfs

# Create 1 VF on a PF (DISRUPTIVE)
sudo scripts/dpdk-setup.sh --iface eno1 --sriov-numvfs 1 --apply

# Bind VF 0 to vfio-pci (DISRUPTIVE)
sudo scripts/dpdk-setup.sh --iface eno1 --vf-index 0 --bind-vf --bind-iommu-group --apply
```

You can also probe DPDK port initialization directly (without running a full validator):

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

## Deployment model (recommended)

The safest production model is:

- **Management NIC** (kernel driver): SSH, RPC, gossip, OS updates
- **DPDK NIC** (vfio/uio): TPU/TVU + turbine traffic
- Assign a **separate routable IPv4** to the DPDK NIC and advertise that IP for TPU/TVU

Sharing the same IP between kernel and DPDK is not supported/recommended because the kernel network
stack will also respond to ARP and can black-hole traffic.

Note: gossip/RPC still use kernel sockets, so the management NIC must be reachable for the roles you
enable (gossip always; RPC if you expose it).

## SR-IOV (single physical port option)

If you only have one “public” physical NIC, the typical production approach is **SR-IOV**:

- Keep the **PF** in the kernel for management/gossip/RPC
- Create a **VF** and bind it to `vfio-pci` for DPDK
- Use a **second public IPv4** (often a routed `/32`) for the VF/DPDK dataplane

Whether SR-IOV is available depends on your NIC/firmware. `scripts/dpdk-status.sh --iface <pf>` will
report `sriov_totalvfs` when supported and list existing VFs.

Note: many SR-IOV setups also require enabling VF “trust” / disabling spoof-check on the PF (driver
dependent), e.g. `ip link set <pf> vf <n> trust on` / `spoofchk off`.

## Binding a NIC/VF to vfio (disruptive)

Binding detaches the device from the kernel and can drop network access. Prefer doing this via an
out-of-band console or on a non-management interface.

Typical (sysfs) flow:

```bash
sudo modprobe vfio-pci

# Replace with your PCI BDF and IDs from scripts/dpdk-status.sh output
echo 0x14e4 0x1750 | sudo tee /sys/bus/pci/drivers/vfio-pci/new_id
echo 0000:41:00.0 | sudo tee /sys/bus/pci/devices/0000:41:00.0/driver/unbind
echo 0000:41:00.0 | sudo tee /sys/bus/pci/drivers/vfio-pci/bind
```

You can also use DPDK’s `dpdk-devbind.py` if installed on the host.

Alternatively, `scripts/dpdk-setup.sh` can bind the target PCI device **and all devices in its IOMMU
group** (recommended for vfio) via `--bind-iommu-group`.

## Quickstart (dedicated DPDK NIC + routed `/32`)

This is the most common production model: keep your normal “management” NIC in the kernel for
SSH/gossip/RPC, and dedicate a second NIC/port (or SR-IOV VF) to DPDK for TPU/TVU + turbine.

You will need a **second public IPv4** that is **L2-reachable** on the DPDK NIC/VF (your provider must
route/ARP that IP to the MAC of the DPDK interface). A private/CGNAT IP generally will not work for
mainnet TPU/TVU.

1) Build the validator with DPDK enabled:

```bash
cargo build -p agave-validator --features dpdk
```

2) Pick the DPDK device (PCI BDF) and verify the host is DPDK-ready:

```bash
scripts/dpdk-status.sh --pci 0000:01:00.0
scripts/dpdk-status.sh --pci 0000:01:00.0 --check
```

3) Bind the DPDK device to `vfio-pci` (DISRUPTIVE; do this only on a non-management interface):

```bash
sudo scripts/dpdk-setup.sh --pci 0000:01:00.0 --bind-pf --bind-iommu-group --apply
```

4) Ensure hugepages are configured/mounted (example values; tune to your host):

```bash
sudo scripts/dpdk-setup.sh --pci 0000:01:00.0 --hugepages2m 1024 --mount-hugepages --apply
```

5) Probe DPDK init + link + gateway reachability (use your routed `/32` and gateway):

```bash
cargo run -p agave-dpdk --features dpdk --bin agave-dpdk-probe -- \
  --devargs 0000:01:00.0 \
  --full-init \
  --ip 203.0.113.10 --prefix-len 32 --gateway 203.0.113.1
```

6) Start the validator with DPDK enabled (showing only the DPDK-related flags):

```bash
agave-validator \
  --public-ip <MANAGEMENT_KERNEL_IP> \
  --experimental-dpdk \
  --experimental-dpdk-devargs 0000:01:00.0 \
  --experimental-dpdk-ip 203.0.113.10 \
  --experimental-dpdk-prefix-len 32 \
  --experimental-dpdk-gateway 203.0.113.1
```

Notes:

- `--public-ip` should remain your management/kernel IP. The validator will advertise TPU/TVU on
  `--experimental-dpdk-ip`.
- For `/31` and `/32`, `--experimental-dpdk-gateway` is required.
- If your network filters ARP, add `--experimental-dpdk-gateway-mac <MAC>` to skip ARP probing.
- DPDK does **not** use the kernel routing table; changes to `ip route` do not affect DPDK. Configure
  routing via `--experimental-dpdk-prefix-len`/`--experimental-dpdk-gateway`.
- Use `--experimental-dpdk-dry-run` to initialize DPDK and exit after printing resolved port info.

## Running the validator with DPDK

Build with the feature flag:

```bash
cargo build -p agave-validator --features dpdk
```

Note: DPDK EAL/port initialization is **process-lifetime**. If you change DPDK settings
(devargs/EAL args/queue counts), restart the validator rather than trying to reinitialize within the
same process.

Then run with the hidden/experimental flags:

- `--experimental-dpdk`
- `--experimental-dpdk-devargs <DEVARGS>` (ex: `0000:01:00.0`)
- `--experimental-dpdk-ip <IPV4>`
- `--experimental-dpdk-prefix-len <0-32>`
- `--experimental-dpdk-gateway <IPV4>` (required for `/31` and `/32`, and generally needed for off-subnet peers; may be inferred from the kernel default route for SR-IOV VFs)
- `--experimental-dpdk-gateway-mac <MAC>` (optional: static gateway MAC; skips ARP probing)
- Optional tuning:
  - `--experimental-dpdk-link-up-timeout-secs <u64>`
  - `--experimental-dpdk-cpu-cores <CPU_LIST>`
  - `--experimental-dpdk-io-threads <u16>` (enables RSS RX when `> 1`)
  - `--experimental-dpdk-rx-desc <u16>`
  - `--experimental-dpdk-tx-desc <u16>`
  - `--experimental-dpdk-mbuf-count <u32>`
  - `--experimental-dpdk-mbuf-data-size <u16>`

Note: these flags are hidden from `--help` by default. To display them, run:
`SOLANA_NO_HIDDEN_CLI_ARGS=1 agave-validator --help`.

Use `--experimental-dpdk-dry-run` to validate that the NIC is detected and link is up before
starting a full validator.

### Startup checks (fail-fast)

When DPDK is enabled, startup will fail early if:

- The DPDK `local_ip` is already configured on a kernel interface (to avoid ARP/route conflicts)
- The PCI device is not bound to `vfio-pci`/UIO (for PCI devargs)
- The DPDK link does not come up within `--experimental-dpdk-link-up-timeout-secs` (default: 10 seconds)
- The configured gateway cannot be resolved via ARP (unless `--experimental-dpdk-gateway-mac` is set)
- Hugepages are not configured/available (ex: `HugePages_Total=0` or `HugePages_Free=0`)

DPDK is mutually exclusive with retransmit XDP (`--experimental-retransmit-xdp-cpu-cores`).

DPDK is not compatible with `--restricted-repair-only-mode` (which removes TPU/TVU ports).

## Observability

When DPDK is enabled, TPU-side receive metrics are reported under:

- `tpu_receiver_dpdk`
- `tpu_forwards_receiver_dpdk`
- `tpu_vote_receiver_dpdk`

DPDK I/O health counters are reported once/sec under:

- `dpdk_io`
  - `quic_rx_enqueued` / `quic_rx_dropped` (bounded QUIC RX queue drops)
  - `arp_event_dropped` (non-zero RX queue ARP forwarding drops)
  - `tx_mbuf_alloc_fail` / `tx_mbuf_append_fail` / `tx_build_fail`
  - `tx_dropped_no_arp` / `tx_dropped_no_gateway`
  - `tx_burst_unsent` (frames that `tx_burst` did not send)

Link up/down changes are logged by the DPDK I/O thread.

## Current limitations

- IPv4-only
- QUIC GSO is not supported (Quinn will use non-GSO sends)
- TX assumes untagged Ethernet (no VLAN insertion). If your uplink uses VLAN tagging, configure the
  switchport/network as access/untagged for the DPDK NIC/VF.
- UDP payloads larger than Solana’s `PACKET_DATA_SIZE` are dropped (no benefit from jumbo payloads
  for TPU/TVU).
- TX uses up to `min(io_threads, NIC/PMD max_tx_queues)` queues; if `io_threads` exceeds that,
  extra I/O threads are RX-only

## Troubleshooting

Start with:

```bash
scripts/dpdk-status.sh --pci <BDF>
cargo run -p agave-dpdk --features dpdk --bin agave-dpdk-probe -- \
  --devargs <BDF> \
  --full-init \
  --ip <IPV4> --prefix-len <0-32> --gateway <IPV4>
```

### DPDK IP is private / not peer-reachable

If you use an RFC1918/CGNAT/link-local address (ex: `10.0.0.0/8`, `100.64.0.0/10`, `169.254.0.0/16`)
for `--experimental-dpdk-ip`, mainnet TPU/TVU peers generally cannot reach it. You typically need a
provider-routed public IPv4 (often a routed `/32`) on the DPDK interface.

### Link stays down

- Confirm the correct device/BDF and driver binding: `scripts/dpdk-status.sh --pci <BDF>`
- Check physical link/cabling/switchport; `agave-dpdk-probe` prints `link: up/down`.

### Gateway ARP probe fails

- Set `--experimental-dpdk-gateway` (required for `/31` and `/32`).
- If ARP is filtered on your network, set `--experimental-dpdk-gateway-mac <MAC>` to skip ARP
  probing.
- Ensure the gateway is reachable on the L2 segment and that the DPDK interface is actually
  connected to the network you expect.

### `local_ip ... already configured on kernel interface(s)`

DPDK does not support sharing the same IP with the kernel network stack. Use a separate IPv4 for
the DPDK dataplane, and ensure the DPDK IP is not assigned to any kernel interface.

### Hugepages / memlock errors

- Ensure hugepages are reserved and hugetlbfs is mounted (commonly `/dev/hugepages`).
- Ensure memlock is high/unlimited (systemd: `LimitMEMLOCK=infinity`; shell: `ulimit -l unlimited`).

### VFIO access or IOMMU group errors

- Run as root or ensure the service user can open `/dev/vfio/<group>` read/write (udev rule / group
  ownership).
- For `vfio-pci`, ensure **all devices in the same IOMMU group** are also bound to `vfio-pci`
  (helper: `scripts/dpdk-setup.sh --bind-iommu-group`).

### RSS not supported (`ENOTSUP`) with `--experimental-dpdk-io-threads > 1`

Some NIC/PMD combos do not support RSS. Re-run with `--experimental-dpdk-io-threads 1` or use an
RSS-capable NIC/PMD.

### EAL init fails under systemd (non-root)

If EAL init fails due to runtime directory creation, pass `--experimental-dpdk-eal-arg --in-memory`
or ensure `XDG_RUNTIME_DIR` is set to a writable directory for the service user.

use {
    assert_cmd::prelude::*,
    solana_keypair::{write_keypair_file, Keypair},
    std::{fs, process::Command},
    tempfile::TempDir,
};

#[test]
fn test_use_the_same_path_for_accounts_and_snapshots() {
    let temp_dir = TempDir::new().unwrap();
    let temp_dir_path = temp_dir.path();

    let id_json_path = temp_dir_path.join("id.json");
    let id_json_str = id_json_path.to_str().unwrap();

    let keypair = Keypair::new();
    write_keypair_file(&keypair, id_json_str).unwrap();

    let temp_dir_str = temp_dir_path.to_str().unwrap();

    let mut cmd = Command::cargo_bin(env!("CARGO_PKG_NAME")).unwrap();
    cmd.args([
        "--identity",
        id_json_str,
        "--log",
        "-",
        "--no-voting",
        "--accounts",
        temp_dir_str,
        "--snapshots",
        temp_dir_str,
    ]);
    cmd.assert().failure().stderr(predicates::str::contains(
        "the --accounts and --snapshots paths must be unique",
    ));
}

#[test]
#[cfg(not(feature = "dpdk"))]
fn test_experimental_dpdk_requires_build_feature() {
    let temp_dir = TempDir::new().unwrap();
    let temp_dir_path = temp_dir.path();

    let id_json_path = temp_dir_path.join("id.json");
    let id_json_str = id_json_path.to_str().unwrap();
    write_keypair_file(&Keypair::new(), id_json_str).unwrap();

    let ledger_path = temp_dir_path.join("ledger");
    fs::create_dir_all(&ledger_path).unwrap();
    let ledger_str = ledger_path.to_str().unwrap();

    let mut cmd = Command::cargo_bin(env!("CARGO_PKG_NAME")).unwrap();
    cmd.args([
        "--identity",
        id_json_str,
        "--ledger",
        ledger_str,
        "--log",
        "-",
        "--no-voting",
        "--experimental-dpdk",
        "--experimental-dpdk-devargs",
        "0000:01:00.0",
        "--experimental-dpdk-ip",
        "203.0.113.10",
    ]);
    cmd.assert().failure().stderr(predicates::str::contains(
        "--experimental-dpdk requires agave-validator built with `--features dpdk`",
    ));
}

#[test]
#[cfg(feature = "dpdk")]
fn test_experimental_dpdk_incompatible_with_restricted_repair_only_mode() {
    let temp_dir = TempDir::new().unwrap();
    let temp_dir_path = temp_dir.path();

    let id_json_path = temp_dir_path.join("id.json");
    let id_json_str = id_json_path.to_str().unwrap();
    write_keypair_file(&Keypair::new(), id_json_str).unwrap();

    let ledger_path = temp_dir_path.join("ledger");
    fs::create_dir_all(&ledger_path).unwrap();
    let ledger_str = ledger_path.to_str().unwrap();

    let mut cmd = Command::cargo_bin(env!("CARGO_PKG_NAME")).unwrap();
    cmd.args([
        "--identity",
        id_json_str,
        "--ledger",
        ledger_str,
        "--log",
        "-",
        "--no-voting",
        "--restricted-repair-only-mode",
        "--experimental-dpdk",
        "--experimental-dpdk-devargs",
        "0000:01:00.0",
        "--experimental-dpdk-ip",
        "203.0.113.10",
    ]);
    cmd.assert().failure().stderr(predicates::str::contains(
        "--experimental-dpdk is not compatible with --restricted-repair-only-mode",
    ));
}

#[test]
#[cfg(feature = "dpdk")]
fn test_experimental_dpdk_incompatible_with_retransmit_xdp() {
    let temp_dir = TempDir::new().unwrap();
    let temp_dir_path = temp_dir.path();

    let id_json_path = temp_dir_path.join("id.json");
    let id_json_str = id_json_path.to_str().unwrap();
    write_keypair_file(&Keypair::new(), id_json_str).unwrap();

    let ledger_path = temp_dir_path.join("ledger");
    fs::create_dir_all(&ledger_path).unwrap();
    let ledger_str = ledger_path.to_str().unwrap();

    let mut cmd = Command::cargo_bin(env!("CARGO_PKG_NAME")).unwrap();
    cmd.args([
        "--identity",
        id_json_str,
        "--ledger",
        ledger_str,
        "--log",
        "-",
        "--no-voting",
        "--experimental-retransmit-xdp-cpu-cores",
        "0",
        "--experimental-dpdk",
        "--experimental-dpdk-devargs",
        "0000:01:00.0",
        "--experimental-dpdk-ip",
        "203.0.113.10",
    ]);
    cmd.assert().failure().stderr(predicates::str::contains(
        "--experimental-dpdk is not compatible with --experimental-retransmit-xdp-cpu-cores",
    ));
}

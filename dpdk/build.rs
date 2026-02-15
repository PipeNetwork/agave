fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os != "linux" {
        return;
    }

    // Only require libdpdk when the crate feature is enabled, so the workspace can
    // build on hosts without DPDK installed.
    if std::env::var_os("CARGO_FEATURE_DPDK").is_none() {
        return;
    }

    println!("cargo:rerun-if-changed=src/ffi.c");

    let lib = match pkg_config::Config::new().cargo_metadata(true).probe("libdpdk") {
        Ok(lib) => lib,
        Err(err) => {
            panic!(
                "pkg-config could not find libdpdk ({err}). Install DPDK development packages \
                 and ensure `pkg-config --cflags libdpdk` works, or build without `--features \
                 dpdk`."
            );
        }
    };

    let mut build = cc::Build::new();
    build.file("src/ffi.c");

    // DPDK's pkg-config exposes important compile flags (e.g. `-include rte_config.h`,
    // `-march=...`) via `--cflags`. The `pkg-config` crate doesn't currently surface these
    // directly, so we shell out to `pkg-config` to mirror the recommended CFLAGS.
    let cflags = std::process::Command::new("pkg-config")
        .args(["--cflags", "libdpdk"])
        .output()
        .expect("failed to run pkg-config --cflags libdpdk");
    if !cflags.status.success() {
        panic!(
            "pkg-config --cflags libdpdk failed: {}",
            String::from_utf8_lossy(&cflags.stderr)
        );
    }
    let cflags = String::from_utf8_lossy(&cflags.stdout);
    let mut iter = cflags.split_whitespace().peekable();
    while let Some(flag) = iter.next() {
        if let Some(inc) = flag.strip_prefix("-I") {
            build.include(inc);
            continue;
        }
        if flag == "-include" {
            let header = iter
                .next()
                .expect("pkg-config returned `-include` without a header");
            build.flag("-include");
            build.flag(header);
            continue;
        }
        build.flag(flag);
    }

    for include_path in lib.include_paths {
        build.include(include_path);
    }
    for (name, value) in lib.defines {
        build.define(&name, value.as_deref());
    }
    build.compile("agave_dpdk_ffi");
}

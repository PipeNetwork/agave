use std::{net::Ipv4Addr, process::ExitCode};

fn usage() {
    eprintln!(
        r#"agave-dpdk-probe

Usage:
  agave-dpdk-probe --devargs <DEVARGS> [options]

Options:
  --devargs <DEVARGS>         DPDK devargs (ex: '0000:3b:00.1')
  --full-init                 Perform full DPDK init (link-up wait + gateway ARP probe) and exit
  --io-threads <N>            RX queue / I/O thread count (default: 1)
  --rx-desc <N>               RX ring descriptors
  --tx-desc <N>               TX ring descriptors
  --mbuf-count <N>            Mempool mbuf count
  --mbuf-data-size <BYTES>    Mbuf data room size
  --ip <IPV4>                 Optional: local IPv4 (printed back in output)
  --prefix-len <0-32>         Optional: IPv4 prefix length (printed back in output)
  --gateway <IPV4>            Optional: IPv4 gateway (printed back in output)
  --gateway-mac <MAC>         Optional: gateway MAC (printed back in output)
  --link-up-timeout-secs <N>  Link-up timeout in seconds (default: 10)
  --eal-arg <ARG>             Extra EAL arg (repeatable)
  -h, --help                  Show this help

Notes:
  - Requires Linux + `agave-dpdk` crate feature `dpdk` (and `libdpdk` installed).
  - Default mode only probes DPDK port init + link status. `--full-init` performs the same
    fail-fast checks as the validator (hugepages, vfio binding, link-up wait, gateway ARP probe).
"#
    );
}

fn parse_next<T: std::str::FromStr>(flag: &str, value: Option<String>) -> Result<T, String> {
    let value = value.ok_or_else(|| format!("missing value for {flag}"))?;
    value
        .parse::<T>()
        .map_err(|_| format!("invalid value for {flag}: {value}"))
}

fn main() -> ExitCode {
    let mut config = agave_dpdk::DpdkNetConfig::default();
    let mut devargs: Option<String> = None;
    let mut eal_args: Vec<String> = Vec::new();
    let mut full_init = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                usage();
                return ExitCode::SUCCESS;
            }
            "--devargs" => {
                devargs = match parse_next::<String>("--devargs", args.next()) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        eprintln!("error: {e}");
                        usage();
                        return ExitCode::from(2);
                    }
                };
            }
            "--full-init" => {
                full_init = true;
            }
            "--io-threads" => match parse_next::<u16>("--io-threads", args.next()) {
                Ok(v) => config.io_threads = v,
                Err(e) => {
                    eprintln!("error: {e}");
                    usage();
                    return ExitCode::from(2);
                }
            },
            "--rx-desc" => match parse_next::<u16>("--rx-desc", args.next()) {
                Ok(v) => config.rx_desc = v,
                Err(e) => {
                    eprintln!("error: {e}");
                    usage();
                    return ExitCode::from(2);
                }
            },
            "--tx-desc" => match parse_next::<u16>("--tx-desc", args.next()) {
                Ok(v) => config.tx_desc = v,
                Err(e) => {
                    eprintln!("error: {e}");
                    usage();
                    return ExitCode::from(2);
                }
            },
            "--mbuf-count" => match parse_next::<u32>("--mbuf-count", args.next()) {
                Ok(v) => config.mbuf_count = v,
                Err(e) => {
                    eprintln!("error: {e}");
                    usage();
                    return ExitCode::from(2);
                }
            },
            "--mbuf-data-size" => match parse_next::<u16>("--mbuf-data-size", args.next()) {
                Ok(v) => config.mbuf_data_size = v,
                Err(e) => {
                    eprintln!("error: {e}");
                    usage();
                    return ExitCode::from(2);
                }
            },
            "--ip" => match parse_next::<Ipv4Addr>("--ip", args.next()) {
                Ok(v) => config.local_ip = v,
                Err(e) => {
                    eprintln!("error: {e}");
                    usage();
                    return ExitCode::from(2);
                }
            },
            "--prefix-len" => match parse_next::<u8>("--prefix-len", args.next()) {
                Ok(v) if v <= 32 => config.prefix_len = v,
                Ok(v) => {
                    eprintln!("error: invalid value for --prefix-len: {v}");
                    usage();
                    return ExitCode::from(2);
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    usage();
                    return ExitCode::from(2);
                }
            },
            "--gateway" => match parse_next::<Ipv4Addr>("--gateway", args.next()) {
                Ok(v) => config.gateway_ip = Some(v),
                Err(e) => {
                    eprintln!("error: {e}");
                    usage();
                    return ExitCode::from(2);
                }
            },
            "--gateway-mac" => match parse_next::<agave_dpdk::DpdkMacAddr>(
                "--gateway-mac",
                args.next(),
            ) {
                Ok(v) => config.gateway_mac = Some(v),
                Err(e) => {
                    eprintln!("error: {e}");
                    usage();
                    return ExitCode::from(2);
                }
            },
            "--link-up-timeout-secs" => match parse_next::<u64>("--link-up-timeout-secs", args.next())
            {
                Ok(v) if v > 0 => config.link_up_timeout_secs = v,
                Ok(v) => {
                    eprintln!("error: invalid value for --link-up-timeout-secs: {v}");
                    usage();
                    return ExitCode::from(2);
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    usage();
                    return ExitCode::from(2);
                }
            },
            "--eal-arg" => {
                let v = match args.next() {
                    Some(v) => v,
                    None => {
                        eprintln!("error: missing value for --eal-arg");
                        usage();
                        return ExitCode::from(2);
                    }
                };
                eal_args.push(v);
            }
            other => {
                eprintln!("error: unknown arg: {other}");
                usage();
                return ExitCode::from(2);
            }
        }
    }

    config.devargs = match devargs {
        Some(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!("error: --devargs is required");
            usage();
            return ExitCode::from(2);
        }
    };
    config.eal_args = eal_args;

    if full_init {
        if config.local_ip.is_unspecified() {
            eprintln!("error: --full-init requires --ip <IPV4>");
            usage();
            return ExitCode::from(2);
        }

        let exit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let builder = agave_dpdk::DpdkBuilder::new(config.clone());
        match builder.build(exit) {
            Ok(dpdk) => {
                println!("DPDK full-init:");
                println!("  devargs: {}", config.devargs);
                println!("  io_threads: {}", config.io_threads);
                println!("  tx_queues: {}", dpdk.tx_queues());
                println!("  port_id: {}", dpdk.port_id());
                println!("  port_name: {}", dpdk.port_name());
                println!("  mac: {}", dpdk.local_mac());
                println!("  ip: {}/{}", dpdk.local_ip(), config.prefix_len);
                if let Some(gw) = config.gateway_ip {
                    println!("  gateway: {gw}");
                    if let Some(mac) = config.gateway_mac {
                        println!("  gateway_mac: {mac}");
                    }
                } else if let Some((gw, source)) = agave_dpdk::infer_gateway_from_devargs(&config.devargs)
                {
                    println!("  gateway (inferred): {gw} ({source})");
                }
                if !config.eal_args.is_empty() {
                    println!("  eal_args: {:?}", config.eal_args);
                }
                drop(dpdk);
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("DPDK full-init failed: {e}");
                ExitCode::from(1)
            }
        }
    } else {
    match agave_dpdk::probe(&config) {
        Ok(info) => {
            println!("DPDK probe:");
            println!("  devargs: {}", config.devargs);
            println!("  io_threads: {}", config.io_threads);
            println!("  tx_queues: {}", info.tx_queues);
            println!("  port_id: {}", info.port_id);
            println!("  port_name: {}", info.port_name);
            println!("  mac: {}", info.mac);
            if info.link_up {
                println!("  link: up ({} Mbps)", info.link_speed_mbps);
            } else {
                println!("  link: down");
            }
            if !config.local_ip.is_unspecified() {
                println!("  ip: {}/{}", config.local_ip, config.prefix_len);
            }
            if let Some(gw) = config.gateway_ip {
                println!("  gateway: {gw}");
                if let Some(mac) = config.gateway_mac {
                    println!("  gateway_mac: {mac}");
                }
            } else if let Some((gw, source)) = agave_dpdk::infer_gateway_from_devargs(&config.devargs)
            {
                println!("  gateway (inferred): {gw} ({source})");
            }
            if !config.eal_args.is_empty() {
                println!("  eal_args: {:?}", config.eal_args);
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("DPDK probe failed: {e}");
            ExitCode::from(1)
        }
    }
    }
}

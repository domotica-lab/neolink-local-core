//! Contains code that is not specific to any of the subcommands
//!
use log::*;

use super::config::CameraConfig;
use anyhow::{anyhow, Context, Error, Result};
use neolink_core::bc_protocol::{
    BcCamera, BcCameraOpt, ConnectionProtocol, Credentials, MaxEncryption,
};
use std::{
    fmt::{Display, Error as FmtError, Formatter},
    net::{IpAddr, ToSocketAddrs},
    str::FromStr,
};

pub(crate) fn timeout<F>(future: F) -> tokio::time::Timeout<F>
where
    F: std::future::Future,
{
    tokio::time::timeout(tokio::time::Duration::from_secs(15), future)
}

pub(crate) enum AddressOrUid {
    Address(String),
    Uid(String),
    AddressWithUid(String, String),
}

impl Display for AddressOrUid {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            AddressOrUid::AddressWithUid(addr, uid) => {
                write!(f, "Address: {}, UID: {}", addr, uid)
            }
            AddressOrUid::Address(host) => write!(f, "Address: {}", host),
            AddressOrUid::Uid(host) => write!(f, "UID: {}", host),
        }
    }
}

impl AddressOrUid {
    // Created by translating the config fields directly
    pub(crate) fn new(address: &Option<String>, uid: &Option<String>) -> Result<Self, Error> {
        match (address, uid) {
            (None, None) => Err(anyhow!("Neither address or uid given")),
            (Some(host), Some(uid)) => Ok(AddressOrUid::AddressWithUid(host.clone(), uid.clone())),
            (Some(host), None) => Ok(AddressOrUid::Address(host.clone())),
            (None, Some(host)) => Ok(AddressOrUid::Uid(host.clone())),
        }
    }

    // Convience method to get the BcCamera with the appropiate method
    // from a camera_config
    pub(crate) async fn connect_camera(
        &self,
        camera_config: &CameraConfig,
    ) -> Result<BcCamera, Error> {
        let (port, addrs) = {
            if let Some(addr_str) = camera_config.camera_addr.as_ref() {
                match addr_str.to_socket_addrs() {
                    Ok(addr_iter) => {
                        let mut port = None;
                        let mut ipaddrs = vec![];
                        for addr in addr_iter {
                            port = Some(addr.port());
                            ipaddrs.push(addr.ip());
                        }
                        Ok((port, ipaddrs))
                    }
                    Err(_) => match IpAddr::from_str(addr_str) {
                        Ok(ip) => Ok((None, vec![ip])),
                        Err(_) => Err(anyhow!("Could not parse address in config")),
                    },
                }
            } else {
                Ok((None, vec![]))
            }
        }?;

        let options = BcCameraOpt {
            name: camera_config.name.clone(),
            channel_id: camera_config.channel_id,
            addrs,
            port,
            uid: camera_config.camera_uid.clone(),
            protocol: ConnectionProtocol::TcpUdp,
            discovery: camera_config.discovery,
            relay_server_region: camera_config.relay_server_region.clone(),
            credentials: Credentials {
                username: camera_config.username.clone(),
                password: camera_config.password.clone(),
            },
            debug: camera_config.debug,
            max_discovery_retries: camera_config.max_discovery_retries,
            udp_gap_skip_ms: camera_config.udp_gap_skip_ms,
            skip_auto_tcp: is_battery_camera(&camera_config.name),
        };

        trace!("Camera Info: {:?}", options);

        Ok(BcCamera::new(&options).await?)
    }
}

pub(crate) async fn connect_and_login(camera_config: &CameraConfig) -> Result<BcCamera> {
    let camera_addr =
        AddressOrUid::new(&camera_config.camera_addr, &camera_config.camera_uid).unwrap();
    let started = std::time::Instant::now();
    info!(
        "{}: Connecting to camera at {}",
        camera_config.name, camera_addr
    );
    // Show the *effective* values that will actually be applied — resolve
    // optional/defaulted settings rather than printing raw `None`.
    let relay_region = match &camera_config.relay_server_region {
        Some(region) => format!("{region:?}"),
        None => "none (all relay servers)".to_string(),
    };
    let udp_gap_skip_ms = match camera_config.udp_gap_skip_ms {
        Some(ms) => format!("{ms} ms"),
        None => format!(
            "{} ms (default)",
            neolink_core::bc_protocol::DEFAULT_GAP_SKIP_WAIT_MS
        ),
    };
    info!(
        "{}: Effective connection config: discovery={:?}, relay_region={}, connect_mode={:?}, udp_gap_skip_ms={}, buffer_duration={} ms, max_encryption={}, max_discovery_retries={}, strict={}, stream={:?}",
        camera_config.name,
        camera_config.discovery,
        relay_region,
        camera_config.connect_mode,
        udp_gap_skip_ms,
        camera_config.buffer_duration,
        camera_config.max_encryption,
        camera_config.max_discovery_retries,
        camera_config.strict,
        camera_config.stream,
    );

    let camera = camera_addr
        .connect_camera(camera_config)
        .await
        .with_context(|| {
            format!(
                "Failed to connect to camera {} at {} on channel {}",
                camera_config.name, camera_addr, camera_config.channel_id
            )
        })?;

    let max_encryption = match camera_config.max_encryption.to_lowercase().as_str() {
        "none" => MaxEncryption::None,
        "bcencrypt" => MaxEncryption::BcEncrypt,
        "aes" => MaxEncryption::Aes,
        _ => MaxEncryption::Aes,
    };
    let found_after = started.elapsed();
    info!("{}: Logging in", camera_config.name);
    timeout(camera.login_with_maxenc(max_encryption))
        .await
        .with_context(|| format!("Failed to login to {}", camera_config.name))??;
    camera
        .verify_auto_tcp()
        .await
        .with_context(|| format!("{}: TCP address check failed", camera_config.name))?;

    info!(
        "{}: Connected and logged in ({:.1}s: found in {:.1}s, login {:.1}s)",
        camera_config.name,
        started.elapsed().as_secs_f32(),
        found_after.as_secs_f32(),
        (started.elapsed() - found_after).as_secs_f32()
    );

    Ok(camera)
}

/// Restore each UID camera's last known LAN address, saved by the status API
/// in `<dir of stream_cache_path>/snapshots/addresses.json`, before any camera
/// connects. Cameras in `always` mode connect at startup and stay connected,
/// so the addresses must be known by then for the automatic TCP attempt to
/// run (the status API itself only starts after the cameras).
pub(crate) fn seed_saved_addresses(stream_cache_path: Option<&str>) {
    let Some(dir) = stream_cache_path.filter(|p| !p.is_empty()).and_then(|p| {
        std::path::Path::new(p)
            .parent()
            .map(|d| d.join("snapshots"))
    }) else {
        return;
    };
    seed_battery_cameras(&dir);
    let file = dir.join("addresses.json");
    let Ok(bytes) = std::fs::read(&file) else {
        return;
    };
    let addrs: std::collections::HashMap<String, IpAddr> =
        serde_json::from_slice(&bytes).unwrap_or_default();
    for (uid, ip) in addrs.iter() {
        neolink_core::bc_protocol::remember_local_address(uid, *ip);
    }
    if !addrs.is_empty() {
        info!("Restored last known address of {} camera(s)", addrs.len());
    }
}

/// Cameras known to run on a battery (they have reported a battery level),
/// by sanitised name ([`battery_file_stem`]).
/// Battery Reolinks sleep and have no TCP service, so the automatic TCP
/// attempt is skipped for them: no probe, no wait, straight to UDP.
static BATTERY_CAMERAS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> =
    std::sync::LazyLock::new(Default::default);

/// Record that `camera` reported a battery level.
#[cfg_attr(not(feature = "gstreamer"), allow(dead_code))]
pub(crate) fn mark_battery_camera(camera: &str) {
    BATTERY_CAMERAS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(battery_file_stem(camera));
}

pub(crate) fn is_battery_camera(camera: &str) -> bool {
    BATTERY_CAMERAS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&battery_file_stem(camera))
}

/// Same file-name mapping the status API uses for `<name>.battery.json`.
fn battery_file_stem(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// At start-up, mark every configured camera with a saved battery reading
/// (`snapshots/<name>.battery.json`, written by the status API) as a battery
/// camera, before any camera connects.
fn seed_battery_cameras(dir: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let stems: std::collections::HashSet<String> = entries
        .flatten()
        .filter_map(|e| {
            e.file_name()
                .to_str()
                .and_then(|n| n.strip_suffix(".battery.json"))
                .map(str::to_string)
        })
        .collect();
    if stems.is_empty() {
        return;
    }
    info!(
        "Known battery camera(s), automatic TCP skipped: {}",
        stems.iter().cloned().collect::<Vec<_>>().join(", ")
    );
    // The set holds sanitised names, the same form as the file stems.
    BATTERY_CAMERAS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .extend(stems);
}

#[cfg(test)]
mod seed_tests {
    use super::*;

    #[test]
    fn seeds_addresses_saved_next_to_the_stream_cache() {
        let dir = std::env::temp_dir().join(format!("neolink-seed-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("snapshots")).unwrap();
        std::fs::write(
            dir.join("snapshots").join("addresses.json"),
            r#"{"TESTUIDSEED0001":"192.168.68.99"}"#,
        )
        .unwrap();
        let cache = dir.join("stream-cache.json");
        seed_saved_addresses(cache.to_str());
        assert_eq!(
            neolink_core::bc_protocol::last_local_address("TESTUIDSEED0001"),
            Some("192.168.68.99".parse().unwrap())
        );
        assert!(!is_battery_camera("Cancello Nord"));
        // Missing/empty path: no panic, nothing to do.
        seed_saved_addresses(None);
        seed_saved_addresses(Some(""));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn battery_cameras_are_known_from_saved_readings_and_at_runtime() {
        let dir = std::env::temp_dir().join(format!("neolink-batt-{}", std::process::id()));
        let snaps = dir.join("snapshots");
        std::fs::create_dir_all(&snaps).unwrap();
        // The status API saves "Area Cani" as Area_Cani.battery.json.
        std::fs::write(snaps.join("Area_Cani.battery.json"), "{}").unwrap();
        seed_saved_addresses(dir.join("stream-cache.json").to_str());
        assert!(is_battery_camera("Area Cani"));
        assert!(is_battery_camera("Area_Cani"));
        assert!(!is_battery_camera("Garage"));
        // A camera that reports a battery level while running.
        mark_battery_camera("Garage");
        assert!(is_battery_camera("Garage"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}

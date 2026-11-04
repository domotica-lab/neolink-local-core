//! Local status API used by the Home Assistant integration.
//!
//! - `GET /api/status`: every camera, whether it is connected, its streams and
//!   how many viewers each one has.
//! - `GET /api/cameras/<name>/snapshot.jpg[?stream=main|sub|extern]`: the last
//!   frame neolink received from that camera, as JPEG. It never contacts the
//!   camera, so a dashboard thumbnail does not wake a battery camera. When the
//!   camera has not streamed since start-up, the image saved on disk the last
//!   time is served instead.
//!
//! The API is read-only and listens on loopback by default (`api_bind`), so
//! only the local machine can reach it.
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context};
use gstreamer::{prelude::*, ClockTime};
use gstreamer_app::{AppSink, AppSrc};
use neolink_core::{bc_protocol::StreamKind, bcmedia::model::VideoType};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::{battery, last_keyframe, seed_battery, stream_viewers, BatteryState, NeoReactor},
    config::{CameraConfig, ConnectMode},
    AnyResult,
};

/// Width of the JPEG still images (height follows the camera's aspect ratio).
const SNAPSHOT_WIDTH: u32 = 1280;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// JPEGs already encoded, keyed by (camera, stream), with the time of the
/// keyframe they were made from so each keyframe is decoded only once.
type JpegCache = HashMap<(String, StreamKind), (SystemTime, Arc<Vec<u8>>)>;
static JPEGS: LazyLock<Mutex<JpegCache>> = LazyLock::new(|| Mutex::new(HashMap::new()));

pub(super) async fn serve(reactor: NeoReactor, cancel: CancellationToken) -> AnyResult<()> {
    let config = reactor.config().await?.borrow().clone();
    if config.api_port == 0 {
        log::info!("Status API disabled (api_port = 0)");
        cancel.cancelled().await;
        return Ok(());
    }
    let addr = format!("{}:{}", config.api_bind, config.api_port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            // Never take the RTSP server down because of the API.
            log::warn!("Status API: cannot listen on {addr}: {e}; continuing without it");
            cancel.cancelled().await;
            return Ok(());
        }
    };
    log::info!("Status API at http://{addr}/api/status");
    let snapshot_dir = snapshot_dir(config.stream_cache_path.as_deref());
    if let Some(dir) = snapshot_dir.as_deref() {
        for cam in config.cameras.iter() {
            if let Some(state) = load_battery(&battery_file(dir, &cam.name)).await {
                seed_battery(&cam.name, state);
            }
        }
    }
    let mut saved: HashMap<String, u64> = HashMap::new();
    let mut save_tick = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = save_tick.tick() => {
                if let Some(dir) = snapshot_dir.as_deref() {
                    let names: Vec<String> = reactor
                        .config()
                        .await?
                        .borrow()
                        .cameras
                        .iter()
                        .map(|c| c.name.clone())
                        .collect();
                    for name in names {
                        if let Some(state) = battery(&name) {
                            if saved.get(&name) != Some(&state.at) {
                                save_battery(&battery_file(dir, &name), &state).await;
                                saved.insert(name, state.at);
                            }
                        }
                    }
                }
            }
            accepted = listener.accept() => {
                let (socket, _) = match accepted {
                    Ok(v) => v,
                    Err(e) => {
                        log::debug!("Status API: accept failed: {e}");
                        continue;
                    }
                };
                let reactor = reactor.clone();
                let snapshot_dir = snapshot_dir.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle(socket, reactor, snapshot_dir).await {
                        log::debug!("Status API: request failed: {e:?}");
                    }
                });
            }
        }
    }
}

/// Still images are saved next to the stream cache (normally /data/snapshots).
fn snapshot_dir(stream_cache_path: Option<&str>) -> Option<PathBuf> {
    let path = PathBuf::from(stream_cache_path.filter(|p| !p.is_empty())?);
    Some(path.parent()?.join("snapshots"))
}

async fn handle(
    mut socket: TcpStream,
    reactor: NeoReactor,
    snapshot_dir: Option<PathBuf>,
) -> AnyResult<()> {
    let request = tokio::time::timeout(REQUEST_TIMEOUT, read_head(&mut socket))
        .await
        .context("timed out reading request")??;
    let mut parts = request.split_whitespace();
    let (method, target) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    let (path, query) = target.split_once('?').unwrap_or((target, ""));

    let response = if method != "GET" {
        Response::text(405, "only GET is supported")
    } else if path == "/api/status" {
        Response::json(status(&reactor).await?)
    } else if let Some(name) = path
        .strip_prefix("/api/cameras/")
        .and_then(|rest| rest.strip_suffix("/snapshot.jpg"))
    {
        let name = percent_decode(name);
        let stream = query
            .split('&')
            .find_map(|kv| kv.strip_prefix("stream="))
            .and_then(parse_stream);
        snapshot(&reactor, &name, stream, snapshot_dir.as_deref()).await
    } else {
        Response::text(404, "not found")
    };
    socket.write_all(&response.into_bytes()).await?;
    socket.shutdown().await.ok();
    Ok(())
}

/// Read the request line and headers (a GET has no body).
async fn read_head(socket: &mut TcpStream) -> AnyResult<String> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > 16 * 1024 {
            return Err(anyhow!("request too large"));
        }
    }
    let head = String::from_utf8_lossy(&buf);
    Ok(head.lines().next().unwrap_or("").to_string())
}

async fn status(reactor: &NeoReactor) -> AnyResult<serde_json::Value> {
    let config = reactor.config().await?.borrow().clone();
    let mut cameras = Vec::new();
    for cam in config.cameras.iter() {
        cameras.push(camera_status(reactor, cam).await);
    }
    Ok(json!({
        "version": env!("NEOLINK_VERSION"),
        "rtsp_port": config.bind_port,
        "cameras": cameras,
    }))
}

async fn camera_status(reactor: &NeoReactor, cam: &CameraConfig) -> serde_json::Value {
    let connected = if cam.enabled {
        match reactor.get(&cam.name).await {
            Ok(instance) => instance.camera().borrow().upgrade().is_some(),
            Err(_) => false,
        }
    } else {
        false
    };
    let streams: Vec<_> = cam
        .stream
        .as_stream_kinds()
        .into_iter()
        .map(|kind| {
            let viewers = stream_viewers(&cam.name, kind);
            json!({
                "stream": stream_name(kind),
                "path": format!("/{}/{}", cam.name, stream_name(kind)),
                "viewers": viewers,
                "streaming": viewers > 0,
                "last_frame_at": last_keyframe(&cam.name, kind).map(|k| unix_secs(k.at)),
            })
        })
        .collect();
    let viewers: usize = streams
        .iter()
        .filter_map(|s| s["viewers"].as_u64())
        .sum::<u64>() as usize;
    json!({
        "name": cam.name,
        "enabled": cam.enabled,
        "connect_mode": match cam.connect_mode {
            ConnectMode::Always => "always",
            ConnectMode::OnDemand => "on_demand",
        },
        "connected": connected,
        "viewers": viewers,
        "streams": streams,
        "battery": battery(&cam.name).map(|b| json!({
            "percent": b.percent,
            "charging": b.charge_status == "charging",
            "charge_status": b.charge_status,
            "adapter": b.adapter_status,
            "temperature": b.temperature,
            "low": b.low_power,
            "updated_at": b.at,
        })),
    })
}

async fn snapshot(
    reactor: &NeoReactor,
    name: &str,
    wanted: Option<StreamKind>,
    snapshot_dir: Option<&std::path::Path>,
) -> Response {
    let Ok(config) = reactor.config().await else {
        return Response::text(503, "not ready");
    };
    let config = config.borrow().clone();
    let Some(cam) = config.cameras.iter().find(|c| c.name == name) else {
        return Response::text(404, "unknown camera");
    };
    // Requested stream first, then the best available (main gives the best image).
    let mut order = vec![StreamKind::Main, StreamKind::Extern, StreamKind::Sub];
    if let Some(w) = wanted {
        order.retain(|k| *k != w);
        order.insert(0, w);
    }
    let available = cam.stream.as_stream_kinds();
    let found = order
        .into_iter()
        .filter(|k| available.contains(k))
        .find_map(|k| last_keyframe(name, k).map(|f| (k, f)));

    let file = snapshot_dir.map(|d| d.join(format!("{}.jpg", sanitize(name))));
    if let Some((kind, frame)) = found {
        let key = (name.to_string(), kind);
        let cached = JPEGS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .filter(|(at, _)| *at == frame.at)
            .map(|(_, jpeg)| jpeg.clone());
        let jpeg = match cached {
            Some(j) => Some(j),
            None => {
                let data = frame.data.clone();
                let vt = frame.video_type;
                match tokio::task::spawn_blocking(move || keyframe_to_jpeg(vt, &data)).await {
                    Ok(Ok(bytes)) => {
                        let bytes = Arc::new(bytes);
                        JPEGS
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .insert(key, (frame.at, bytes.clone()));
                        if let Some(file) = file.as_ref() {
                            save_snapshot(file, &bytes).await;
                        }
                        Some(bytes)
                    }
                    Ok(Err(e)) => {
                        log::warn!("{name}: could not make a still image: {e:?}");
                        None
                    }
                    Err(e) => {
                        log::warn!("{name}: still image task failed: {e}");
                        None
                    }
                }
            }
        };
        if let Some(jpeg) = jpeg {
            return Response::jpeg(jpeg.to_vec(), frame.at);
        }
    }
    // Nothing seen since start-up: the image saved last time, if any.
    if let Some(file) = file {
        if let Ok(bytes) = tokio::fs::read(&file).await {
            let at = tokio::fs::metadata(&file)
                .await
                .and_then(|m| m.modified())
                .unwrap_or(UNIX_EPOCH);
            return Response::jpeg(bytes, at);
        }
    }
    Response::text(404, "no image yet: open the camera once")
}

fn battery_file(dir: &std::path::Path, name: &str) -> PathBuf {
    dir.join(format!("{}.battery.json", sanitize(name)))
}

async fn load_battery(file: &std::path::Path) -> Option<BatteryState> {
    let bytes = tokio::fs::read(file).await.ok()?;
    serde_json::from_slice(&bytes).ok()
}

async fn save_battery(file: &std::path::Path, state: &BatteryState) {
    let Ok(bytes) = serde_json::to_vec(state) else {
        return;
    };
    if let Some(dir) = file.parent() {
        if tokio::fs::create_dir_all(dir).await.is_err() {
            return;
        }
    }
    let tmp = file.with_extension("json.tmp");
    if tokio::fs::write(&tmp, &bytes).await.is_ok() {
        let _ = tokio::fs::rename(&tmp, file).await;
    }
}

async fn save_snapshot(file: &std::path::Path, bytes: &[u8]) {
    if let Some(dir) = file.parent() {
        if let Err(e) = tokio::fs::create_dir_all(dir).await {
            log::debug!("snapshot dir {dir:?}: {e}");
            return;
        }
    }
    // Write then rename, so a reader never sees a half-written image.
    let tmp = file.with_extension("jpg.tmp");
    if tokio::fs::write(&tmp, bytes).await.is_ok() {
        let _ = tokio::fs::rename(&tmp, file).await;
    }
}

/// Decode one H.264/H.265 keyframe and encode it as a JPEG still image.
pub(super) fn keyframe_to_jpeg(video_type: VideoType, data: &[u8]) -> AnyResult<Vec<u8>> {
    let (parse, dec, kind) = match video_type {
        VideoType::H264 => ("h264parse", "avdec_h264", "h264"),
        VideoType::H265 => ("h265parse", "avdec_h265", "h265"),
    };
    let desc = format!(
        "appsrc name=src format=time caps=video/x-{kind},stream-format=byte-stream,alignment=au ! \
         {parse} ! {dec} ! videoconvert ! videoscale ! \
         video/x-raw,width={SNAPSHOT_WIDTH},pixel-aspect-ratio=1/1 ! \
         jpegenc quality=85 ! appsink name=sink sync=false"
    );
    let pipeline = gstreamer::parse::launch(&desc)
        .context("snapshot: parse pipeline")?
        .downcast::<gstreamer::Pipeline>()
        .map_err(|_| anyhow!("snapshot: not a pipeline"))?;
    struct NullGuard(gstreamer::Pipeline);
    impl Drop for NullGuard {
        fn drop(&mut self) {
            let _ = self.0.set_state(gstreamer::State::Null);
        }
    }
    let _guard = NullGuard(pipeline.clone());
    let src = pipeline
        .by_name("src")
        .context("snapshot: appsrc missing")?
        .downcast::<AppSrc>()
        .map_err(|_| anyhow!("snapshot: src is not an AppSrc"))?;
    let sink = pipeline
        .by_name("sink")
        .context("snapshot: appsink missing")?
        .downcast::<AppSink>()
        .map_err(|_| anyhow!("snapshot: sink is not an AppSink"))?;
    pipeline.set_state(gstreamer::State::Playing)?;

    let mut buffer = gstreamer::Buffer::from_slice(data.to_vec());
    buffer
        .get_mut()
        .context("snapshot: buffer not writable")?
        .set_pts(ClockTime::ZERO);
    src.push_buffer(buffer)
        .map_err(|e| anyhow!("snapshot: push failed: {e:?}"))?;
    // End of stream makes the decoder drain the single frame.
    let _ = src.end_of_stream();
    let sample = sink
        .try_pull_sample(Some(ClockTime::from_seconds(5)))
        .context("snapshot: decoder produced no image")?;
    let buf = sample.buffer().context("snapshot: empty sample")?;
    let map = buf.map_readable()?;
    Ok(map.as_slice().to_vec())
}

fn stream_name(kind: StreamKind) -> &'static str {
    match kind {
        StreamKind::Main => "main",
        StreamKind::Sub => "sub",
        StreamKind::Extern => "extern",
    }
}

fn parse_stream(s: &str) -> Option<StreamKind> {
    match s.to_ascii_lowercase().as_str() {
        "main" | "mainstream" => Some(StreamKind::Main),
        "sub" | "substream" => Some(StreamKind::Sub),
        "extern" | "externstream" => Some(StreamKind::Extern),
        _ => None,
    }
}

fn unix_secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// File-name safe version of a camera name.
fn sanitize(name: &str) -> String {
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

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
            if let Ok(v) = u8::from_str_radix(hex, 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

struct Response {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
    last_modified: Option<SystemTime>,
}

impl Response {
    fn text(status: u16, msg: &str) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            body: msg.as_bytes().to_vec(),
            last_modified: None,
        }
    }
    fn json(value: serde_json::Value) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            body: value.to_string().into_bytes(),
            last_modified: None,
        }
    }
    fn jpeg(body: Vec<u8>, at: SystemTime) -> Self {
        Self {
            status: 200,
            content_type: "image/jpeg",
            body,
            last_modified: Some(at),
        }
    }
    fn into_bytes(self) -> Vec<u8> {
        let reason = match self.status {
            200 => "OK",
            404 => "Not Found",
            405 => "Method Not Allowed",
            503 => "Service Unavailable",
            _ => "Error",
        };
        let mut head = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n",
            self.status,
            reason,
            self.content_type,
            self.body.len()
        );
        if let Some(at) = self.last_modified {
            head.push_str(&format!("X-Image-Time: {}\r\n", unix_secs(at)));
        }
        head.push_str("\r\n");
        let mut out = head.into_bytes();
        out.extend_from_slice(&self.body);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn battery_survives_a_restart() {
        let dir = std::env::temp_dir().join(format!("neolink-bat-{}", std::process::id()));
        let file = battery_file(&dir, "Cam 1");
        assert!(
            file.ends_with("Cam_1.battery.json")
                || file.to_string_lossy().contains(".battery.json")
        );
        let state = BatteryState {
            percent: 87,
            charge_status: "none".into(),
            adapter_status: "solarPanel".into(),
            temperature: 21,
            low_power: false,
            at: 1_700_000_000,
        };
        save_battery(&file, &state).await;
        assert_eq!(load_battery(&file).await, Some(state.clone()));
        seed_battery("api-test-cam", state.clone());
        let older = BatteryState {
            percent: 10,
            at: 1,
            ..state.clone()
        };
        seed_battery("api-test-cam", older);
        assert_eq!(battery("api-test-cam").map(|b| b.percent), Some(87));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn percent_decoding() {
        assert_eq!(percent_decode("Porta%20Nord"), "Porta Nord");
        assert_eq!(percent_decode("Giardino"), "Giardino");
        assert_eq!(percent_decode("bad%zz"), "bad%zz");
        assert_eq!(percent_decode("end%2"), "end%2");
    }

    #[test]
    fn stream_parsing_and_names() {
        assert_eq!(parse_stream("SUB"), Some(StreamKind::Sub));
        assert_eq!(parse_stream("mainStream"), Some(StreamKind::Main));
        assert_eq!(parse_stream("x"), None);
        assert_eq!(stream_name(StreamKind::Extern), "extern");
    }

    #[test]
    fn snapshot_dir_is_next_to_the_cache() {
        assert_eq!(
            snapshot_dir(Some("/data/stream-cache.json")),
            Some(PathBuf::from("/data/snapshots"))
        );
        assert_eq!(snapshot_dir(None), None);
        assert_eq!(snapshot_dir(Some("")), None);
        assert_eq!(sanitize("Porta Nord/1"), "Porta_Nord_1");
    }

    #[test]
    fn keyframe_becomes_a_jpeg() {
        gstreamer::init().expect("gstreamer init");
        let idr = super::super::factory::encode_black_keyframe(VideoType::H264, 640, 360)
            .expect("test keyframe");
        let jpeg = keyframe_to_jpeg(VideoType::H264, &idr).expect("jpeg");
        assert_eq!(&jpeg[..2], &[0xFF, 0xD8], "JPEG start marker");
        assert!(jpeg.len() > 500);
    }
}

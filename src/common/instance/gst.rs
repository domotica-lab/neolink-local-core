use super::*;

use crate::common::UseCounter;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use neolink_core::{
    bc_protocol::StreamKind,
    bcmedia::model::{BcMedia, VideoType},
};
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex as StdMutex, Weak},
    time::{Duration, SystemTime},
};
use tokio::sync::{broadcast, mpsc::Receiver as MpscReceiver, watch};
use tokio_util::sync::CancellationToken;

/// Buffer capacity for the media-frame mpsc channel feeding a consumer.
const MEDIA_CHANNEL_CAP: usize = 500;

impl NeoInstance {
    /// Streams a camera source while not paused
    pub(crate) async fn stream_while_live(
        &self,
        stream: StreamKind,
    ) -> AnyResult<MpscReceiver<BcMedia>> {
        let config = self.config().await?.borrow().clone();
        let name = config.name.clone();

        let media_rx = if config.pause.on_motion {
            let (media_tx, media_rx) = tokio::sync::mpsc::channel(MEDIA_CHANNEL_CAP);
            let counter = UseCounter::new().await;

            let mut md = self.motion().await?;
            let mut tasks = FuturesUnordered::new();
            // Stream for 5s on a new client always
            // This lets us negotiate the camera stream type
            let init_permit = counter.create_activated().await?;
            tokio::spawn(
                async {
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    drop(init_permit);
                }
                .map(|e| {
                    log::debug!("Init permit thread stopped {e:?}");
                    e
                }),
            );

            // Create the permit for controlling the motion
            let mut md_permit = {
                let md_state = md.borrow_and_update().clone();
                match md_state {
                    MdState::Start(_) => {
                        log::info!("{name}::{stream:?}: Starting with Motion");
                        counter.create_activated().await?
                    }
                    MdState::Stop(_) | MdState::Unknown => {
                        log::info!("{name}::{stream:?}: Waiting with Motion");
                        counter.create_deactivated().await?
                    }
                }
            };
            // Now listen to the motion
            let thread_name = name.clone();
            tasks.push(tokio::spawn(
                async move {
                    loop {
                        match md.changed().await {
                            Ok(_) => {
                                let md_state: MdState = md.borrow_and_update().clone();
                                match md_state {
                                    MdState::Start(_) => {
                                        log::info!("{thread_name}::{stream:?}: Motion Started");
                                        md_permit.activate().await?;
                                    }
                                    MdState::Stop(_) => {
                                        log::info!("{thread_name}::{stream:?}: Motion Stopped");
                                        md_permit.deactivate().await?;
                                    }
                                    MdState::Unknown => {}
                                }
                            }
                            Err(e) => {
                                // Use break here so we can define the full type on the async closure
                                break AnyResult::Err(e.into());
                            }
                        }
                    }?;
                    AnyResult::Ok(())
                }
                .map(|e| {
                    log::debug!("Motion thread stopped {e:?}");
                    e
                }),
            ));

            // Send the camera when the permit is active
            let camera_permit = counter.create_deactivated().await?;
            let thread_camera = self.clone();
            let thread_name2 = name.clone();
            tokio::spawn(
                async move {
                    let res = loop {
                        log::debug!("{thread_name2}: Waiting for stream permit users");
                        // Also stop if the consumer goes away while we are paused,
                        // otherwise this task (and the motion task) would live forever.
                        tokio::select! {
                            v = camera_permit.aquired_users() => {
                                if let Err(e) = v {
                                    break AnyResult::Err(e);
                                }
                            }
                            _ = media_tx.closed() => {
                                log::info!("{thread_name2}::{stream:?}: Consumer closed while paused, stopping stream task");
                                break AnyResult::Ok(());
                            }
                        }
                        log::info!(
                            "{thread_name2}: Stream permit acquired, starting camera stream"
                        );
                        let closed = tokio::select! {
                            _ = media_tx.closed() => {
                                log::info!("{thread_name2}::{stream:?}: Consumer closed, stopping camera stream and releasing permit");
                                Ok(true)
                            },
                            v = camera_permit.dropped_users() => {
                                log::debug!("{thread_name2}: Dropped users: {v:?}");
                                v.map(|_| false)
                            },
                            v = async {
                                log::info!("{thread_name2}: Requesting stream from camera");
                                let mut stream = thread_camera.stream(stream).await?;
                                log::info!("{thread_name2}: Got stream from camera, sending media");
                                while let Some(media) = stream.recv().await {
                                    media_tx.send(media).await?;
                                }
                                AnyResult::Ok(())
                            } => {
                                log::debug!("Stopped stream: {v:?}");
                                v.map(|_| false)
                            },
                            v = tasks.next() => {
                                log::debug!("Task failed: {v:?}");
                                Err(anyhow!("Task ended prematurly: {v:?}"))
                            }
                        }?;
                        if closed {
                            break AnyResult::Ok(());
                        }
                        log::debug!("Pausing stream");
                    };
                    // The motion listener is a detached task: stop it with us.
                    for t in tasks.iter() {
                        t.abort();
                    }
                    drop(counter); // Make sure counter is owned by this thread
                    res
                }
                .map(|e| {
                    log::debug!("Stream thread stopped {e:?}");
                    e
                }),
            );

            Ok(media_rx)
        } else {
            self.stream(stream).await
        }?;

        Ok(media_rx)
    }

    /// Streams a camera source
    ///
    /// All consumers of the same camera stream share ONE camera-side stream
    /// (see [`StreamHub`]): the camera gets a single start command, every viewer
    /// receives a copy of the frames, and a viewer that (re)connects while the
    /// stream is running starts at once from the last keyframe. This also stops
    /// one viewer's teardown (the camera's video-stop command) from cutting the
    /// video of the others.
    ///
    /// The camera-side stream holds a use-permit while it runs and stops a short
    /// moment after its last viewer leaves, so an `on_demand` camera still
    /// disconnects.
    pub(crate) async fn stream(&self, stream: StreamKind) -> AnyResult<MpscReceiver<BcMedia>> {
        let (media_tx, media_rx) = tokio::sync::mpsc::channel(MEDIA_CHANNEL_CAP);
        let config = self.config().await?.borrow().clone();
        let name = config.name.clone();

        let (hub, snapshot, mut rx, viewers) = StreamHub::join(self, &name, stream, config.strict);
        if viewers > 1 {
            log::info!(
                "{name}::{stream:?}: Joined the running camera stream ({viewers} viewers, {} buffered frames)",
                snapshot.len()
            );
        }

        tokio::task::spawn(async move {
            // Leaves the hub (and lets it stop) however this task ends.
            let _viewer = ViewerGuard(hub.clone());
            for media in snapshot {
                if media_tx.send(media).await.is_err() {
                    return;
                }
            }
            let mut wait_keyframe = false;
            loop {
                tokio::select! {
                    _ = hub.stopped.cancelled() => break,
                    _ = media_tx.closed() => break,
                    r = rx.recv() => match r {
                        Ok(media) => {
                            if wait_keyframe {
                                if !matches!(media, BcMedia::Iframe(_)) {
                                    continue;
                                }
                                wait_keyframe = false;
                            }
                            if media_tx.send(media).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            // This viewer fell behind: skip to the next keyframe so
                            // it never decodes a frame whose references are missing.
                            log::debug!("{}::{:?}: viewer lagged by {n} frames, resyncing on next keyframe", hub.name, hub.stream);
                            wait_keyframe = true;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                }
            }
        });

        Ok(media_rx)
    }
}

/// How long a camera stream keeps running after its last viewer leaves, so a
/// player that reconnects (go2rtc often does) finds it still running.
const HUB_LINGER: Duration = Duration::from_secs(2);
/// Upper bound on frames kept since the last keyframe (≈ 25s at 15 fps + audio).
const HUB_GOP_MAX: usize = 800;

type HubKey = (String, StreamKind);
static HUBS: LazyLock<StdMutex<HashMap<HubKey, Weak<StreamHub>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

/// The most recent complete keyframe seen on a stream, kept after the stream
/// stops so a still image can be served without waking the camera.
#[derive(Clone)]
pub(crate) struct LastKeyframe {
    pub(crate) video_type: VideoType,
    pub(crate) data: Arc<Vec<u8>>,
    pub(crate) at: SystemTime,
}

static LAST_KEYFRAMES: LazyLock<StdMutex<HashMap<HubKey, LastKeyframe>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

/// Last keyframe received for `camera`'s `stream`, if any since start-up.
pub(crate) fn last_keyframe(camera: &str, stream: StreamKind) -> Option<LastKeyframe> {
    LAST_KEYFRAMES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&(camera.to_string(), stream))
        .cloned()
}

/// Viewers currently attached to `camera`'s running `stream` (0 if not running).
pub(crate) fn stream_viewers(camera: &str, stream: StreamKind) -> usize {
    HUBS.lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&(camera.to_string(), stream))
        .and_then(Weak::upgrade)
        .filter(|h| !h.stopped.is_cancelled())
        .map(|h| *h.viewers.borrow())
        .unwrap_or(0)
}

/// One running camera-side stream, shared by every viewer of that stream.
struct StreamHub {
    name: String,
    stream: StreamKind,
    tx: broadcast::Sender<BcMedia>,
    /// Frames since the most recent keyframe, replayed to a joining viewer.
    gop: StdMutex<Vec<BcMedia>>,
    /// Number of viewers attached; only changed while `HUBS` is locked.
    viewers: watch::Sender<usize>,
    /// Cancelled when the camera stream has stopped.
    stopped: CancellationToken,
}

struct ViewerGuard(Arc<StreamHub>);

impl Drop for ViewerGuard {
    fn drop(&mut self) {
        let _map = HUBS.lock().unwrap_or_else(|e| e.into_inner());
        self.0.viewers.send_modify(|v| *v = v.saturating_sub(1));
    }
}

impl StreamHub {
    /// Attach a viewer to the running stream, starting one if needed. Returns the
    /// hub, the frames to replay, the live receiver and the viewer count.
    fn join(
        camera: &NeoInstance,
        name: &str,
        stream: StreamKind,
        strict: bool,
    ) -> (
        Arc<StreamHub>,
        Vec<BcMedia>,
        broadcast::Receiver<BcMedia>,
        usize,
    ) {
        let mut map = HUBS.lock().unwrap_or_else(|e| e.into_inner());
        let key = (name.to_string(), stream);
        let hub = match map
            .get(&key)
            .and_then(Weak::upgrade)
            .filter(|h| !h.stopped.is_cancelled())
        {
            Some(hub) => hub,
            None => {
                let hub = StreamHub::new(name, stream);
                map.insert(key, Arc::downgrade(&hub));
                tokio::task::spawn(hub.clone().run(camera.clone(), strict));
                hub
            }
        };
        let (snapshot, rx, viewers) = hub.attach();
        (hub, snapshot, rx, viewers)
    }

    fn new(name: &str, stream: StreamKind) -> Arc<StreamHub> {
        Arc::new(StreamHub {
            name: name.to_string(),
            stream,
            tx: broadcast::channel(MEDIA_CHANNEL_CAP).0,
            gop: StdMutex::new(Vec::new()),
            viewers: watch::channel(0).0,
            stopped: CancellationToken::new(),
        })
    }

    /// Count one more viewer and hand it the replay + live receiver. Must be
    /// called with `HUBS` locked (viewer counts only change under that lock).
    fn attach(&self) -> (Vec<BcMedia>, broadcast::Receiver<BcMedia>, usize) {
        self.viewers.send_modify(|v| *v += 1);
        let viewers = *self.viewers.borrow();
        // Snapshot + subscribe under the gop lock: the camera task pushes and
        // broadcasts under the same lock, so the viewer gets no gap and no duplicate.
        let gop = self.gop.lock().unwrap_or_else(|e| e.into_inner());
        (gop.clone(), self.tx.subscribe(), viewers)
    }

    /// Runs the camera-side stream until it fails or nobody watches it anymore.
    async fn run(self: Arc<Self>, camera: NeoInstance, strict: bool) {
        let name = self.name.clone();
        let stream = self.stream;
        log::info!("{name}::{stream:?}: Starting camera stream");
        let hub = self.clone();
        let res = tokio::select! {
            v = camera.run_task(move |cam| {
                let hub = hub.clone();
                Box::pin(async move {
                    let mut media_stream = cam.start_video(stream, 0, strict).await?;
                    log::trace!("Camera started");
                    while let Ok(media) = media_stream.get_data().await? {
                        hub.publish(media);
                    }
                    AnyResult::Ok(())
                })
            }) => v,
            _ = self.wait_unwatched() => {
                log::info!("{name}::{stream:?}: Consumer closed, stopping camera stream task and releasing permit");
                AnyResult::Ok(())
            }
        };
        self.retire(&mut HUBS.lock().unwrap_or_else(|e| e.into_inner()));
        log::debug!("{name}::{stream:?}: Camera finished streaming: {res:?}");
    }

    /// Unregister this hub (if it is still the registered one) and mark it
    /// stopped. Must be called with `HUBS` locked; safe to call twice.
    fn retire(&self, map: &mut HashMap<HubKey, Weak<StreamHub>>) {
        let key = (self.name.clone(), self.stream);
        if map
            .get(&key)
            .is_some_and(|cur| std::ptr::eq(cur.as_ptr(), self as *const StreamHub))
        {
            map.remove(&key);
        }
        self.stopped.cancel();
    }

    /// Store a frame for joiners and send it to every viewer.
    fn publish(&self, media: BcMedia) {
        let mut gop = self.gop.lock().unwrap_or_else(|e| e.into_inner());
        if let BcMedia::Iframe(frame) = &media {
            LAST_KEYFRAMES
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(
                    (self.name.clone(), self.stream),
                    LastKeyframe {
                        video_type: frame.video_type,
                        data: Arc::new(frame.data.clone()),
                        at: SystemTime::now(),
                    },
                );
            gop.clear();
            gop.push(media.clone());
        } else if !gop.is_empty() {
            if gop.len() >= HUB_GOP_MAX {
                gop.clear();
            } else {
                gop.push(media.clone());
            }
        }
        // No receivers is fine: viewers may be momentarily absent.
        let _ = self.tx.send(media);
    }

    /// Resolves once the stream has had no viewers for `HUB_LINGER`.
    async fn wait_unwatched(&self) {
        let mut viewers = self.viewers.subscribe();
        loop {
            if viewers.wait_for(|v| *v == 0).await.is_err() {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(HUB_LINGER) => {
                    // Re-check under the map lock, where joins happen, and retire
                    // the hub in the same critical section so no viewer can join
                    // a stream that is about to stop.
                    let mut map = HUBS.lock().unwrap_or_else(|e| e.into_inner());
                    if *self.viewers.borrow() == 0 {
                        self.retire(&mut map);
                        return;
                    }
                }
                r = viewers.wait_for(|v| *v > 0) => {
                    if r.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use neolink_core::bcmedia::model::{BcMediaAac, BcMediaIframe, BcMediaPframe, VideoType};

    fn iframe(us: u32) -> BcMedia {
        BcMedia::Iframe(BcMediaIframe {
            video_type: VideoType::H264,
            microseconds: us,
            time: None,
            data: vec![0, 0, 0, 1, 0x65],
        })
    }
    fn pframe(us: u32) -> BcMedia {
        BcMedia::Pframe(BcMediaPframe {
            video_type: VideoType::H264,
            microseconds: us,
            data: vec![0, 0, 0, 1, 0x41],
        })
    }
    fn aac() -> BcMedia {
        BcMedia::Aac(BcMediaAac {
            data: vec![0xff, 0xf1],
        })
    }
    fn micros(m: &BcMedia) -> Option<u32> {
        match m {
            BcMedia::Iframe(f) => Some(f.microseconds),
            BcMedia::Pframe(f) => Some(f.microseconds),
            _ => None,
        }
    }
    fn lock() -> std::sync::MutexGuard<'static, HashMap<HubKey, Weak<StreamHub>>> {
        HUBS.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[tokio::test]
    async fn joiner_replays_from_last_keyframe_then_goes_live() {
        let hub = StreamHub::new("test-replay", StreamKind::Main);
        // Frames before the first keyframe are useless to a decoder: not kept.
        hub.publish(pframe(1));
        hub.publish(iframe(10));
        hub.publish(pframe(11));
        hub.publish(aac());
        hub.publish(iframe(20)); // new GOP replaces the old one
        hub.publish(pframe(21));

        let (snapshot, mut rx, viewers) = {
            let _m = lock();
            hub.attach()
        };
        assert_eq!(viewers, 1);
        let got: Vec<_> = snapshot.iter().map(micros).collect();
        assert_eq!(got, vec![Some(20), Some(21)]);

        hub.publish(pframe(22));
        assert_eq!(micros(&rx.recv().await.unwrap()), Some(22));
    }

    #[tokio::test]
    async fn gop_buffer_is_bounded() {
        let hub = StreamHub::new("test-bound", StreamKind::Sub);
        hub.publish(iframe(0));
        for i in 0..(HUB_GOP_MAX as u32 + 10) {
            hub.publish(pframe(i + 1));
        }
        assert!(hub.gop.lock().unwrap().len() <= HUB_GOP_MAX);
    }

    #[tokio::test]
    async fn stops_only_after_linger_with_no_viewers() {
        let hub = StreamHub::new("test-linger", StreamKind::Main);
        lock().insert(
            ("test-linger".into(), StreamKind::Main),
            Arc::downgrade(&hub),
        );
        let viewer = {
            let _m = lock();
            hub.attach();
            ViewerGuard(hub.clone())
        };
        let waiter = tokio::spawn({
            let hub = hub.clone();
            async move { hub.wait_unwatched().await }
        });

        drop(viewer); // last viewer leaves
        tokio::time::sleep(HUB_LINGER / 2).await;
        assert!(!waiter.is_finished(), "must linger before stopping");
        assert!(!hub.stopped.is_cancelled());

        tokio::time::sleep(HUB_LINGER).await;
        waiter.await.unwrap();
        assert!(hub.stopped.is_cancelled(), "retired after the linger");
        assert!(
            lock()
                .get(&("test-linger".into(), StreamKind::Main))
                .is_none(),
            "a stopped hub is no longer joinable"
        );
    }

    #[tokio::test]
    async fn rejoin_during_linger_keeps_stream_running() {
        let hub = StreamHub::new("test-rejoin", StreamKind::Main);
        let first = {
            let _m = lock();
            hub.attach();
            ViewerGuard(hub.clone())
        };
        let waiter = tokio::spawn({
            let hub = hub.clone();
            async move { hub.wait_unwatched().await }
        });
        drop(first);
        tokio::time::sleep(HUB_LINGER / 2).await;
        // A player reconnects before the linger ends (what go2rtc does).
        let second = {
            let _m = lock();
            hub.attach();
            ViewerGuard(hub.clone())
        };
        tokio::time::sleep(HUB_LINGER * 3).await;
        assert!(!waiter.is_finished(), "a returning viewer cancels the stop");
        assert!(!hub.stopped.is_cancelled());

        drop(second);
        tokio::time::sleep(HUB_LINGER * 2).await;
        waiter.await.unwrap();
        assert!(hub.stopped.is_cancelled());
    }
}

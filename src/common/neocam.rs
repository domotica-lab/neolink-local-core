//! This is the common code for creating a camera instance
//!
//! Features:
//!    Shared stream BC delivery
//!    Common restart code
//!    Clonable interface to share amongst threadsanyhow::anyhow;
use anyhow::Context;
use futures::stream::StreamExt;
use std::sync::Weak;
use tokio::{
    sync::{
        broadcast::Receiver as BroadcastReceiver,
        mpsc::{channel as mpsc, Sender as MpscSender},
        oneshot::{channel as oneshot, Sender as OneshotSender},
        watch::{channel as watch, Receiver as WatchReceiver, Sender as WatchSender},
    },
    task::JoinSet,
    time::{sleep, Duration, Instant},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use super::{
    DoorbellEvent, MdRequest, MdState, NeoCamMdThread, NeoCamThread, NeoCamThreadState,
    NeoInstance, Permit, UseCounter,
};
use crate::{
    config::{CameraConfig, ConnectMode},
    AnyResult, Result,
};
use neolink_core::bc_protocol::BcCamera;

pub(crate) enum NeoCamCommand {
    HangUp,
    Instance(OneshotSender<Result<NeoInstance>>),
    Motion(OneshotSender<WatchReceiver<MdState>>),
    Doorbell(OneshotSender<BroadcastReceiver<DoorbellEvent>>),
    Config(OneshotSender<WatchReceiver<CameraConfig>>),
    Disconnect(OneshotSender<()>),
    Connect(OneshotSender<()>),
    GetPermit(OneshotSender<Permit>),
}
/// The underlying camera binding
pub(crate) struct NeoCam {
    cancel: CancellationToken,
    config_watch: WatchSender<CameraConfig>,
    commander: MpscSender<NeoCamCommand>,
    camera_watch: WatchReceiver<Weak<BcCamera>>,
    set: JoinSet<AnyResult<()>>,
    created_at: Instant,
}

/// Spawn one of the camera's long-lived tasks. They only end when the camera
/// is being torn down, so one that stops on its own (error, unexpected
/// return or panic) leaves the camera half-working: e.g. no connect loop means
/// it never connects again. Cancelling the shared token instead marks the
/// whole camera as stopped, so the reactor rebuilds it and the RTSP side
/// re-attaches to the new one.
fn spawn_critical<F>(
    set: &mut JoinSet<AnyResult<()>>,
    camera: &str,
    label: &'static str,
    cancel: &CancellationToken,
    fut: F,
) where
    F: std::future::Future<Output = AnyResult<()>> + Send + 'static,
{
    let camera = camera.to_string();
    let cancel = cancel.clone();
    set.spawn(async move {
        // Cancels on drop, so a panic also tears the camera down.
        let guard = cancel.clone().drop_guard();
        let res = fut.await;
        if !cancel.is_cancelled() {
            match &res {
                Err(e) => log::error!("{camera}: {label} stopped: {e:?}; restarting the camera"),
                Ok(()) => {
                    log::warn!("{camera}: {label} stopped unexpectedly; restarting the camera")
                }
            }
        }
        drop(guard);
        res
    });
}

impl NeoCam {
    pub(crate) async fn new(config: CameraConfig) -> Result<NeoCam> {
        let (commander_tx, commander_rx) = mpsc(100);
        let (watch_config_tx, watch_config_rx) = watch(config.clone());
        let (camera_watch_tx, camera_watch_rx) = watch(Weak::new());
        let (md_request_tx, md_request_rx) = mpsc(100);
        // Start disconnected - camera will connect when first RTSP client arrives (via permit system)
        let (state_tx, state_rx) = watch(NeoCamThreadState::Disconnected);

        let set = JoinSet::new();
        let users = UseCounter::new().await;

        let cam_name = config.name.clone();
        let mut me = Self {
            created_at: Instant::now(),
            cancel: CancellationToken::new(),
            config_watch: watch_config_tx,
            commander: commander_tx.clone(),
            camera_watch: camera_watch_rx.clone(),
            set,
        };

        // This thread recieves messages from the instances
        // and acts on it.
        //
        // This thread must be started first so that we can begin creating instances for the
        // other threads
        let sender_cancel = me.cancel.clone();
        let mut commander_rx = ReceiverStream::new(commander_rx);
        let thread_commander_tx = commander_tx.clone();
        let thread_watch_config_rx = watch_config_rx.clone();

        spawn_critical(
            &mut me.set,
            &cam_name,
            "command loop",
            &me.cancel,
            async move {
                let thread_cancel = sender_cancel.clone();
                let res = tokio::select! {
                    _ = sender_cancel.cancelled() => {
                        Result::Ok(())
                    },
                    v = async {
                        while let Some(command) = commander_rx.next().await {
                            match command {
                                NeoCamCommand::HangUp => {
                                    sender_cancel.cancel();
                                    return Result::<(), anyhow::Error>::Ok(());
                                }
                                NeoCamCommand::Instance(result) => {
                                    let instance = NeoInstance::new(
                                        camera_watch_rx.clone(),
                                        thread_commander_tx.clone(),
                                        thread_cancel.clone(),
                                    );
                                    let _ = result.send(instance);
                                }
                                // If the motion thread is gone the request (and its
                                // oneshot) is dropped, so only that caller sees an
                                // error. Propagating it with `?` would end this
                                // command loop and leave every other user of the
                                // camera (permits, connect, config) talking to a dead
                                // channel.
                                NeoCamCommand::Motion(sender) => {
                                    if md_request_tx.send(MdRequest::Get { sender }).await.is_err() {
                                        log::warn!("Motion watcher requested but the motion thread has stopped");
                                    }
                                },
                                NeoCamCommand::Doorbell(sender) => {
                                    if md_request_tx.send(MdRequest::GetDoorbell { sender }).await.is_err() {
                                        log::warn!("Doorbell watcher requested but the motion thread has stopped");
                                    }
                                },
                                NeoCamCommand::Config(sender) => {
                                    let _ = sender.send(thread_watch_config_rx.clone());
                                },
                                NeoCamCommand::Connect(sender) => {
                                    if !matches!(*state_tx.borrow(), NeoCamThreadState::Connected) {
                                        state_tx.send_replace(NeoCamThreadState::Connected);
                                    }
                                    let _ = sender.send(());
                                }
                                NeoCamCommand::Disconnect(sender) => {
                                    if !matches!(*state_tx.borrow(), NeoCamThreadState::Disconnected) {
                                        state_tx.send_replace(NeoCamThreadState::Disconnected);
                                    }
                                    let _ = sender.send(());
                                }
                                NeoCamCommand::GetPermit(sender) => {
                                    match users.create_activated().await {
                                        Ok(permit) => {
                                            let _ = sender.send(permit);
                                        }
                                        Err(e) => {
                                            // The use counter is owned by this task, so this
                                            // cannot really fail; if it does, fail only this
                                            // request (the caller sees a closed channel).
                                            log::warn!("Could not create a camera use permit: {e:?}");
                                        }
                                    }
                                }
                            }
                        }
                        Ok(())
                    } => {
                        v
                    }
                };
                res
            },
        );

        // This gets the first instance which we use for making the other threads
        let (instance_tx, instance_rx) = oneshot();
        commander_tx
            .send(NeoCamCommand::Instance(instance_tx))
            .await?;
        let instance = instance_rx.await??;

        // This thread maintains the camera loop
        //
        // It will keep it logged and reconnect
        let thread_watch_config_rx = watch_config_rx.clone();
        let mut cam_thread = NeoCamThread::new(
            state_rx,
            thread_watch_config_rx,
            camera_watch_tx,
            me.cancel.clone(),
        )
        .await;
        spawn_critical(
            &mut me.set,
            &cam_name,
            "connection thread",
            &me.cancel,
            async move { cam_thread.run().await },
        );

        // This thread monitors the motion
        let md_instance = instance.subscribe().await?;
        let md_cancel = me.cancel.clone();
        let mut md_thread = NeoCamMdThread::new(md_request_rx, md_instance).await?;
        spawn_critical(
            &mut me.set,
            &cam_name,
            "motion thread",
            &me.cancel,
            async move {
                tokio::select! {
                    _ = md_cancel.cancelled() => AnyResult::Ok(()),
                    v = md_thread.run() => {
                        v
                    },
                }
            },
        );

        // Camera info reporting removed - was triggering connection at startup
        // Model/firmware info and UID queries now happen only when camera is already
        // connected for another reason (RTSP client, MQTT command, etc.)
        let _report_instance = instance.subscribe().await?;
        let _uid_instance = instance.clone();

        // MD permits
        let md_permit_instance = instance.subscribe().await?;
        let md_permit_cancel = me.cancel.clone();
        spawn_critical(
            &mut me.set,
            &cam_name,
            "motion permit watcher",
            &me.cancel,
            async move {
                tokio::select! {
                    _ = md_permit_cancel.cancelled() => {
                        AnyResult::Ok(())
                    },
                    v = async {
                        let mut md = md_permit_instance.motion().await.with_context(|| "Unable to acquire motion watcher")?;
                        loop{
                            md.wait_for(|md| matches!(md, MdState::Start(_))).await.with_context(|| "MD Watcher lost")?;
                            let _permit = md_permit_instance.permit().await.with_context(|| "Unuable to acquire motion permit")?;
                            md.wait_for(|md| matches!(md, MdState::Stop(_))).await.with_context(|| "MD Watcher lost")?;
                            // Try waiting for 30s
                            // If in those 30s we get motion then return to
                            // loop early to reaquire the permit
                            tokio::select!{
                                _ = sleep(Duration::from_secs(30)) => {},
                                v = md.wait_for(|md| matches!(md, MdState::Start(_))) => {v.with_context(|| "MD Watcher lost")?;},
                            }
                        }
                    } => {
                        v
                    },
                }
            },
        );

        // This thread manages the camera connection lifecycle per `connect_mode`:
        //   - Always:    connect at startup and stay connected; if idle_timeout_secs > 0,
        //                disconnect after that long idle and reconnect on demand.
        //   - OnDemand:  connect only when a permit is held; disconnect when idle, after
        //                an optional relay_warm_seconds grace.
        // Permits are created when RTSP clients connect or when other tasks need the camera.
        // The loop re-reads the config each cycle so changes take effect.
        let connect_instance = instance.subscribe().await?;
        let connect_cancel = me.cancel.clone();
        spawn_critical(
            &mut me.set,
            &cam_name,
            "connect lifecycle",
            &me.cancel,
            async move {
                tokio::select! {
                    _ = connect_cancel.cancelled() => {
                        AnyResult::Ok(())
                    },
                    v = async {
                        let mut permit = connect_instance.permit().await?;
                        permit.deactivate().await?; // Watching only, don't count as active

                        let mut config_rx = connect_instance.config().await?;
                        let name = config_rx.borrow().name.clone();

                        'lifecycle: loop {
                            // Snapshot the mode (and mark the current config as seen so a
                            // later `changed()` only fires on a genuine config change).
                            let mode = config_rx.borrow_and_update().connect_mode;
                            match mode {
                                ConnectMode::Always => {
                                    // Connect at startup / stay connected (idempotent).
                                    connect_instance.connect().await?;
                                    let idle_timeout = config_rx.borrow().idle_timeout_secs;
                                    if idle_timeout == 0 {
                                        // Never idle-disconnect; just wait for a config change.
                                        if config_rx.changed().await.is_err() {
                                            break;
                                        }
                                        continue;
                                    }
                                    // idle_timeout > 0: disconnect after being idle that long.
                                    tokio::select! {
                                        r = config_rx.changed() => { if r.is_err() { break; } continue; }
                                        r = permit.dropped_users() => {
                                            r?;
                                            let idle_deadline = sleep(Duration::from_secs(idle_timeout));
                                            tokio::pin!(idle_deadline);
                                            tokio::select! {
                                                _ = &mut idle_deadline => {
                                                    log::info!("{name}: idle {idle_timeout}s, disconnecting (connect_mode=always)");
                                                    connect_instance.disconnect().await?;
                                                    // Reconnect on demand (or re-evaluate on config change).
                                                    tokio::select! {
                                                        r = permit.aquired_users() => { r?; }
                                                        r = config_rx.changed() => { if r.is_err() { break; } }
                                                    }
                                                }
                                                // Re-used before timeout: stay connected.
                                                r = permit.aquired_users() => { r?; }
                                                r = config_rx.changed() => { if r.is_err() { break; } }
                                            }
                                            continue;
                                        }
                                    }
                                }
                                ConnectMode::OnDemand => {
                                    // Wait for demand before connecting.
                                    tokio::select! {
                                        r = config_rx.changed() => { if r.is_err() { break; } continue; }
                                        r = permit.aquired_users() => { r?; }
                                    }
                                    log::info!("{name}: Permit acquired, connecting to camera relay");
                                    connect_instance.connect().await?;

                                    // Stay connected until all permits drop, plus a warm grace.
                                    loop {
                                        // Wait for users to drop, but also react to config
                                        // changes (e.g. connect_mode) while connected. Users
                                        // are present here, so re-evaluating re-reads the mode
                                        // without dropping the active stream (connect() is
                                        // idempotent and aquired_users() returns immediately).
                                        tokio::select! {
                                            r = permit.dropped_users() => { r?; }
                                            r = config_rx.changed() => {
                                                if r.is_err() { break 'lifecycle; }
                                                log::info!("{name}: config changed; re-evaluating connect mode");
                                                continue 'lifecycle;
                                            }
                                        }
                                        let warm_secs = config_rx.borrow().relay_warm_seconds;
                                        if warm_secs == 0 {
                                            break;
                                        }
                                        log::info!(
                                            "{name}: All permits dropped, keeping relay warm for {}s",
                                            warm_secs
                                        );
                                        let warm_deadline = sleep(Duration::from_secs(warm_secs));
                                        tokio::pin!(warm_deadline);
                                        tokio::select! {
                                            _ = &mut warm_deadline => {
                                                break;
                                            }
                                            v = permit.aquired_users() => {
                                                v?;
                                                log::info!("{name}: Permit reacquired during warm relay window");
                                                continue;
                                            }
                                            r = config_rx.changed() => {
                                                if r.is_err() { break 'lifecycle; }
                                                // No active users during the warm window, so
                                                // ending it (disconnect) to re-evaluate the mode
                                                // is safe and disrupts nothing.
                                                log::info!("{name}: config changed during warm window; re-evaluating");
                                                break;
                                            }
                                        }
                                    }

                                    log::info!("{name}: All permits dropped, disconnecting from camera relay");
                                    connect_instance.disconnect().await?;
                                }
                            }
                        }
                        AnyResult::Ok(())
                    } => {
                        v
                    },
                }
            },
        );

        Ok(me)
    }

    /// Whether this camera's task set is still live. A `NeoCam` cancels its
    /// shared `cancel` token when its tasks stop (notably on a fatal
    /// `CameraLoginFail`, where every task is torn down). Once cancelled the
    /// instance is a zombie — its command channel is dead — so the reactor uses
    /// this to decide whether to recreate it instead of handing it back.
    pub(crate) fn is_alive(&self) -> bool {
        !self.cancel.is_cancelled()
    }

    /// How long ago this camera was created. The reactor uses it to rebuild a
    /// stopped camera at most once per cooldown, so a camera whose login keeps
    /// failing is not hammered (and its account not locked) by every status
    /// poll.
    pub(crate) fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    pub(crate) async fn subscribe(&self) -> Result<NeoInstance> {
        NeoInstance::new(
            self.camera_watch.clone(),
            self.commander.clone(),
            self.cancel.clone(),
        )
    }

    /// Push a new config to this camera's tasks. Nothing is sent when it is
    /// identical to the current one: every receiver treats a change as a
    /// reason to act (the connection thread reconnects on any change), so a
    /// reload that only touched another camera must not drop this one.
    pub(crate) async fn update_config(&self, config: CameraConfig) -> Result<()> {
        self.config_watch.send_if_modified(|current| {
            if *current == config {
                false
            } else {
                *current = config;
                true
            }
        });
        Ok(())
    }
}

impl Drop for NeoCam {
    fn drop(&mut self) {
        log::trace!("Drop NeoCam");
        let mut set = std::mem::take(&mut self.set);
        let commander = self.commander.clone();
        let _gt = tokio::runtime::Handle::current().enter();
        tokio::task::spawn(async move {
            let _ = commander.send(NeoCamCommand::HangUp).await;
            while set.join_next().await.is_some() {}
            log::trace!("Dropped NeoCam");
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_failed_task_stops_the_camera() {
        let cancel = CancellationToken::new();
        let mut set = JoinSet::new();
        spawn_critical(&mut set, "cam", "test task", &cancel, async {
            Err(anyhow::anyhow!("boom"))
        });
        set.join_next().await;
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn an_unexpected_return_or_panic_stops_the_camera() {
        let cancel = CancellationToken::new();
        let mut set = JoinSet::new();
        spawn_critical(&mut set, "cam", "test task", &cancel, async { Ok(()) });
        set.join_next().await;
        assert!(cancel.is_cancelled());

        let cancel = CancellationToken::new();
        let mut set = JoinSet::new();
        spawn_critical(&mut set, "cam", "test task", &cancel, async {
            panic!("task panicked")
        });
        assert!(set.join_next().await.unwrap().is_err());
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn a_running_task_leaves_the_camera_alive() {
        let cancel = CancellationToken::new();
        let mut set = JoinSet::new();
        spawn_critical(&mut set, "cam", "test task", &cancel, async {
            futures::future::pending::<()>().await;
            Ok(())
        });
        tokio::task::yield_now().await;
        assert!(!cancel.is_cancelled());
        set.abort_all();
    }
}

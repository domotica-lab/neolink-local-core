use super::*;

use crate::common::UseCounter;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use neolink_core::{bc_protocol::StreamKind, bcmedia::model::BcMedia};
use tokio::sync::mpsc::Receiver as MpscReceiver;

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
    /// The spawned task holds a use-permit (via `run_task`) for as long as it
    /// runs. It must therefore stop as soon as the consumer drops the returned
    /// receiver, even if the camera is not connected yet or never sends another
    /// frame (e.g. a second `start_video` on the same stream took over the
    /// camera's media). Otherwise the permit leaks and an `on_demand` camera
    /// never disconnects.
    pub(crate) async fn stream(&self, stream: StreamKind) -> AnyResult<MpscReceiver<BcMedia>> {
        let (media_tx, media_rx) = tokio::sync::mpsc::channel(MEDIA_CHANNEL_CAP);
        let config = self.config().await?.borrow().clone();
        let strict = config.strict;
        let name = config.name.clone();
        let thread_camera = self.clone();
        tokio::task::spawn(async move {
            let closed_tx = media_tx.clone();
            let res = tokio::select! {
                // Consumer gone: dropping the run_task future releases its permit
                // and drops the camera StreamData (which sends the video stop).
                _ = closed_tx.closed() => {
                    log::info!("{name}::{stream:?}: Consumer closed, stopping camera stream task and releasing permit");
                    AnyResult::Ok(())
                }
                v = thread_camera.run_task(move |cam| {
                    let media_tx = media_tx.clone();
                    Box::pin(async move {
                        let mut media_stream = cam.start_video(stream, 0, strict).await?;
                        log::trace!("Camera started");
                        while let Ok(media) = media_stream.get_data().await? {
                            media_tx.send(media).await?;
                        }
                        AnyResult::Ok(())
                    })
                }) => v,
            };
            log::debug!("{name}::{stream:?}: Camera finished streaming: {res:?}");
        });

        Ok(media_rx)
    }
}

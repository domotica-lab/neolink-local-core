//! Used to track number of users of a service
use tokio::{
    sync::{
        mpsc::{channel as mpsc, Sender as MpscSender},
        watch::{channel as watch, Receiver as WatchReceiver},
    },
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{AnyResult, Result};

/// Counts the active users of the stream
pub(crate) struct UseCounter {
    value: WatchReceiver<u32>,
    notifier_tx: MpscSender<bool>,
    cancel: CancellationToken,
    set: JoinSet<AnyResult<()>>,
}

impl UseCounter {
    pub(crate) async fn new() -> Self {
        let (notifier_tx, mut notifier) = mpsc(100);
        let (value_tx, value) = watch(0);
        let cancel = CancellationToken::new();
        let mut set = JoinSet::new();

        let thread_cancel = cancel.clone();
        set.spawn(async move {
            let r = tokio::select! {
                _ = thread_cancel.cancelled() => {
                    AnyResult::Ok(())
                },
                v = async {
                    while let Some(noti) = notifier.recv().await {
                        value_tx.send_modify(|value| {
                            if noti {
                                log::trace!("Usecounter: {}->{}", *value, (*value) + 1);
                                *value += 1;
                            } else if *value == 0 {
                                // Never wrap: a wrapped count would look like
                                // ~4 billion users and keep the camera
                                // connected forever.
                                log::warn!("Usecounter: release without a matching acquire; ignored");
                            } else {
                                log::trace!("Usecounter: {}->{}", *value, (*value) - 1);
                                *value -= 1;
                            }
                        });
                    }
                    AnyResult::Ok(())
                } => v,
            };
            log::trace!("End Use Counter: {r:?}");
            r
        });
        Self {
            value,
            notifier_tx,
            cancel,
            set,
        }
    }

    pub(crate) async fn create_activated(&self) -> Result<Permit> {
        let mut res = Permit::new(self);
        res.activate().await?;
        Ok(res)
    }

    #[cfg_attr(not(feature = "gstreamer"), allow(dead_code))]
    pub(crate) async fn create_deactivated(&self) -> Result<Permit> {
        Ok(Permit::new(self))
    }
}

impl Drop for UseCounter {
    fn drop(&mut self) {
        log::trace!("Drop UseCounter");
        self.cancel.cancel();

        let mut set = std::mem::take(&mut self.set);
        let _gt = tokio::runtime::Handle::current().enter();
        tokio::task::spawn(async move { while set.join_next().await.is_some() {} });
        log::trace!("Dropped UseCounter");
    }
}

pub(crate) struct Permit {
    is_active: bool,
    value: WatchReceiver<u32>,
    notifier: MpscSender<bool>,
}

impl Permit {
    fn new(source: &UseCounter) -> Self {
        Self {
            is_active: false,
            value: source.value.clone(),
            notifier: source.notifier_tx.clone(),
        }
    }

    // The flag changes only once the message is sent: if the caller drops
    // the future mid-send, nothing was counted and the flag still matches the
    // counter, so the drop does not release a use that was never added (which
    // would take one away from another user and could disconnect it).
    pub(crate) async fn activate(&mut self) -> Result<()> {
        if !self.is_active {
            self.notifier.send(true).await?;
            self.is_active = true;
        }
        Ok(())
    }

    pub(crate) async fn deactivate(&mut self) -> Result<()> {
        if self.is_active {
            self.notifier.send(false).await?;
            self.is_active = false;
        }
        Ok(())
    }

    pub(crate) async fn aquired_users(&self) -> Result<()> {
        self.value
            .clone()
            .wait_for(|curr| {
                log::trace!("acquired_users: {}", *curr);
                *curr > 0
            })
            .await?;
        Ok(())
    }

    pub(crate) async fn dropped_users(&self) -> Result<()> {
        self.value
            .clone()
            .wait_for(|curr| {
                log::trace!("dropped_users: {}", *curr);
                *curr == 0
            })
            .await?;
        Ok(())
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if self.is_active {
            self.is_active = false;
            // Usually there is room: release right away, without a task.
            match self.notifier.try_send(false) {
                Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    // A release must never be lost (the camera would stay
                    // connected), so queue it. `try_current` keeps a drop
                    // outside the runtime (e.g. on a GStreamer thread) from
                    // panicking.
                    match tokio::runtime::Handle::try_current() {
                        Ok(handle) => {
                            let notifier = self.notifier.clone();
                            handle.spawn(async move {
                                let _ = notifier.send(false).await;
                            });
                        }
                        Err(_) => {
                            let _ = self.notifier.blocking_send(false);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn count_settles_at(permit: &Permit, want: u32) {
        let mut value = permit.value.clone();
        tokio::time::timeout(Duration::from_secs(2), value.wait_for(|v| *v == want))
            .await
            .expect("use count did not settle")
            .unwrap();
    }

    #[tokio::test]
    async fn permits_count_users_and_release_on_drop() {
        let counter = UseCounter::new().await;
        let watcher = counter.create_deactivated().await.unwrap();
        let a = counter.create_activated().await.unwrap();
        let mut b = counter.create_activated().await.unwrap();
        count_settles_at(&watcher, 2).await;
        b.deactivate().await.unwrap();
        b.deactivate().await.unwrap(); // idempotent
        count_settles_at(&watcher, 1).await;
        drop(a);
        count_settles_at(&watcher, 0).await;
        drop(b); // inactive: releases nothing
        b = counter.create_activated().await.unwrap();
        count_settles_at(&watcher, 1).await;
        drop(b);
        count_settles_at(&watcher, 0).await;
    }
}

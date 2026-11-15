# Connection modes & lifecycle

How and when Neolink opens, holds, and closes a camera connection. Configured
per-camera by `connect_mode`; implemented by the on-demand connect loop
(`src/common/neocam.rs`) and the connection thread (`src/common/camthread.rs`).

## `connect_mode`

```toml
[[cameras]]
connect_mode = "always"      # or "on_demand"   (default: always)
```

| Mode | Behaviour |
|---|---|
| `always` (default) | Connect at startup and stay connected, reconnecting on drops. Optionally idle-disconnect after `idle_timeout_secs`. |
| `on_demand` | Connect only when something needs the camera; disconnect when nothing does, after a `relay_warm_seconds` grace. |

Config aliases: `connect`, `connection_mode`; values also accept `always`/`connected`/`on`
and `ondemand`/`demand`/`lazy`.

### Related settings

| Setting | Default | Applies to | Meaning |
|---|---|---|---|
| `idle_timeout_secs` (alias `idle_timeout`, `idle_secs`) | `0` (never) | `always` | Disconnect after this many seconds with no permit holders; reconnect on demand. `0` = stay connected forever. |
| `relay_warm_seconds` (alias `relay_warm`, `relay_warm_secs`) | `60` | `on_demand` | Keep the connection warm for this many seconds after the last permit drops, in case a new request arrives. `0` = disconnect immediately. |

The loop re-reads the config each cycle, so `connect_mode` and these values can change
at runtime (e.g. via the MQTT config topic).

## What counts as "needing" the camera

The camera stays connected while at least one **permit** is held (see
[architecture.md](architecture.md) → UseCounter / Permit). Permits are taken by:

- an **RTSP client** connected to a stream path,
- an **MQTT command** or any **one-shot CLI subcommand** (image, battery, ptz, talk,
  …) for the duration of the call,
- an active **motion event** (held until 30 s after motion stops).

When the use-count goes from 0 → ≥1 the loop connects; when it returns to 0 it starts
the disconnect grace.

## The on-demand connect loop

`on_demand`:

1. Wait for `aquired_users()` (use-count > 0) → log "Permit acquired, connecting to
   camera relay" and `connect()`.
2. Wait for `dropped_users()` (use-count back to 0).
3. If `relay_warm_seconds == 0`, disconnect immediately. Otherwise keep the
   connection warm for that window; if a permit is re-acquired during it, stay
   connected and continue; if the window expires, "All permits dropped, disconnecting
   from camera relay" and `disconnect()`.

`always`:

1. `connect()` at startup.
2. If `idle_timeout_secs == 0`, stay connected (only react to config changes).
3. Otherwise, when the use-count hits 0, start the idle timer; on expiry log "idle
   Ns, disconnecting (connect_mode=always)" and `disconnect()`, then reconnect when a
   permit is next acquired.

`connect()` / `disconnect()` only flip the `NeoCamThreadState` watch
(`Connected`/`Disconnected`); the actual transport work happens in `NeoCamThread`.

## Connection thread, reconnect & backoff

`NeoCamThread` (`camthread.rs`) gates on the state watch (`wait_for(Connected)`) and
then performs discovery + login (see [connection-and-bandwidth.md](connection-and-bandwidth.md)
and [discovery-handshake.md](discovery-handshake.md)).

- On a successful connect it wraps the camera in an `Arc<BcCamera>` and publishes
  `Arc::downgrade(&camera)` over a `watch<Weak<BcCamera>>`; this is how in-flight
  tasks observe a reconnect and re-run against the new object.
- **Exponential backoff** on reconnection: starts at 50 ms, doubles per failure,
  capped at 5 s. The backoff resets to the minimum only if the previous connection
  lasted longer than 60 s.
- A 5 s keep-alive ping (`get_linktype`) tolerates up to 5 missed pings before
  declaring the link dead; cameras that don't support pings are not timed out this way.
- **Fatal vs retryable:** a login failure (`CameraLoginFail`) is fatal for that
  camera instance — it cancels and stops (no reconnect loop of its own). All other
  losses are retried with backoff, including a connection that ends without an error
  (the camera closed it): only a requested shutdown stops the loop.
- On exit it best-effort logs out and shuts the camera down. A live connection is also
  torn down early if the camera's config changes (an identical config pushed by a
  reload is ignored) or the state flips to `Disconnected`.
- **Detecting a lost link:** a remote close ends `BcConnection::join()` with
  `DroppedConnection` at once. TCP connections have a 10 s connect timeout,
  keepalive (10 s idle, 5 s × 3 probes) and a 30 s user timeout on Linux, so a camera
  that loses power is noticed in seconds rather than after kernel retransmit limits.

## Recovering a stopped camera

Every long-lived task of a `NeoCam` (command loop, connection thread, motion thread,
motion-permit watcher, connect lifecycle) cancels the camera's shared token if it
stops on its own — error, unexpected return or panic — so a camera is never left
half-working. A stopped camera is rebuilt from the current config:

- by the reactor on the next `get(name)`, at most once per 60 s per camera (so a
  rejected login is not retried on every status-API poll and does not lock the
  camera account), and always on a config reload;
- the RTSP side supervises each camera (`supervise_camera` in `src/rtsp/mod.rs`):
  when its stream task fails or its `NeoInstance` reports `stopped()`, it fetches a
  fresh instance and restarts, with a 1 s → 60 s backoff that resets after a minute of
  healthy running. MQTT restarts its per-camera handler every 5 s the same way.

## Waking a fully-disconnected camera

An `on_demand` camera that is currently disconnected is woken by anything that takes a
permit: an RTSP client connecting, an MQTT command, the `/control/wakeup` topic, or a
motion event. (Push notifications were removed — see the changelog.)

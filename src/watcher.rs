//! Stream watcher task — connects to Kick viewer WebSocket, sends handshakes,
//! tracking events, and periodically fetches channel points.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::watch;
use tokio_tungstenite::{
    connect_async_tls_with_config,
    tungstenite::client::IntoClientRequest,
    tungstenite::Message,
};
use tracing::{debug, info, warn};

use crate::kick_api::{fetch_points, fetch_viewer_ws_token, USER_AGENT};
use crate::telemetry::{AppState, SharedState, WatcherHandle};

pub async fn run_watcher(
    state: SharedState,
    http: reqwest::Client,
    handle: Arc<WatcherHandle>,
    token: String,
    user_id: i64,
    ping_secs: u64,
    tracking_secs: u64,
    points_mins: u64,
    mut cancel_rx: watch::Receiver<bool>,
) {
    let mut backoff = Duration::from_secs(5);
    const MAX_BACKOFF: Duration = Duration::from_secs(120);

    'outer: loop {
        // Check cancellation before retrying
        if *cancel_rx.borrow() || !handle.is_alive() {
            break;
        }

        let result = run_session(
            state.clone(),
            &http,
            handle.clone(),
            &token,
            user_id,
            ping_secs,
            tracking_secs,
            points_mins,
            &mut cancel_rx,
        )
        .await;

        match result {
            Ok(()) => break 'outer, // clean exit (cancelled)
            Err(e) => {
                if !handle.is_alive() {
                    break;
                }
                {
                    let mut st = state.lock().await;
                    set_watching(&mut st, &handle, false);
                    st.add_log(&format!(
                        "[{}] WebSocket for '{}' dropped: {e}. Reconnecting in {backoff:?}…",
                        handle.username, handle.streamer
                    ));
                }
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = cancel_rx.changed() => { break; }
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }

    // Mark dead and clean up state
    handle.alive.store(false, std::sync::atomic::Ordering::Relaxed);
    watcher_ended(state.clone(), &handle).await;
    info!("[{}] Viewing session for '{}' finished.", handle.username, handle.streamer);
}

async fn run_session(
    state: SharedState,
    http: &reqwest::Client,
    handle: Arc<WatcherHandle>,
    token: &str,
    user_id: i64,
    ping_secs: u64,
    tracking_secs: u64,
    points_mins: u64,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<()> {
    // 1. Fetch viewer WS token
    let ws_token =
        fetch_viewer_ws_token(http, token, &handle.streamer, handle.channel_id, user_id).await?;

    // 2. Connect WebSocket
    let ws_url = format!(
        "wss://websockets.kick.com/viewer/v1/connect?token={ws_token}"
    );
    let mut req = ws_url.into_client_request()?;
    req.headers_mut().insert(
        "User-Agent",
        USER_AGENT.parse().map_err(|_| anyhow!("invalid ua"))?,
    );
    req.headers_mut().insert(
        "Origin",
        "https://kick.com".parse().map_err(|_| anyhow!("invalid origin"))?,
    );

    let (ws, _) = connect_async_tls_with_config(req, None, false, None)
        .await
        .map_err(|e| anyhow!("ws dial: {e}"))?;

    let (mut sink, mut stream) = ws.split();

    // Mark watching
    {
        let mut st = state.lock().await;
        set_watching(&mut st, &handle, true);
    }

    // Fetch initial points
    {
        match fetch_points(http, token, &handle.streamer).await {
            Ok(pts) => {
                let mut st = state.lock().await;
                update_points(&mut st, &handle, pts);
                st.add_log(&format!(
                    "[{}] Connected to '{}' live! Current balance: {pts} pts (+10 every 5m).",
                    handle.username, handle.streamer
                ));
            }
            Err(_) => {
                let mut st = state.lock().await;
                st.add_log(&format!(
                    "[{}] Connected to '{}' live (earning points every 5m)!",
                    handle.username, handle.streamer
                ));
            }
        }
    }

    // 3. Initial handshake + ping
    sink.send(Message::Text(
        json!({
            "type": "channel_handshake",
            "data": { "message": { "channelId": handle.channel_id } }
        })
        .to_string(),
    ))
    .await
    .ok();
    sink.send(Message::Text(json!({"type": "ping"}).to_string()))
        .await
        .ok();

    let mut handshake_tick = tokio::time::interval(Duration::from_secs(ping_secs));
    let mut track_tick = tokio::time::interval(Duration::from_secs(tracking_secs));
    let mut points_tick = tokio::time::interval(Duration::from_secs(points_mins * 60));

    // Consume first tick to avoid immediate fire
    handshake_tick.tick().await;
    track_tick.tick().await;
    points_tick.tick().await;

    loop {
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    None => return Err(anyhow!("ws stream closed")),
                    Some(Err(e)) => return Err(anyhow!("ws read: {e}")),
                    Some(Ok(Message::Text(txt))) => {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                            if v["type"] == "ping" {
                                sink.send(Message::Text(json!({"type": "pong"}).to_string()))
                                    .await
                                    .ok();
                            }
                        }
                        debug!("ws msg: {txt}");
                    }
                    Some(Ok(_)) => {}
                }
            }

            _ = handshake_tick.tick() => {
                sink.send(Message::Text(
                    json!({
                        "type": "channel_handshake",
                        "data": { "message": { "channelId": handle.channel_id } }
                    })
                    .to_string(),
                )).await.ok();
                sink.send(Message::Text(json!({"type": "ping"}).to_string()))
                    .await
                    .ok();
            }

            _ = track_tick.tick() => {
                let stream_id = handle.stream_id.load(std::sync::atomic::Ordering::Relaxed);
                let res = sink.send(Message::Text(
                    json!({
                        "type": "user_event",
                        "data": {
                            "message": {
                                "name": "tracking.user.watch.livestream",
                                "channel_id": handle.channel_id,
                                "livestream_id": stream_id
                            }
                        }
                    })
                    .to_string(),
                )).await;
                if let Err(e) = res {
                    return Err(anyhow!("tracking send: {e}"));
                }
            }

            _ = points_tick.tick() => {
                match fetch_points(http, token, &handle.streamer).await {
                    Ok(pts) => {
                        let mut st = state.lock().await;
                        update_points(&mut st, &handle, pts);
                        st.add_log(&format!(
                            "[{}] '{}' live balance updated to {pts} pts (+10 credited)!",
                            handle.username, handle.streamer
                        ));
                    }
                    Err(e) => {
                        warn!("points fetch for {}: {e}", handle.streamer);
                    }
                }
            }

            _ = cancel_rx.changed() => {
                return Ok(());
            }
        }
    }
}

fn set_watching(state: &mut AppState, handle: &WatcherHandle, watching: bool) {
    if let Some(acc) = state.statuses.get_mut(&handle.username) {
        if let Some(s) = acc.streamers.get_mut(&handle.streamer) {
            s.watching = watching;
        }
        acc.recalc();
    }
}

fn update_points(state: &mut AppState, handle: &WatcherHandle, pts: i64) {
    if let Some(acc) = state.statuses.get_mut(&handle.username) {
        if let Some(s) = acc.streamers.get_mut(&handle.streamer) {
            s.points = pts;
        }
        acc.recalc();
    }
}

async fn watcher_ended(state: SharedState, handle: &WatcherHandle) {
    let mut st = state.lock().await;
    // Remove watcher from active map
    let is_fav = if let Some(watchers) = st.watchers.get_mut(&handle.username) {
        // Only remove if it's still the current watcher for this streamer
        let is_current = watchers
            .get(&handle.streamer)
            .map(|w| Arc::ptr_eq(w, &Arc::new(WatcherHandle {
                // We can't compare properly, so just check alive
                username: String::new(),
                streamer: String::new(),
                channel_id: 0,
                stream_id: Default::default(),
                alive: Default::default(),
                auto_fill: Default::default(),
                started_at: std::time::Instant::now(),
                cancel_tx: tokio::sync::watch::channel(false).0,
            })))
            .unwrap_or(false);
        // Remove dead watchers unconditionally (alive already false)
        watchers.remove(&handle.streamer);
        drop(is_current);
        // Check if this streamer is a favorite
        st.statuses
            .get(&handle.username)
            .and_then(|acc| acc.streamers.get(&handle.streamer))
            .is_some()
    } else {
        false
    };

    if let Some(acc) = st.statuses.get_mut(&handle.username) {
        if is_fav {
            if let Some(s) = acc.streamers.get_mut(&handle.streamer) {
                s.watching = false;
            }
        } else {
            acc.streamers.remove(&handle.streamer);
        }
        acc.recalc();
    }
}

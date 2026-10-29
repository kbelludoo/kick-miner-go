//! Manager — orchestrates all accounts, groups, autofill, and prune cycles.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::time;
use tracing::warn;

use crate::config::{AccountConfig, Config};
use crate::kick_api::{discover_live_streamers, get_channel_info};
use crate::telemetry::{AppState, SharedState, StreamerStatus, WatcherHandle};
use crate::watcher::run_watcher;

pub struct Manager {
    pub cfg: Arc<Config>,
    pub state: SharedState,
    pub http: reqwest::Client,
}

impl Manager {
    pub fn new(cfg: Arc<Config>, state: SharedState, http: reqwest::Client) -> Self {
        Self { cfg, state, http }
    }

    pub async fn start(self: Arc<Self>, ctx: tokio::sync::watch::Receiver<bool>) {
        {
            let mut st = self.state.lock().await;
            st.add_log(&format!(
                "Starting Kick Miner Rust for {} account(s)…",
                self.cfg.accounts.len()
            ));
        }

        // Daily claim loop
        if self.cfg.settings.is_daily_claim_enabled() {
            let mgr = self.clone();
            let ctx2 = ctx.clone();
            tokio::spawn(async move {
                crate::daily::run_daily_claims(mgr.cfg.clone(), mgr.state.clone(), mgr.http.clone(), ctx2).await;
            });
        }

        // Initial check
        self.check_all_accounts(ctx.clone()).await;

        let check_interval = Duration::from_secs(self.cfg.settings.check_interval_seconds);
        let mut ticker = time::interval(check_interval);
        ticker.tick().await; // consume first

        let mut ctx_watch = ctx;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.check_all_accounts(ctx_watch.clone()).await;
                }
                _ = ctx_watch.changed() => {
                    if *ctx_watch.borrow() {
                        break;
                    }
                }
            }
        }
    }

    async fn check_all_accounts(&self, ctx: tokio::sync::watch::Receiver<bool>) {
        for acc in &self.cfg.accounts {
            let acc = acc.clone();
            let mgr_http = self.http.clone();
            let state = self.state.clone();
            let cfg = self.cfg.clone();
            let ctx2 = ctx.clone();
            tokio::spawn(async move {
                check_account(cfg, state, mgr_http, acc, ctx2).await;
            });
        }
    }
}

async fn check_account(
    cfg: Arc<Config>,
    state: SharedState,
    http: reqwest::Client,
    acc: AccountConfig,
    ctx: tokio::sync::watch::Receiver<bool>,
) {
    if acc.bearer_token.is_empty() {
        return;
    }

    // Prune offline watchers first
    prune_offline_watchers(&cfg, &state, &http, &acc).await;

    // Process each group independently
    for group in acc.effective_groups() {
        check_group(&cfg, &state, &http, &acc, &group, ctx.clone()).await;
    }

    // Auto-fill remaining slots
    if cfg.settings.auto_fill_enabled {
        check_auto_fill(&cfg, &state, &http, &acc, ctx).await;
    }
}

async fn check_group(
    cfg: &Config,
    state: &SharedState,
    http: &reqwest::Client,
    acc: &AccountConfig,
    group: &crate::config::StreamerGroup,
    ctx: tokio::sync::watch::Receiver<bool>,
) {
    let group_set: HashSet<String> = group
        .streamers
        .iter()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();

    let max_streams = group.max_concurrent.max(1) as usize;

    for streamer_raw in &group.streamers {
        let clean = streamer_raw.trim().to_lowercase();
        if clean.is_empty() {
            continue;
        }

        // Check if already watching
        let already_alive = {
            let st = state.lock().await;
            st.watchers
                .get(&acc.username)
                .and_then(|m| m.get(&clean))
                .map(|w| w.is_alive())
                .unwrap_or(false)
        };

        if already_alive {
            // Promote from autofill to favorite if needed
            let is_af = {
                let st = state.lock().await;
                st.watchers
                    .get(&acc.username)
                    .and_then(|m| m.get(&clean))
                    .map(|w| w.is_auto_fill())
                    .unwrap_or(false)
            };
            if is_af {
                promote_to_favorite(state, &acc.username, &clean).await;
            }
            continue;
        }

        // Count active for this group
        let active_count = {
            let st = state.lock().await;
            st.watchers
                .get(&acc.username)
                .map(|m| {
                    m.iter()
                        .filter(|(k, w)| group_set.contains(k.as_str()) && w.is_alive())
                        .count()
                })
                .unwrap_or(0)
        };

        if active_count >= max_streams {
            break;
        }

        // Fetch channel info
        tokio::time::sleep(Duration::from_millis(150)).await;
        let info = match get_channel_info(http, &acc.bearer_token, &clean).await {
            Ok(i) => i,
            Err(e) => {
                warn!("channel info for {clean}: {e}");
                continue;
            }
        };

        // Update live status
        {
            let mut st = state.lock().await;
            if let Some(acc_st) = st.statuses.get_mut(&acc.username) {
                if let Some(s) = acc_st.streamers.get_mut(&clean) {
                    s.is_live = info.is_live;
                }
            }
        }

        if !info.is_live {
            continue;
        }

        // Double-check alive before spawning
        let already = {
            let st = state.lock().await;
            st.watchers
                .get(&acc.username)
                .and_then(|m| m.get(&clean))
                .map(|w| w.is_alive())
                .unwrap_or(false)
        };
        if already {
            continue;
        }

        // Evict autofill if at target cap
        {
            let st = state.lock().await;
            let _ = st.evict_auto_fill(&acc.username, cfg.settings.auto_fill_target as usize);
        }

        spawn_watcher(cfg, state, http, acc, &clean, &info, false, ctx.clone()).await;
    }
}

async fn check_auto_fill(
    cfg: &Config,
    state: &SharedState,
    http: &reqwest::Client,
    acc: &AccountConfig,
    ctx: tokio::sync::watch::Receiver<bool>,
) {
    let target = cfg.settings.auto_fill_target as usize;

    let slots_needed = {
        let st = state.lock().await;
        let alive = st.alive_count(&acc.username);
        if alive >= target {
            return;
        }
        target - alive
    };

    let discovered = discover_live_streamers(http).await;
    if discovered.is_empty() {
        let should_warn = {
            let st = state.lock().await;
            st.discovery_warned_at
                .map(|t| t.elapsed() > Duration::from_secs(600))
                .unwrap_or(true)
        };
        if should_warn {
            let mut st = state.lock().await;
            st.discovery_warned_at = Some(std::time::Instant::now());
            st.add_log(&format!(
                "[{}] [Auto-Fill] No live channels discovered; {slots_needed} slot(s) idle.",
                acc.username
            ));
        }
        return;
    }

    let mut remaining = slots_needed;
    for slug in &discovered {
        if remaining == 0 {
            break;
        }

        let already = {
            let st = state.lock().await;
            st.watchers
                .get(&acc.username)
                .and_then(|m| m.get(slug.as_str()))
                .map(|w| w.is_alive())
                .unwrap_or(false)
        };
        if already {
            continue;
        }

        tokio::time::sleep(Duration::from_millis(150)).await;
        let info = match get_channel_info(http, &acc.bearer_token, slug).await {
            Ok(i) => i,
            Err(_) => continue,
        };
        if !info.is_live {
            continue;
        }

        // Re-check alive count under lock
        let at_target = {
            let st = state.lock().await;
            st.alive_count(&acc.username) >= target
        };
        if at_target {
            break;
        }

        spawn_watcher(cfg, state, http, acc, slug, &info, true, ctx.clone()).await;
        remaining -= 1;
    }
}

async fn spawn_watcher(
    cfg: &Config,
    state: &SharedState,
    http: &reqwest::Client,
    acc: &AccountConfig,
    streamer: &str,
    info: &crate::kick_api::ChannelInfo,
    auto_fill: bool,
    ctx: tokio::sync::watch::Receiver<bool>,
) {
    let (handle, cancel_rx) = WatcherHandle::new(
        acc.username.clone(),
        streamer.to_string(),
        info.channel_id,
        info.stream_id,
        auto_fill,
    );

    {
        let mut st = state.lock().await;
        // Ensure streamer status exists
        if let Some(acc_st) = st.statuses.get_mut(&acc.username) {
            acc_st.streamers.entry(streamer.to_string()).or_insert(StreamerStatus {
                streamer: streamer.to_string(),
                is_live: true,
                auto_fill,
                ..Default::default()
            });
        }
        if let Some(watchers) = st.watchers.get_mut(&acc.username) {
            watchers.insert(streamer.to_string(), handle.clone());
        }
        let af_label = if auto_fill { " [auto-fill]" } else { "" };
        st.add_log(&format!(
            "[{}] Starting watcher for '{streamer}'{af_label}.",
            acc.username
        ));
    }

    let state2 = state.clone();
    let http2 = http.clone();
    let token = acc.bearer_token.clone();
    let user_id = info.user_id;
    let ping_secs = cfg.settings.ping_interval_seconds;
    let tracking_secs = cfg.settings.tracking_interval_seconds;
    let points_mins = cfg.settings.points_log_interval_minutes;

    tokio::spawn(async move {
        run_watcher(
            state2,
            http2,
            handle,
            token,
            user_id,
            ping_secs,
            tracking_secs,
            points_mins,
            cancel_rx,
        )
        .await;
    });
    // Give cancel_rx to the task via ctx propagation
    let _ = ctx; // ctx is used by caller; watcher uses cancel_rx from handle
}

async fn prune_offline_watchers(
    _cfg: &Config,
    state: &SharedState,
    http: &reqwest::Client,
    acc: &AccountConfig,
) {
    // Collect alive watchers snapshot
    let active: Vec<Arc<WatcherHandle>> = {
        let st = state.lock().await;
        st.watchers
            .get(&acc.username)
            .map(|m| m.values().filter(|w| w.is_alive()).cloned().collect())
            .unwrap_or_default()
    };

    for w in active {
        let info = match get_channel_info(http, &acc.bearer_token, &w.streamer).await {
            Ok(i) => i,
            Err(_) => continue,
        };

        let current_stream_id = w.stream_id.load(std::sync::atomic::Ordering::Relaxed);
        let restarted = info.is_live
            && info.stream_id != 0
            && current_stream_id != 0
            && info.stream_id != current_stream_id;

        if info.is_live && !restarted {
            continue;
        }

        // Update live status
        {
            let mut st = state.lock().await;
            if let Some(acc_st) = st.statuses.get_mut(&acc.username) {
                if let Some(s) = acc_st.streamers.get_mut(&w.streamer) {
                    s.is_live = info.is_live;
                }
            }
        }

        w.close();
        let mut st = state.lock().await;
        if restarted {
            st.add_log(&format!(
                "[{}] '{}' stream restarted; reconnecting to new broadcast.",
                acc.username, w.streamer
            ));
        } else {
            st.add_log(&format!(
                "[{}] '{}' stream ended; slot freed.",
                acc.username, w.streamer
            ));
        }
    }
}

async fn promote_to_favorite(state: &SharedState, username: &str, streamer: &str) {
    let mut st = state.lock().await;
    if let Some(watchers) = st.watchers.get(username) {
        if let Some(w) = watchers.get(streamer) {
            w.auto_fill.store(false, std::sync::atomic::Ordering::Relaxed);
        }
    }
    if let Some(acc) = st.statuses.get_mut(username) {
        if let Some(s) = acc.streamers.get_mut(streamer) {
            s.auto_fill = false;
        }
    }
}

pub fn collect_telemetry(state: &AppState, _cfg: &Config) -> crate::telemetry::GlobalTelemetry {
    let uptime = {
        let secs = state.started_at.elapsed().as_secs();
        format_duration(secs)
    };

    // Approximate RAM via /proc/self/status on Linux; fall back to 0 on Windows in tests
    let ram_mb = get_ram_mb();

    // Thread count as goroutine proxy
    let goroutines = num_cpus_approx();

    let mut total_streams = 0usize;
    let mut accounts = std::collections::HashMap::new();

    for (uname, acc_st) in &state.statuses {
        let mut acc_copy = acc_st.clone();
        // Annotate watch_time for actively watching streamers
        for (sname, s) in &mut acc_copy.streamers {
            if s.watching {
                if let Some(w) = state.watchers.get(uname).and_then(|m| m.get(sname)) {
                    s.watch_time = format_duration(w.duration().as_secs());
                }
            }
        }
        total_streams += acc_copy.active_streams;
        accounts.insert(uname.clone(), acc_copy);
    }

    crate::telemetry::GlobalTelemetry {
        uptime,
        ram_mb,
        goroutines,
        total_streams,
        accounts,
        logs: state.logs.clone(),
    }
}

fn format_duration(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h{m}m{s}s")
    } else if m > 0 {
        format!("{m}m{s}s")
    } else {
        format!("{s}s")
    }
}

fn get_ram_mb() -> f64 {
    // On Windows read working set via GetProcessMemoryInfo equivalent: try reading
    // the environment or fall back to 0.
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
            for line in s.lines() {
                if line.starts_with("VmRSS:") {
                    let kb: u64 = line
                        .split_whitespace()
                        .nth(1)
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                    return kb as f64 / 1024.0;
                }
            }
        }
        0.0
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Windows: use a rough heap approximation
        0.0
    }
}

fn num_cpus_approx() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Top-level entry point called from main.
/// Builds the HTTP client, wraps Manager in Arc, wires chatters, runs until Ctrl-C/SIGTERM.
pub async fn run(cfg: crate::config::Config, state: SharedState) {
    use std::sync::Arc;
    use tokio::signal;

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        .build()
        .expect("reqwest client build");

    let cfg = Arc::new(cfg);

    // Graceful-shutdown channel
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Chatbot for owner's channel (first account)
    if let Some(acc) = cfg.accounts.first() {
        let token = acc.bearer_token.clone();
        let http2 = http.clone();
        let mut rx2 = shutdown_rx.clone();
        let chatbot = Arc::new(crate::chatbot::Bot::new(token, "kbelludoo".to_string(), http2));
        tokio::spawn(async move {
            tokio::select! {
                _ = chatbot.start(rx2.clone()) => {}
                _ = rx2.changed() => {}
            }
        });
    }

    // Humanized chatters (if enabled)
    if cfg.chatter.enabled {
        if let Some(acc) = cfg.accounts.first() {
            let chatter_streamers: Vec<String> = if cfg.chatter.streamers.is_empty() {
                acc.effective_groups()
                    .into_iter()
                    .find(|g| g.name == "prioridade")
                    .map(|g| g.streamers)
                    .unwrap_or_default()
            } else {
                cfg.chatter.streamers.clone()
            };
            for streamer in chatter_streamers {
                let token = acc.bearer_token.clone();
                let username = acc.username.clone();
                let chatter_cfg = cfg.chatter.clone();
                let mut rx2 = shutdown_rx.clone();
                tokio::spawn(async move {
                    tokio::select! {
                        _ = crate::chatter::run_chatter(token, username, streamer, chatter_cfg) => {}
                        _ = rx2.changed() => {}
                    }
                });
            }
        }
    }

    // Start Manager (internally handles daily-claim loop + watcher loop)
    let mgr = Arc::new(Manager::new(cfg.clone(), state.clone(), http));
    let rx_mgr = shutdown_rx.clone();
    let mgr2 = mgr.clone();
    tokio::spawn(async move {
        mgr2.start(rx_mgr).await;
    });

    // Console ticker every 5 seconds
    {
        let state2 = state.clone();
        let mut rx2 = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let guard = state2.lock().await;
                        let tel = guard.build_telemetry();
                        drop(guard);
                        let mut watching = Vec::new();
                        let mut total_pts = 0i64;
                        for acc in tel.accounts.values() {
                            total_pts += acc.total_points;
                            for (slug, ss) in &acc.streamers {
                                if ss.watching {
                                    watching.push(format!("{slug} ({} pts)", ss.points));
                                }
                            }
                        }
                        let desc = if watching.is_empty() {
                            "Nenhum canal ao vivo".to_string()
                        } else {
                            watching.join(", ")
                        };
                        println!(
                            "[Ticker] RAM: {:.2} MB | Uptime: {} | Streams: {} | Pontos: {} | Assistindo: [{}]",
                            tel.ram_mb, tel.uptime, tel.total_streams, total_pts, desc
                        );
                    }
                    _ = rx2.changed() => break,
                }
            }
        });
    }

    // Wait for Ctrl-C / SIGTERM
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("sigterm handler");
        tokio::select! {
            _ = signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
    }

    tracing::info!("Encerrando Kick Miner Rust...");
    let _ = shutdown_tx.send(true);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
}

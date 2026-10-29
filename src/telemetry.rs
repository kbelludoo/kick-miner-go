use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Per-streamer state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StreamerStatus {
    pub streamer: String,
    #[serde(rename = "is_live")]
    pub is_live: bool,
    pub watching: bool,
    #[serde(rename = "auto_fill")]
    pub auto_fill: bool,
    pub points: i64,
    #[serde(rename = "watch_time")]
    pub watch_time: String,
    #[serde(rename = "last_error", skip_serializing_if = "String::is_empty")]
    pub last_error: String,
}

// ---------------------------------------------------------------------------
// Per-account state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AccountStatus {
    pub username: String,
    #[serde(rename = "active_streams")]
    pub active_streams: usize,
    #[serde(rename = "total_points")]
    pub total_points: i64,
    pub streamers: HashMap<String, StreamerStatus>,
}

impl AccountStatus {
    pub fn recalc(&mut self) {
        let mut active = 0usize;
        let mut total = 0i64;
        for s in self.streamers.values() {
            if s.watching {
                active += 1;
            }
            total += s.points;
        }
        self.active_streams = active;
        self.total_points = total;
    }
}

// ---------------------------------------------------------------------------
// Global telemetry (API response shape)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalTelemetry {
    pub uptime: String,
    pub ram_mb: f64,
    /// thread count (Rust doesn't expose goroutine count; use OS thread count)
    pub goroutines: usize,
    pub total_streams: usize,
    pub accounts: HashMap<String, AccountStatus>,
    pub logs: Vec<String>,
}

// ---------------------------------------------------------------------------
// Active watcher handle — shared between manager and watcher task
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct WatcherHandle {
    pub username: String,
    pub streamer: String,
    pub channel_id: i64,
    pub stream_id: AtomicI64,
    pub alive: AtomicBool,
    pub auto_fill: AtomicBool,
    pub started_at: Instant,
    /// Sending `()` to this causes the watcher task to stop.
    pub cancel_tx: tokio::sync::watch::Sender<bool>,
}

impl WatcherHandle {
    pub fn new(
        username: String,
        streamer: String,
        channel_id: i64,
        stream_id: i64,
        auto_fill: bool,
    ) -> (Arc<Self>, tokio::sync::watch::Receiver<bool>) {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let handle = Arc::new(Self {
            username,
            streamer,
            channel_id,
            stream_id: AtomicI64::new(stream_id),
            alive: AtomicBool::new(true),
            auto_fill: AtomicBool::new(auto_fill),
            started_at: Instant::now(),
            cancel_tx,
        });
        (handle, cancel_rx)
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    pub fn is_auto_fill(&self) -> bool {
        self.auto_fill.load(Ordering::Relaxed)
    }

    pub fn duration(&self) -> std::time::Duration {
        self.started_at.elapsed()
    }

    pub fn close(&self) {
        self.alive.store(false, Ordering::Relaxed);
        let _ = self.cancel_tx.send(true);
    }
}

// ---------------------------------------------------------------------------
// Shared app state (used by manager, watcher, server)
// ---------------------------------------------------------------------------

pub type SharedState = Arc<Mutex<AppState>>;

#[derive(Debug)]
pub struct AppState {
    pub statuses: HashMap<String, AccountStatus>,
    pub watchers: HashMap<String, HashMap<String, Arc<WatcherHandle>>>,
    pub logs: Vec<String>,
    pub started_at: Instant,
    pub discovery_warned_at: Option<Instant>,
}

impl AppState {
    pub fn new(accounts: &[crate::config::AccountConfig]) -> Self {
        let mut statuses = HashMap::new();
        let mut watchers: HashMap<String, HashMap<String, Arc<WatcherHandle>>> = HashMap::new();

        for acc in accounts {
            let mut st = AccountStatus {
                username: acc.username.clone(),
                ..Default::default()
            };
            let mut favs: HashMap<String, StreamerStatus> = HashMap::new();
            for g in acc.effective_groups() {
                for s in &g.streamers {
                    let clean = s.trim().to_lowercase();
                    if !clean.is_empty() {
                        favs.insert(
                            clean.clone(),
                            StreamerStatus {
                                streamer: clean,
                                ..Default::default()
                            },
                        );
                    }
                }
            }
            st.streamers = favs;
            statuses.insert(acc.username.clone(), st);
            watchers.insert(acc.username.clone(), HashMap::new());
        }

        Self {
            statuses,
            watchers,
            logs: Vec::with_capacity(100),
            started_at: Instant::now(),
            discovery_warned_at: None,
        }
    }

    pub fn add_log(&mut self, msg: &str) {
        tracing::info!("{}", msg);
        let entry = format!(
            "[{}] {}",
            chrono::Local::now().format("%H:%M:%S"),
            msg
        );
        self.logs.push(entry);
        if self.logs.len() > 100 {
            self.logs.remove(0);
        }
    }

    pub fn alive_count(&self, username: &str) -> usize {
        self.watchers
            .get(username)
            .map(|m| m.values().filter(|w| w.is_alive()).count())
            .unwrap_or(0)
    }

    /// Evict shortest-running autofill watcher to make room for a favorite.
    /// Returns evicted streamer name if any.
    pub fn evict_auto_fill(&self, username: &str, target: usize) -> Option<String> {
        if self.alive_count(username) < target {
            return None;
        }
        let watchers = self.watchers.get(username)?;
        let victim = watchers
            .values()
            .filter(|w| w.is_alive() && w.is_auto_fill())
            .min_by_key(|w| w.duration());
        victim.map(|v| {
            v.close();
            v.streamer.clone()
        })
    }
}

impl AppState {
    /// Create an Arc<Mutex<AppState>> ready for sharing across tasks.
    pub fn new_shared(accounts: &[crate::config::AccountConfig]) -> SharedState {
        Arc::new(Mutex::new(Self::new(accounts)))
    }

    /// Build a snapshot of telemetry for the HTTP API.
    pub fn build_telemetry(&self) -> GlobalTelemetry {
        use std::thread;

        let ram_mb = {
            // Approximate: read /proc/self/status on Linux, fall back to 0 on Windows.
            #[cfg(target_os = "linux")]
            {
                std::fs::read_to_string("/proc/self/status")
                    .ok()
                    .and_then(|s| {
                        s.lines()
                            .find(|l| l.starts_with("VmRSS:"))
                            .and_then(|l| l.split_whitespace().nth(1))
                            .and_then(|v| v.parse::<f64>().ok())
                    })
                    .unwrap_or(0.0)
                    / 1024.0
            }
            #[cfg(not(target_os = "linux"))]
            {
                0.0_f64
            }
        };

        let uptime = {
            let secs = self.started_at.elapsed().as_secs();
            let h = secs / 3600;
            let m = (secs % 3600) / 60;
            let s = secs % 60;
            format!("{h:02}:{m:02}:{s:02}")
        };

        // Copy account statuses, enriching watch_time from live handles
        let mut accounts_snap: HashMap<String, AccountStatus> = HashMap::new();
        let mut total_streams = 0usize;

        for (uname, st) in &self.statuses {
            let mut copy = st.clone();
            let watchers_map = self.watchers.get(uname);
            for (slug, ss) in &mut copy.streamers {
                if ss.watching {
                    if let Some(wmap) = watchers_map {
                        if let Some(h) = wmap.get(slug) {
                            let d = h.duration();
                            let s_tot = d.as_secs();
                            ss.watch_time = format!(
                                "{}:{:02}:{:02}",
                                s_tot / 3600,
                                (s_tot % 3600) / 60,
                                s_tot % 60
                            );
                        }
                    }
                }
            }
            total_streams += copy.active_streams;
            accounts_snap.insert(uname.clone(), copy);
        }

        GlobalTelemetry {
            uptime,
            ram_mb,
            goroutines: thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            total_streams,
            accounts: accounts_snap,
            logs: self.logs.clone(),
        }
    }
}

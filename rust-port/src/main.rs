use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tungstenite::client::IntoClientRequest;
use tungstenite::http::HeaderValue;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, Message};

const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const DASHBOARD: &str = r#"<!doctype html><html lang="pt-br"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Kick Miner Rust</title><style>body{background:#10151c;color:#ececec;font:16px system-ui;margin:3rem auto;max-width:850px}pre{white-space:pre-wrap;background:#1e2934;padding:1rem;border-radius:10px}h1{color:#82e65b}</style><h1>Kick Miner Rust</h1><p>Telemetria local do port em Rust</p><pre id="out">Carregando...</pre><script>async function update(){try{let r=await fetch('/api/status');document.getElementById('out').textContent=JSON.stringify(await r.json(),null,2)}catch(e){document.getElementById('out').textContent=String(e)}}update();setInterval(update,4000)</script></html>"#;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Config {
    accounts: Vec<Account>,
    settings: Settings,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Account {
    username: String,
    bearer_token: String,
    streamers: Vec<String>,
    max_concurrent_streams: usize,
    groups: Vec<Group>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Group {
    name: String,
    streamers: Vec<String>,
    max_concurrent: usize,
}

impl Account {
    fn groups(&self) -> Vec<Group> {
        if !self.groups.is_empty() {
            return self.groups.iter().cloned().map(|mut g| {
                if g.max_concurrent == 0 { g.max_concurrent = 2; }
                g
            }).collect();
        }
        vec![Group {
            name: "default".to_string(),
            streamers: self.streamers.clone(),
            max_concurrent: self.max_concurrent_streams.max(2),
        }]
    }
    fn favorites(&self) -> HashSet<String> {
        self.groups().iter().flat_map(|g| g.streamers.iter())
            .map(|s| s.trim().to_lowercase()).collect()
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Settings {
    check_interval_seconds: u64,
    ping_interval_seconds: u64,
    tracking_interval_seconds: u64,
    points_log_interval_minutes: u64,
    web_dashboard_port: u16,
    auto_fill_enabled: bool,
    auto_fill_target: usize,
    daily_claim_enabled: Option<bool>,
    daily_claim_check_minutes: u64,
}
impl Settings {
    fn apply_defaults(&mut self) {
        if self.check_interval_seconds == 0 { self.check_interval_seconds = 45; }
        if self.ping_interval_seconds == 0 { self.ping_interval_seconds = 30; }
        if self.tracking_interval_seconds == 0 { self.tracking_interval_seconds = 10; }
        if self.points_log_interval_minutes == 0 { self.points_log_interval_minutes = 5; }
        if self.web_dashboard_port == 0 { self.web_dashboard_port = 3007; }
        if self.auto_fill_target == 0 { self.auto_fill_target = 12; }
        if self.daily_claim_check_minutes == 0 { self.daily_claim_check_minutes = 15; }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamerStatus {
    streamer: String,
    is_live: bool,
    watching: bool,
    auto_fill: bool,
    points: i64,
    watch_time: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    last_error: String,
}
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct AccountStatus {
    username: String,
    active_streams: usize,
    total_points: i64,
    streamers: HashMap<String, StreamerStatus>,
}
struct Watcher {
    stop: AtomicBool,
    auto_fill: AtomicBool,
    started: Instant,
    stream_id: i64,
}
struct Manager {
    cfg: Config,
    http: Client,
    statuses: Mutex<HashMap<String, AccountStatus>>,
    watchers: Mutex<HashMap<(String, String), Arc<Watcher>>>,
    logs: Mutex<Vec<String>>,
    started: Instant,
    stop: AtomicBool,
}

impl Manager {
    fn new(cfg: Config) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        let mut statuses = HashMap::new();
        for a in &cfg.accounts {
            let mut s = AccountStatus { username: a.username.clone(), ..Default::default() };
            for name in a.favorites() {
                s.streamers.insert(name.clone(), StreamerStatus { streamer: name, ..Default::default() });
            }
            statuses.insert(a.username.clone(), s);
        }
        let http = Client::builder().timeout(Duration::from_secs(15)).user_agent(UA).build()?;
        Ok(Arc::new(Self {
            cfg, http, statuses: Mutex::new(statuses), watchers: Mutex::new(HashMap::new()),
            logs: Mutex::new(Vec::new()), started: Instant::now(), stop: AtomicBool::new(false),
        }))
    }
    fn log(&self, msg: &str) {
        println!("[Kick Rust] {msg}");
        let mut logs = self.logs.lock().unwrap();
        logs.push(msg.to_owned());
        if logs.len() > 100 { logs.remove(0); }
    }
    fn telemetry(&self) -> Value {
        let mut accounts = self.statuses.lock().unwrap().clone();
        let watchers = self.watchers.lock().unwrap();
        let mut active = 0usize;
        for ((username, slug), w) in watchers.iter() {
            if let Some(s) = accounts.get_mut(username).and_then(|a| a.streamers.get_mut(slug)) {
                if s.watching && !w.stop.load(Ordering::Relaxed) {
                    s.watch_time = format!("{}s", w.started.elapsed().as_secs());
                    active += 1;
                }
            }
        }
        json!({
            "uptime": format!("{}s", self.started.elapsed().as_secs()),
            "ramMb": resident_mb(), "goroutines": watchers.len(),
            "totalStreams": active, "accounts": accounts, "logs": *self.logs.lock().unwrap()
        })
    }
    fn update_status(&self, username: &str, slug: &str, update: impl FnOnce(&mut StreamerStatus)) {
        let mut all = self.statuses.lock().unwrap();
        if let Some(account) = all.get_mut(username) {
            let s = account.streamers.entry(slug.to_owned())
                .or_insert_with(|| StreamerStatus { streamer: slug.to_owned(), ..Default::default() });
            update(s);
            account.active_streams = account.streamers.values().filter(|x| x.watching).count();
            account.total_points = account.streamers.values().map(|x| x.points).sum();
        }
    }
    fn get_json(&self, url: &str, token: &str) -> Result<Value, Box<dyn std::error::Error>> {
        let mut request = self.http.get(url).header("Accept", "application/json")
            .header("Origin", "https://kick.com");
        if !token.is_empty() { request = request.bearer_auth(token); }
        Ok(request.send()?.error_for_status()?.json()?)
    }
    fn channel_info(&self, token: &str, slug: &str) -> Option<(i64, i64, i64, bool)> {
        let d = self.get_json(&format!("https://kick.com/api/v2/channels/{slug}"), token).ok()?;
        let id = d["id"].as_i64()?;
        let uid = d["user_id"].as_i64().unwrap_or(0);
        let livestream = d.get("livestream").filter(|v| !v.is_null());
        let streamid = livestream.and_then(|v| v["id"].as_i64()).unwrap_or(0);
        Some((id, uid, streamid, livestream.is_some()))
    }
    fn points(&self, token: &str, slug: &str) -> Option<i64> {
        self.get_json(&format!("https://kick.com/api/v2/channels/{slug}/points"), token).ok()?
            .pointer("/data/points")?.as_i64()
    }
    fn viewer_token(&self, token: &str, slug: &str, ch: i64, uid: i64) -> Option<String> {
        let data: Value = self.http.get("https://websockets.kick.com/viewer/v1/token")
            .header("Authorization", format!("Bearer {token}"))
            .header("Origin", "https://kick.com")
            .header("Referer", format!("https://kick.com/{slug}/"))
            .header("X-Chatroom", ch.to_string())
            .header("X-User-Id", uid.to_string())
            .header("X-Client-Token", "e1393935a959b4020a4491574f6490129f678acdaa92760471263db43487f823")
            .send().ok()?.error_for_status().ok()?.json().ok()?;
        data.pointer("/data/token").or_else(|| data.get("token"))?.as_str().map(str::to_owned)
    }
    fn check_account(self: &Arc<Self>, acc: Account) {
        if acc.bearer_token.is_empty() { return; }
        let username = acc.username.clone();
        // Recheck the live state of active watchers, including restart of the stream id.
        let snapshot: Vec<_> = self.watchers.lock().unwrap().iter()
            .filter(|((a, _), _)| a == &username)
            .map(|((_, slug), w)| (slug.clone(), Arc::clone(w))).collect();
        for (slug, w) in snapshot {
            if w.stop.load(Ordering::Relaxed) { continue; }
            if let Some((_, _, stream_id, live)) = self.channel_info(&acc.bearer_token, &slug) {
                if !live || (stream_id != 0 && w.stream_id != 0 && stream_id != w.stream_id) {
                    w.stop.store(true, Ordering::Relaxed);
                    self.update_status(&username, &slug, |s| { s.watching = false; s.is_live = live; });
                }
            }
        }
        for group in acc.groups() {
            let allowed: HashSet<String> = group.streamers.iter().map(|x| x.trim().to_lowercase()).collect();
            let mut n = self.watchers.lock().unwrap().iter()
                .filter(|((a, slug), w)| a == &username && allowed.contains(slug) && !w.stop.load(Ordering::Relaxed))
                .count();
            for slug in group.streamers {
                if n >= group.max_concurrent { break; }
                let slug = slug.trim().to_lowercase();
                if slug.is_empty() { continue; }
                if self.is_running(&username, &slug) { continue; }
                if let Some((ch, uid, sid, live)) = self.channel_info(&acc.bearer_token, &slug) {
                    self.update_status(&username, &slug, |s| s.is_live = live);
                    if live {
                        self.start_watcher(acc.clone(), slug, ch, uid, sid, false);
                        n += 1;
                    }
                }
            }
        }
        if self.cfg.settings.auto_fill_enabled {
            let target = self.cfg.settings.auto_fill_target;
            for slug in self.discover_live() {
                if self.running_count(&username) >= target { break; }
                if self.is_running(&username, &slug) { continue; }
                if let Some((ch, uid, sid, true)) = self.channel_info(&acc.bearer_token, &slug) {
                    self.start_watcher(acc.clone(), slug.clone(), ch, uid, sid, !acc.favorites().contains(&slug));
                }
            }
        }
    }
    fn is_running(&self, username: &str, slug: &str) -> bool {
        self.watchers.lock().unwrap().get(&(username.to_owned(), slug.to_owned()))
            .is_some_and(|w| !w.stop.load(Ordering::Relaxed))
    }
    fn running_count(&self, username: &str) -> usize {
        self.watchers.lock().unwrap().iter()
            .filter(|((a, _), w)| a == username && !w.stop.load(Ordering::Relaxed)).count()
    }
    fn start_watcher(self: &Arc<Self>, acc: Account, slug: String, ch: i64, uid: i64, sid: i64, auto: bool) {
        if self.is_running(&acc.username, &slug) {
            if !auto {
                if let Some(w) = self.watchers.lock().unwrap().get(&(acc.username.clone(), slug.clone())) {
                    w.auto_fill.store(false, Ordering::Relaxed);
                    self.update_status(&acc.username, &slug, |s| s.auto_fill = false);
                }
            }
            return;
        }
        if !auto && self.cfg.settings.auto_fill_enabled
            && self.running_count(&acc.username) >= self.cfg.settings.auto_fill_target
        {
            let victim = self.watchers.lock().unwrap().iter()
                .filter(|((u, _), w)| u == &acc.username && w.auto_fill.load(Ordering::Relaxed))
                .max_by_key(|(_, w)| w.started).map(|(_, w)| Arc::clone(w));
            if let Some(w) = victim { w.stop.store(true, Ordering::Relaxed); }
        }
        let w = Arc::new(Watcher { stop: AtomicBool::new(false),
            auto_fill: AtomicBool::new(auto), started: Instant::now(), stream_id: sid });
        self.watchers.lock().unwrap().insert((acc.username.clone(), slug.clone()), Arc::clone(&w));
        self.update_status(&acc.username, &slug, |s| { s.is_live = true; s.auto_fill = auto; s.last_error.clear(); });
        let mgr = Arc::clone(self);
        thread::spawn(move || mgr.watch(acc, slug, ch, uid, sid, w));
    }
    fn discover_live(&self) -> Vec<String> {
        let urls = [
            "https://kick.com/stream/featured-livestreams/pt",
            "https://kick.com/stream/featured-livestreams/en",
            "https://kick.com/stream/livestreams/pt?page=1&limit=30",
            "https://kick.com/stream/livestreams/en?page=1&limit=30",
        ];
        let mut result = Vec::new();
        let mut seen = HashSet::new();
        for url in urls {
            if let Ok(data) = self.get_json(url, "") {
                let list = data.get("data").and_then(Value::as_array).or_else(|| data.as_array());
                if let Some(items) = list {
                    for item in items {
                        let slug = item.pointer("/channel/slug").or_else(|| item.get("slug"))
                            .and_then(Value::as_str).unwrap_or("").trim().to_lowercase();
                        if !slug.is_empty() && seen.insert(slug.clone()) { result.push(slug); }
                    }
                }
            }
        }
        result
    }
    fn watch(self: Arc<Self>, acc: Account, slug: String, ch: i64, uid: i64, sid: i64, watcher: Arc<Watcher>) {
        let mut backoff = 5u64;
        while !self.stop.load(Ordering::Relaxed) && !watcher.stop.load(Ordering::Relaxed) {
            let result = self.watch_session(&acc, &slug, ch, uid, sid, &watcher);
            if let Err(err) = result { self.log(&format!("[{}] {}: {err}", acc.username, slug)); }
            self.update_status(&acc.username, &slug, |s| s.watching = false);
            for _ in 0..backoff {
                if self.stop.load(Ordering::Relaxed) || watcher.stop.load(Ordering::Relaxed) { break; }
                thread::sleep(Duration::from_secs(1));
            }
            backoff = (backoff * 2).min(120);
        }
        self.update_status(&acc.username, &slug, |s| { s.watching = false; s.is_live = false; });
        let key = (acc.username.clone(), slug.clone());
        let mut watchers = self.watchers.lock().unwrap();
        if watchers.get(&key).is_some_and(|current| Arc::ptr_eq(current, &watcher)) {
            watchers.remove(&key);
        }
        drop(watchers);
        if !acc.favorites().contains(&slug) {
            if let Some(a) = self.statuses.lock().unwrap().get_mut(&acc.username) {
                a.streamers.remove(&slug);
            }
        }
    }
    fn watch_session(&self, acc: &Account, slug: &str, ch: i64, uid: i64, sid: i64, watcher: &Watcher)
        -> Result<(), Box<dyn std::error::Error>>
    {
        let token = self.viewer_token(&acc.bearer_token, slug, ch, uid)
            .ok_or("could not obtain viewer token")?;
        let ws_url = format!("wss://websockets.kick.com/viewer/v1/connect?token={}", urlencoding::encode(&token));
        let mut request = ws_url.into_client_request()?;
        request.headers_mut().insert("Origin", HeaderValue::from_static("https://kick.com"));
        request.headers_mut().insert("User-Agent", HeaderValue::from_static(UA));
        let (mut socket, _) = connect(request)?;
        match socket.get_mut() {
            MaybeTlsStream::Plain(s) => { let _ = s.set_read_timeout(Some(Duration::from_millis(900))); }
            MaybeTlsStream::Rustls(s) => { let _ = s.sock.set_read_timeout(Some(Duration::from_millis(900))); }
            _ => {}
        }
        self.update_status(&acc.username, slug, |s| s.watching = true);
        if let Some(p) = self.points(&acc.bearer_token, slug) {
            self.update_status(&acc.username, slug, |s| s.points = p);
        }
        self.log(&format!("[{}] Conectado ao canal {slug}", acc.username));
        let handshake = json!({"type":"channel_handshake","data":{"message":{"channelId":ch}}}).to_string();
        let tracking = json!({"type":"user_event","data":{"message":{
            "name":"tracking.user.watch.livestream","channel_id":ch,"livestream_id":sid
        }}}).to_string();
        socket.send(Message::Text(handshake.clone().into()))?;
        socket.send(Message::Text(r#"{"type":"ping"}"#.into()))?;
        let mut last_ping = Instant::now();
        let mut last_track = Instant::now();
        let mut last_points = Instant::now();
        let cfg = &self.cfg.settings;
        while !self.stop.load(Ordering::Relaxed) && !watcher.stop.load(Ordering::Relaxed) {
            if last_ping.elapsed() >= Duration::from_secs(cfg.ping_interval_seconds) {
                socket.send(Message::Text(handshake.clone().into()))?;
                socket.send(Message::Text(r#"{"type":"ping"}"#.into()))?;
                last_ping = Instant::now();
            }
            if last_track.elapsed() >= Duration::from_secs(cfg.tracking_interval_seconds) {
                socket.send(Message::Text(tracking.clone().into()))?;
                last_track = Instant::now();
            }
            if last_points.elapsed() >= Duration::from_secs(cfg.points_log_interval_minutes * 60) {
                if let Some(p) = self.points(&acc.bearer_token, slug) {
                    self.update_status(&acc.username, slug, |s| s.points = p);
                }
                last_points = Instant::now();
            }
            match socket.read() {
                Ok(Message::Text(msg)) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&msg) {
                        if v["type"] == "ping" { socket.send(Message::Text(r#"{"type":"pong"}"#.into()))?; }
                    }
                }
                Ok(Message::Ping(p)) => { socket.send(Message::Pong(p))?; }
                Ok(Message::Close(_)) => break,
                Err(tungstenite::Error::Io(e))
                    if matches!(e.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock) => {}
                Err(e) => return Err(Box::new(e)),
                _ => {}
            }
        }
        let _ = socket.close(None);
        Ok(())
    }
    fn daily_claim(self: Arc<Self>, acc: Account) {
        let default = self.cfg.settings.daily_claim_check_minutes * 60;
        while !self.stop.load(Ordering::Relaxed) {
            let mut next = default;
            let endpoint = "https://web.kick.com/api/v1/gamification/challenges";
            let request = self.http.get(endpoint).bearer_auth(&acc.bearer_token)
                .header("Origin", "https://kick.com").header("x-app-platform", "web");
            match request.send().and_then(|r| r.error_for_status()).and_then(|r| r.json::<Value>()) {
                Ok(body) => {
                    let list = body.get("data").and_then(Value::as_array).or_else(|| body.as_array());
                    if let Some(challenge) = list.and_then(|a| a.iter().find(|x| x["recurrence"] == "daily")) {
                        if challenge["status"] == "claimable" {
                            if let Some(id) = challenge["id"].as_str() {
                                let url = format!("{}/{}/claim", endpoint, urlencoding::encode(id));
                                let resp = self.http.post(&url).bearer_auth(&acc.bearer_token)
                                    .header("Origin", "https://kick.com")
                                    .header("x-app-platform", "web")
                                    .header("Cookie", format!("session_token={}", acc.bearer_token))
                                    .json(&json!({})).send();
                                if resp.is_ok_and(|r| r.status().is_success()) {
                                    self.log(&format!("[{}] Recompensa diária resgatada", acc.username));
                                } else { next = 1800; }
                            }
                        } else if challenge["status"] == "in_progress" {
                            let threshold = challenge["condition"]["threshold"].as_f64().unwrap_or(0.);
                            let progress = challenge["condition"]["progress"].as_f64().unwrap_or(0.);
                            next = next.min(((threshold - progress).max(1.) * 60.) as u64);
                        }
                    }
                }
                Err(err) => { self.log(&format!("[{}] Daily: {err}", acc.username)); next = 1800; }
            }
            for _ in 0..next.max(60) {
                if self.stop.load(Ordering::Relaxed) { return; }
                thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

fn resident_mb() -> f64 {
    let text = fs::read_to_string("/proc/self/status").unwrap_or_default();
    text.lines().find(|l| l.starts_with("VmRSS:"))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.) / 1024.
}
fn start_dashboard(mgr: Arc<Manager>, port: u16) {
    thread::spawn(move || {
        let Ok(server) = tiny_http::Server::http(("0.0.0.0", port)) else {
            eprintln!("Não foi possível abrir dashboard na porta {port}");
            return;
        };
        println!("Dashboard em http://localhost:{port}");
        for req in server.incoming_requests() {
            let api = req.url().starts_with("/api/status");
            let body = if api { mgr.telemetry().to_string() } else { DASHBOARD.to_string() };
            let content_type = if api { "application/json" } else { "text/html; charset=utf-8" };
            let response = tiny_http::Response::from_string(body).with_header(
                tiny_http::Header::from_bytes("Content-Type", content_type).unwrap()
            );
            let _ = req.respond(response);
        }
    });
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file = std::env::args().nth(1).unwrap_or_else(|| "config.json".to_owned());
    if !std::path::Path::new(&file).exists() {
        if let Ok(example) = fs::read("config.example.json") { fs::write(&file, example)?; }
        return Err(format!("Configure o arquivo {file} antes de executar.").into());
    }
    let mut cfg: Config = serde_json::from_str(&fs::read_to_string(&file)?)?;
    cfg.settings.apply_defaults();
    if cfg.accounts.is_empty() { return Err("Nenhuma conta configurada".into()); }
    let mgr = Manager::new(cfg)?;
    let signal_mgr = Arc::clone(&mgr);
    ctrlc::set_handler(move || signal_mgr.stop.store(true, Ordering::SeqCst))?;
    start_dashboard(Arc::clone(&mgr), mgr.cfg.settings.web_dashboard_port);
    if mgr.cfg.settings.daily_claim_enabled.unwrap_or(true) {
        for acc in mgr.cfg.accounts.clone() {
            if !acc.bearer_token.is_empty() {
                let m = Arc::clone(&mgr);
                thread::spawn(move || m.daily_claim(acc));
            }
        }
    }
    mgr.log("Miner Rust iniciado.");
    let mut last_check = Instant::now() - Duration::from_secs(mgr.cfg.settings.check_interval_seconds);
    let mut last_log = Instant::now();
    while !mgr.stop.load(Ordering::Relaxed) {
        if last_check.elapsed() >= Duration::from_secs(mgr.cfg.settings.check_interval_seconds) {
            for acc in mgr.cfg.accounts.clone() {
                let m = Arc::clone(&mgr);
                thread::spawn(move || m.check_account(acc));
            }
            last_check = Instant::now();
        }
        if last_log.elapsed() >= Duration::from_secs(5) {
            let telemetry = mgr.telemetry();
            println!("RAM: {:.2} MB | Lives: {} | Uptime: {}",
                telemetry["ramMb"].as_f64().unwrap_or(0.),
                telemetry["totalStreams"], telemetry["uptime"].as_str().unwrap_or(""));
            last_log = Instant::now();
        }
        thread::sleep(Duration::from_millis(200));
    }
    for w in mgr.watchers.lock().unwrap().values() { w.stop.store(true, Ordering::Relaxed); }
    mgr.log("Encerrando...");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_groups_default_to_two_slots() {
        let a = Account { streamers: vec!["example".into()], ..Default::default() };
        assert_eq!(a.groups()[0].max_concurrent, 2);
    }
    #[test]
    fn settings_have_expected_defaults() {
        let mut s = Settings::default();
        s.apply_defaults();
        assert_eq!(s.web_dashboard_port, 3007);
        assert_eq!(s.check_interval_seconds, 45);
        assert!(s.daily_claim_enabled.unwrap_or(true));
    }
}

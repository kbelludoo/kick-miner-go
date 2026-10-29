//! Chatbot — sends periodic announcements and responds to commands via Pusher WebSocket.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::kick_api::resolve_chatroom_id;

const PUSHER_WS_URL: &str =
    "wss://ws-us2.pusher.com/app/eb1d5f283081a78b932c?protocol=7&client=js&version=7.6.0&flash=false";
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(12 * 60);
const FIRST_ANNOUNCE_DELAY: Duration = Duration::from_secs(60);

fn bot_responses() -> HashMap<&'static str, &'static str> {
    let mut m = HashMap::new();
    m.insert("!bots", "[BOT FARM 24/7] Central Hub: https://kbelludoo.github.io/bot-template-go/ | Miner: !miner | Games: !huntera, !sigma, !leveling, !baiak, !tibidle | Repo: !github");
    m.insert("!hub", "Central VPS: https://kbelludoo.github.io/bot-template-go/");
    m.insert("!miner", "Kick Miner Rust: https://kbelludoo.github.io/kick-miner-go/ (Points & drops 24/7 in native Rust)");
    m.insert("!kickminer", "Kick Miner Rust: https://kbelludoo.github.io/kick-miner-go/ (Points & drops 24/7 in native Rust)");
    m.insert("!pontos", "Kick Miner: Auto mining up to 12 simultaneous streams! Dashboard: https://kbelludoo.github.io/kick-miner-go/");
    m.insert("!huntera", "Huntera Idle Dashboard: https://kbelludoo.github.io/huntera-bot/");
    m.insert("!tibidle", "Tibidle Multi-Bot Dashboard: https://kbelludoo.github.io/tibidle-bot/");
    m.insert("!sigma", "Pokemon Sigma Dashboard: https://kbelludoo.github.io/sigma-bot/");
    m.insert("!leveling", "Leveling Idle 2D Dashboard: https://kbelludoo.github.io/leveling-bot/");
    m.insert("!baiak", "Baiak Idle Fleet Dashboard: https://kbelludoo.github.io/baiak-idle-bot/");
    m.insert("!github", "GitHub Repo: https://github.com/kbelludoo/bot-template-go");
    m.insert("!vps", "VPS Oracle Cloud (sa-saopaulo-1) - Zero OBS - Native Go+Rust restream");
    m
}

pub struct Bot {
    token: String,
    streamer: String,
    http: reqwest::Client,
}

impl Bot {
    pub fn new(token: String, streamer: String, http: reqwest::Client) -> Self {
        Self {
            token,
            streamer: streamer.trim().to_lowercase(),
            http,
        }
    }

    pub async fn start(self: Arc<Self>, mut cancel: watch::Receiver<bool>) {
        if self.token.is_empty() || self.streamer.is_empty() {
            return;
        }

        // Resolve chatroom ID with retry
        let chatroom_id = loop {
            match resolve_chatroom_id(&self.http, &self.token, &self.streamer).await {
                Ok(id) if id > 0 => {
                    info!("[Chatbot] Connected to '{}' (chatroom {})", self.streamer, id);
                    break id;
                }
                Ok(_) | Err(_) => {}
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                _ = cancel.changed() => { return; }
            }
        };

        // Periodic announcements
        {
            let bot = self.clone();
            let mut cancel2 = cancel.clone();
            let _http = self.http.clone();
            tokio::spawn(async move {
                bot.periodic_announcements(chatroom_id, &mut cancel2).await;
            });
        }

        // Chat command listener
        self.listen_chat(chatroom_id, &mut cancel).await;
    }

    async fn send_message(&self, chatroom_id: i64, content: &str) -> Result<()> {
        let url = format!("https://kick.com/api/v2/messages/send/{chatroom_id}");
        let resp = self
            .http
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/plain, */*")
            .header("Origin", "https://kick.com")
            .header("Referer", format!("https://kick.com/{}", self.streamer))
            .json(&json!({"content": content, "type": "message"}))
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("send message status {status}: {body}");
        }
        Ok(())
    }

    async fn periodic_announcements(
        &self,
        chatroom_id: i64,
        cancel: &mut watch::Receiver<bool>,
    ) {
        // First announcement after 1 minute
        tokio::select! {
            _ = tokio::time::sleep(FIRST_ANNOUNCE_DELAY) => {}
            _ = cancel.changed() => { return; }
        }
        let _ = self.send_message(
            chatroom_id,
            "[BOT FARM 24/7] Live dashboards for all bots: https://kbelludoo.github.io/bot-template-go/ | Use !bots for individual links!",
        ).await;

        let mut ticker = tokio::time::interval(ANNOUNCE_INTERVAL);
        ticker.tick().await; // consume first

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let _ = self.send_message(
                        chatroom_id,
                        "[BOT FARM 24/7] Live telemetry, gold & XP: https://kbelludoo.github.io/bot-template-go/ | Miner: https://kbelludoo.github.io/kick-miner-go/ | Commands: !bots, !miner, !huntera, !sigma, !baiak",
                    ).await;
                }
                _ = cancel.changed() => { return; }
            }
        }
    }

    async fn listen_chat(&self, chatroom_id: i64, cancel: &mut watch::Receiver<bool>) {
        let responses = bot_responses();

        loop {
            if *cancel.borrow() {
                return;
            }

            let conn = connect_async(PUSHER_WS_URL).await;
            let (ws, _) = match conn {
                Ok(v) => v,
                Err(e) => {
                    warn!("[Chatbot] WS connect failed: {e}, retry in 10s");
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                        _ = cancel.changed() => { return; }
                    }
                    continue;
                }
            };

            let (mut sink, mut stream) = ws.split();

            // Subscribe to chatroom channel
            let sub = json!({
                "event": "pusher:subscribe",
                "data": {
                    "auth": "",
                    "channel": format!("chatrooms.{chatroom_id}.v2")
                }
            });
            let _ = sink.send(Message::Text(sub.to_string())).await;

            'inner: loop {
                tokio::select! {
                    msg = stream.next() => {
                        match msg {
                            None | Some(Err(_)) => break 'inner,
                            Some(Ok(Message::Text(txt))) => {
                                self.handle_chat_event(&txt, &responses, &mut sink).await;
                            }
                            Some(Ok(_)) => {}
                        }
                    }
                    _ = cancel.changed() => { return; }
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                _ = cancel.changed() => { return; }
            }
        }
    }

    async fn handle_chat_event(
        &self,
        raw: &str,
        responses: &HashMap<&str, &str>,
        _sink: &mut futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
    ) {
        let Ok(event) = serde_json::from_str::<serde_json::Value>(raw) else {
            return;
        };

        if event["event"] != r#"App\Events\ChatMessageEvent"# {
            return;
        }

        let data_str = event["data"].as_str().unwrap_or("");
        let Ok(chat) = serde_json::from_str::<serde_json::Value>(data_str) else {
            return;
        };

        let content = chat["content"].as_str().unwrap_or("").trim().to_lowercase();
        let sender = chat["sender"]["username"].as_str().unwrap_or("?");

        if let Some(reply) = responses.get(content.as_str()) {
            info!("[Chatbot] Responding to command '{content}' from '{sender}'");
            // Send message (fire-and-forget)
            let bot_clone = self.http.clone();
            let reply_str = format!("@{sender} {reply}");
            let token = self.token.clone();
            let streamer = self.streamer.clone();
            let chatroom_id = chat["chatroom_id"].as_i64().unwrap_or(0);
            if chatroom_id > 0 {
                tokio::spawn(async move {
                    let url = format!("https://kick.com/api/v2/messages/send/{chatroom_id}");
                    let _ = bot_clone
                        .post(&url)
                        .header("Authorization", format!("Bearer {token}"))
                        .header("Content-Type", "application/json")
                        .header("Accept", "application/json, text/plain, */*")
                        .header("Origin", "https://kick.com")
                        .header("Referer", format!("https://kick.com/{streamer}"))
                        .json(&json!({"content": reply_str, "type": "message"}))
                        .send()
                        .await;
                });
            }
        }
    }
}

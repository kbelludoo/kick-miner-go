use std::time::Duration;

use serde_json::json;
use futures_util::{SinkExt, StreamExt};

use crate::config::ChatterConfig;

/// Humanized LLM chatter for a single streamer channel.
pub async fn run_chatter(
    token: String,
    _username: String,
    streamer: String,
    cfg: ChatterConfig,
) {
    use tokio::time::sleep;
    use rand::Rng;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("reqwest client");

    // Resolve chatroom id
    let chatroom_id = match crate::kick_api::resolve_chatroom_id(&client, &token, &streamer).await {
        Ok(id) => id,
        Err(e) => {
            tracing::warn!("[Chatter/{streamer}] falha ao resolver chatroom: {e}");
            return;
        }
    };

    tracing::info!("[Chatter/{streamer}] chatroom {chatroom_id} — iniciando chatter humanizado");

    loop {
        let min = cfg.min_interval_minutes.max(1);
        let max = cfg.max_interval_minutes.max(min + 1);
        let wait_mins = rand::thread_rng().gen_range(min..=max);
        sleep(Duration::from_secs(wait_mins * 60)).await;

        // Generate message via LLM
        let msg = match generate_llm_message(&cfg).await {
            Some(m) => m,
            None => "🎮 Bots 24/7 farmando pontos! kbelludoo.github.io/bot-template-rust/".to_string(),
        };

        // POST to Kick chat
        let url = format!("https://kick.com/api/v2/messages/send/{chatroom_id}");
        let payload = json!({"content": msg, "type": "message"});
        let res = client
            .post(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .header("Origin", "https://kick.com")
            .header("Referer", format!("https://kick.com/{streamer}"))
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .json(&payload)
            .send()
            .await;

        match res {
            Ok(r) if r.status().is_success() => {
                tracing::debug!("[Chatter/{streamer}] mensagem enviada: {msg}");
            }
            Ok(r) => {
                tracing::warn!("[Chatter/{streamer}] status {}: {msg}", r.status());
            }
            Err(e) => {
                tracing::warn!("[Chatter/{streamer}] erro ao enviar: {e}");
            }
        }
    }
}

async fn generate_llm_message(cfg: &ChatterConfig) -> Option<String> {
    if cfg.llm_api_key.is_empty() || cfg.llm_base_url.is_empty() {
        return None;
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .ok()?;

    let payload = json!({
        "model": cfg.llm_model,
        "messages": [
            {"role": "user", "content": "Gere uma mensagem curta e natural (máximo 80 chars) em pt-BR para enviar no chat da Kick, como se fosse um espectador assistindo a live. Seja casual e variado. Só a mensagem, sem aspas."}
        ],
        "max_tokens": 60,
        "temperature": 1.1
    });

    let url = format!("{}/chat/completions", cfg.llm_base_url.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", cfg.llm_api_key))
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await
        .ok()?;

    let body: serde_json::Value = resp.json().await.ok()?;
    body["choices"][0]["message"]["content"]
        .as_str()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

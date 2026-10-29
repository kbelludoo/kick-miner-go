//! Daily gamification claim loop.
//! Polls https://web.kick.com/api/v1/gamification/challenges,
//! claims daily rewards, and sleeps until the next window.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use chrono::DateTime;
use serde::Deserialize;
use tokio::sync::watch;

use crate::config::Config;
use crate::telemetry::SharedState;

const CHALLENGES_URL: &str = "https://web.kick.com/api/v1/gamification/challenges";
const ERROR_BACKOFF: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Deserialize)]
struct Challenge {
    #[serde(default)]
    id: String,
    #[serde(default)]
    recurrence: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    window: Window,
    #[serde(default)]
    condition: Condition,
    #[serde(default)]
    winner: Option<Winner>,
}

#[derive(Debug, Deserialize, Default)]
struct Window {
    #[serde(default, rename = "ends_at")]
    ends_at: String,
}

#[derive(Debug, Deserialize, Default)]
struct Condition {
    #[serde(default)]
    threshold: f64,
    #[serde(default)]
    progress: f64,
}

#[derive(Debug, Deserialize, Default)]
struct Winner {
    #[serde(default)]
    name: String,
    #[serde(default)]
    rarity: String,
}

pub async fn run_daily_claims(
    cfg: Arc<Config>,
    state: SharedState,
    http: reqwest::Client,
    mut cancel: watch::Receiver<bool>,
) {
    let check_interval = Duration::from_secs(cfg.settings.daily_claim_check_minutes * 60);

    for acc in &cfg.accounts {
        if acc.bearer_token.is_empty() {
            continue;
        }
        let token = acc.bearer_token.clone();
        let username = acc.username.clone();
        let http2 = http.clone();
        let state2 = state.clone();
        let mut cancel2 = cancel.clone();
        let fallback = check_interval;

        tokio::spawn(async move {
            loop {
                let wait = process_daily(&http2, &state2, &username, &token, fallback).await;

                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = cancel2.changed() => { return; }
                }
            }
        });
    }

    // Keep the outer task alive until cancelled
    let _ = cancel.changed().await;
}

async fn process_daily(
    http: &reqwest::Client,
    state: &SharedState,
    username: &str,
    token: &str,
    fallback: Duration,
) -> Duration {
    let ch = match fetch_daily_challenge(http, token).await {
        Ok(c) => c,
        Err(e) => {
            let mut st = state.lock().await;
            st.add_log(&format!(
                "[{username}] Failed to query daily reward: {e} (retry in 30min)"
            ));
            return ERROR_BACKOFF;
        }
    };

    let Some(ch) = ch else {
        let mut st = state.lock().await;
        st.add_log(&format!(
            "[{username}] Daily reward not found in API (retry in {fallback:?})"
        ));
        return fallback;
    };

    match ch.status.as_str() {
        "claimed" => {
            let reset = daily_reset_time(&ch);
            {
                let mut st = state.lock().await;
                st.add_log(&format!(
                    "[{username}] Daily reward already claimed. Next claim: {}",
                    reset.format("%d/%m %H:%M")
                ));
            }
            until_next_check(reset, fallback)
        }
        "claimable" => {
            if ch.id.is_empty() {
                return fallback;
            }
            match claim_daily(http, token, &ch.id).await {
                Err(e) => {
                    let mut st = state.lock().await;
                    st.add_log(&format!(
                        "[{username}] Failed to claim daily reward: {e} (retry in 30min)"
                    ));
                    ERROR_BACKOFF
                }
                Ok(()) => {
                    // Fetch updated challenge for reward name
                    let reward = match fetch_daily_challenge(http, token).await {
                        Ok(Some(ch2)) => {
                            let w = ch2.winner.unwrap_or_default();
                            if w.name.is_empty() {
                                String::new()
                            } else if w.rarity.is_empty() {
                                w.name
                            } else {
                                format!("{} ({})", w.name, w.rarity)
                            }
                        }
                        _ => String::new(),
                    };
                    let reset = daily_reset_time(&ch);
                    let mut st = state.lock().await;
                    if reward.is_empty() {
                        st.add_log(&format!(
                            "[{username}] Daily reward claimed! Next claim: {}",
                            reset.format("%d/%m %H:%M")
                        ));
                    } else {
                        st.add_log(&format!(
                            "[{username}] Daily reward claimed: {reward}! Next claim: {}",
                            reset.format("%d/%m %H:%M")
                        ));
                    }
                    until_next_check(reset, fallback)
                }
            }
        }
        "in_progress" => {
            let remaining = (ch.condition.threshold - ch.condition.progress).max(1.0);
            let next = Duration::from_secs_f64(remaining * 60.0).min(fallback);
            next
        }
        _ => fallback,
    }
}

async fn fetch_daily_challenge(
    http: &reqwest::Client,
    token: &str,
) -> Result<Option<Challenge>> {
    let resp = http
        .get(CHALLENGES_URL)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/json, text/plain, */*")
        .header("Origin", "https://kick.com")
        .header("Referer", "https://kick.com/")
        .header("x-app-platform", "web")
        .header("Cookie", format!("session_token={token}"))
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        return Err(anyhow!("challenges status {status}"));
    }

    let body: serde_json::Value = resp.json().await?;

    // Try wrapped and unwrapped arrays
    let list: Vec<Challenge> = if let Some(arr) = body["data"].as_array() {
        serde_json::from_value(serde_json::Value::Array(arr.clone()))
            .unwrap_or_default()
    } else if body.is_array() {
        serde_json::from_value(body).unwrap_or_default()
    } else {
        vec![]
    };

    Ok(list.into_iter().find(|c| c.recurrence == "daily"))
}

async fn claim_daily(http: &reqwest::Client, token: &str, id: &str) -> Result<()> {
    let url = format!(
        "{CHALLENGES_URL}/{}/claim",
        id
    );
    let resp = http
        .post(&url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/plain, */*")
        .header("Origin", "https://kick.com")
        .header("Referer", "https://kick.com/")
        .header("x-app-platform", "web")
        .header("Cookie", format!("session_token={token}"))
        .body("{}")
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        let snippet = &body[..body.len().min(120)];
        return Err(anyhow!("claim status {status}: {snippet}"));
    }
    Ok(())
}

fn daily_reset_time(ch: &Challenge) -> chrono::DateTime<chrono::Utc> {
    if !ch.window.ends_at.is_empty() {
        if let Ok(t) = DateTime::parse_from_rfc3339(&ch.window.ends_at) {
            return t.with_timezone(&chrono::Utc) + chrono::Duration::minutes(1);
        }
    }
    // Fallback: 00:02 UTC next day
    let now = chrono::Utc::now();
    let tomorrow = now.date_naive().succ_opt().unwrap_or(now.date_naive());
    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
        tomorrow.and_hms_opt(0, 2, 0).expect("valid time"),
        chrono::Utc,
    )
}

fn until_next_check(
    reset: chrono::DateTime<chrono::Utc>,
    fallback: Duration,
) -> Duration {
    let diff = reset - chrono::Utc::now();
    let secs = diff.num_seconds();
    if secs <= 60 {
        return fallback;
    }
    Duration::from_secs(secs as u64 + 60) // +1 min margin
}

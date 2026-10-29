use anyhow::{anyhow, Context, Result};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, ORIGIN, REFERER};
use serde::Deserialize;

pub const CLIENT_TOKEN: &str = "e1393935a959b4020a4491574f6490129f678acdaa92760471263db43487f823";
pub const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

/// Shared HTTP client for all Kick API calls.
pub fn build_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .context("build reqwest client")
}

#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub slug: String,
    pub channel_id: i64,
    pub user_id: i64,
    pub stream_id: i64,
    pub is_live: bool,
}

/// GET https://kick.com/api/v2/channels/{streamer}
pub async fn get_channel_info(
    client: &reqwest::Client,
    token: &str,
    streamer: &str,
) -> Result<ChannelInfo> {
    let url = format!("https://kick.com/api/v2/channels/{streamer}");
    let resp = client
        .get(&url)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(ORIGIN, "https://kick.com")
        .header(REFERER, format!("https://kick.com/{streamer}/"))
        .header("Accept", "application/json, text/plain, */*")
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        .header("Cookie", format!("session_token={token}"))
        .send()
        .await
        .context("channel info request")?;

    let status = resp.status();
    if !status.is_success() {
        return Err(anyhow!("channel info status {status}"));
    }

    let body: serde_json::Value = resp.json().await.context("channel info parse")?;

    let channel_id = body["id"]
        .as_f64()
        .ok_or_else(|| anyhow!("missing channel id"))? as i64;

    let user_id = body["user_id"].as_f64().unwrap_or(0.0) as i64;

    let livestream = &body["livestream"];
    let is_live = livestream.is_object() && !livestream.is_null();
    let stream_id = if is_live {
        livestream["id"].as_f64().unwrap_or(0.0) as i64
    } else {
        0
    };

    Ok(ChannelInfo {
        slug: streamer.to_string(),
        channel_id,
        user_id,
        stream_id,
        is_live,
    })
}

/// GET https://kick.com/api/v2/channels/{streamer}/points → data.points
pub async fn fetch_points(
    client: &reqwest::Client,
    token: &str,
    streamer: &str,
) -> Result<i64> {
    #[derive(Deserialize)]
    struct Outer {
        data: Inner,
    }
    #[derive(Deserialize)]
    struct Inner {
        points: i64,
    }

    let url = format!("https://kick.com/api/v2/channels/{streamer}/points");
    let resp = client
        .get(&url)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(ORIGIN, "https://kick.com")
        .header(REFERER, format!("https://kick.com/{streamer}/"))
        .header("Accept", "application/json, text/plain, */*")
        .send()
        .await
        .context("points request")?;

    let status = resp.status();
    if !status.is_success() {
        return Err(anyhow!("points status {status}"));
    }

    let outer: Outer = resp.json().await.context("points parse")?;
    Ok(outer.data.points)
}

/// GET https://websockets.kick.com/viewer/v1/token
pub async fn fetch_viewer_ws_token(
    client: &reqwest::Client,
    token: &str,
    streamer: &str,
    channel_id: i64,
    user_id: i64,
) -> Result<String> {
    #[derive(Deserialize)]
    struct Outer {
        #[serde(default)]
        data: Option<Inner>,
        #[serde(default)]
        token: Option<String>,
    }
    #[derive(Deserialize)]
    struct Inner {
        token: String,
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).context("auth header")?,
    );
    headers.insert("X-Chatroom", HeaderValue::from_str(&channel_id.to_string()).context("chatroom header")?);
    headers.insert("X-User-Id", HeaderValue::from_str(&user_id.to_string()).context("user-id header")?);
    headers.insert("X-Client-Token", HeaderValue::from_static(CLIENT_TOKEN));
    headers.insert(ORIGIN, HeaderValue::from_static("https://kick.com"));
    headers.insert(
        REFERER,
        HeaderValue::from_str(&format!("https://kick.com/{streamer}/")).context("referer header")?,
    );
    headers.insert("Accept", HeaderValue::from_static("application/json, text/plain, */*"));

    let resp = client
        .get("https://websockets.kick.com/viewer/v1/token")
        .headers(headers)
        .send()
        .await
        .context("viewer token request")?;

    let status = resp.status();
    if !status.is_success() {
        return Err(anyhow!("viewer token status {status}"));
    }

    let outer: Outer = resp.json().await.context("viewer token parse")?;

    if let Some(inner) = outer.data {
        if !inner.token.is_empty() {
            return Ok(inner.token);
        }
    }
    if let Some(t) = outer.token {
        if !t.is_empty() {
            return Ok(t);
        }
    }

    Err(anyhow!("viewer token not found in response"))
}

/// Discover live streamers from Kick public endpoints (PT and EN) matching Go miner 100%
pub async fn discover_live_streamers(client: &reqwest::Client) -> Vec<String> {
    let endpoints = [
        "https://kick.com/stream/featured-livestreams/pt",
        "https://kick.com/stream/featured-livestreams/en",
        "https://kick.com/stream/livestreams/pt?page=1&limit=30",
        "https://kick.com/stream/livestreams/en?page=1&limit=30",
    ];

    let mut result = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for url in endpoints {
        let resp = client
            .get(url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .header("Accept", "application/json, text/plain, */*")
            .header(ORIGIN, "https://kick.com")
            .send()
            .await;

        let resp = match resp {
            Ok(r) if r.status().is_success() => r,
            _ => continue,
        };

        let body: serde_json::Value = match resp.json().await {
            Ok(v) => v,
            Err(_) => continue,
        };

        if let Some(arr) = body["data"].as_array() {
            for item in arr {
                if let Some(slug) = item["channel"]["slug"].as_str().or_else(|| item["slug"].as_str()) {
                    let clean = slug.trim().to_lowercase();
                    if !clean.is_empty() && seen.insert(clean.clone()) {
                        result.push(clean);
                    }
                }
            }
            continue;
        }

        if let Some(arr) = body.as_array() {
            for item in arr {
                if let Some(slug) = item["channel"]["slug"].as_str().or_else(|| item["slug"].as_str()) {
                    let clean = slug.trim().to_lowercase();
                    if !clean.is_empty() && seen.insert(clean.clone()) {
                        result.push(clean);
                    }
                }
            }
        }
    }

    result
}

/// Resolve chatroom ID from channel API
pub async fn resolve_chatroom_id(
    client: &reqwest::Client,
    token: &str,
    streamer: &str,
) -> Result<i64> {
    let url = format!("https://kick.com/api/v2/channels/{streamer}");
    let body: serde_json::Value = client
        .get(&url)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(ORIGIN, "https://kick.com")
        .header("Accept", "application/json")
        .send()
        .await
        .context("resolve chatroom request")?
        .json()
        .await
        .context("resolve chatroom parse")?;

    body["chatroom"]["id"]
        .as_f64()
        .map(|f| f as i64)
        .ok_or_else(|| anyhow!("chatroom id not found"))
}

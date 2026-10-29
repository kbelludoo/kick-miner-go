
use axum::{
    extract::State,
    http::header,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use tokio::net::TcpListener;

use crate::telemetry::SharedState;

pub async fn start(port: u16, state: SharedState) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/", get(dashboard))
        .route("/api/status", get(api_status))
        .route("/metrics", get(metrics))
        .with_state(state);

    let addr = format!("0.0.0.0:{port}");
    tracing::info!("Dashboard Kick Miner online em http://localhost:{port}");
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn dashboard() -> Response {
    let html = include_str!("../docs/index.html");
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
        .into_response()
}

async fn api_status(State(state): State<SharedState>) -> impl IntoResponse {
    let guard = state.lock().await;
    let tel = guard.build_telemetry();
    drop(guard);
    Json(tel)
}

async fn metrics(State(state): State<SharedState>) -> impl IntoResponse {
    let guard = state.lock().await;
    let tel = guard.build_telemetry();
    drop(guard);

    let mut out = String::new();
    out.push_str(&format!("kick_ram_mb {:.4}\n", tel.ram_mb));
    out.push_str(&format!("kick_total_streams {}\n", tel.total_streams));
    out.push_str(&format!("kick_threads {}\n", tel.goroutines));
    for (uname, acc) in &tel.accounts {
        out.push_str(&format!(
            "kick_account_points{{account=\"{}\"}} {}\n",
            uname, acc.total_points
        ));
        out.push_str(&format!(
            "kick_account_streams{{account=\"{}\"}} {}\n",
            uname, acc.active_streams
        ));
    }
    ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], out)
}

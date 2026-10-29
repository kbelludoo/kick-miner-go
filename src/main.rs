mod chatbot;
mod chatter;
mod config;
mod daily;
mod kick_api;
mod manager;
mod server;
mod telemetry;
mod watcher;


use anyhow::Context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialise tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("================================================================================");
    println!("     ⚡ KICK CHANNEL POINTS MINER RUST (ULTRA-LEVE & ALTA PERFORMANCE) ⚡      ");
    println!("              Zero Navegadores • Zero Docker • Zero-GC • ~2MB RAM              ");
    println!("================================================================================");

    // Resolve config path (same dir as executable, or first CLI arg)
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.json".to_string());

    let cfg = config::load_config(&config_path)
        .with_context(|| format!("Erro na configuração: {config_path}"))?;

    tracing::info!(
        "Configuração carregada: {} conta(s), porta {}",
        cfg.accounts.len(),
        cfg.settings.web_dashboard_port
    );

    // Build shared state
    let state = telemetry::AppState::new_shared(&cfg.accounts);

    // Launch HTTP server (non-blocking)
    {
        let state_clone = state.clone();
        let port = cfg.settings.web_dashboard_port;
        tokio::spawn(async move {
            if let Err(e) = server::start(port, state_clone).await {
                tracing::error!("HTTP server error: {e}");
            }
        });
    }

    // Launch manager (owns the main loop)
    manager::run(cfg, state).await;

    Ok(())
}

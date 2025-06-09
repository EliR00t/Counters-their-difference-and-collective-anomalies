use crate::errors::TelemetryError;
use clap::Parser;
use polars::prelude::{CsvWriter, SerWriter};
use std::fs::File;
use tracing::info;
use tracing_subscriber::{self, fmt::time::ChronoUtc, EnvFilter};

mod config;
mod errors;
mod http_load;
mod telemetry;

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_timer(ChronoUtc::new("%Y-%m-%d %H:%M:%S%.3f UTC".to_string()))
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(true)
        .with_target(false)
        .init();
}

#[tokio::main]
async fn main() -> Result<(), TelemetryError> {
    init_tracing();
    let cfg = config::Config::parse();
    let (mut df, load_bytes) = tokio::try_join!(
        telemetry::snmp_telemetry(&cfg.snmp),
        http_load::generate_load(&cfg.http_load)
    )?;
    info!("Load total bytes: {load_bytes}");
    let mut f = File::create(format!("rates_{}.csv", cfg.http_load.banner))?;
    CsvWriter::new(&mut f)
        .include_header(true)
        .with_separator(b',')
        .finish(&mut df)?;
    Ok(())
}

use crate::{
    config::{HTTPLoadConfig, LOAD_PERIOD},
    errors::TelemetryError,
};
use futures::{stream, StreamExt};
use reqwest::Client;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::time::{sleep, Duration, Instant};
use tracing::{error, info};

macro_rules! http_port {
    () => {
        "8080"
    };
}

const TRANSITION_DELAY: Duration = Duration::new(5, 0);

#[derive(Clone)]
struct HTTPLoadStats {
    total_bytes: Arc<AtomicU64>,
}

impl HTTPLoadStats {
    fn new() -> Self {
        HTTPLoadStats {
            total_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    fn add(&self, bytes: usize) {
        self.total_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }
}

pub async fn generate_load(cfg: &HTTPLoadConfig) -> Result<u64, TelemetryError> {
    let urls = vec![
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/benches/"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/build.rs"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/Cargo.toml"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/CHANGELOG.md"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/CONTRIBUTING.md"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/docker/"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/docs/"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/fuzz/"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/LICENSE-APACHE"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/LICENSE-MIT"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/Makefile"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/README.md"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/scripts/"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/SECURITY.md"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/src/"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/systemd/"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/target/"
        ),
        concat!(
            "http://",
            crate::hostname!(),
            ":",
            http_port!(),
            "/static-web-server/tests/"
        ),
    ];
    let stats = HTTPLoadStats::new();
    let client = Arc::new(Client::new());
    info!("{} normal load", cfg.banner);
    let start = Instant::now();
    while start.elapsed() < LOAD_PERIOD {
        let bodies = stream::iter(urls.iter())
            .map(|u| {
                let client = Arc::clone(&client);
                let stats = stats.clone();
                async move {
                    let resp = client.get(*u).send().await;
                    match resp {
                        Ok(r) => {
                            let bytes = r.bytes().await?;
                            stats.add(bytes.len());
                            Ok(())
                        }
                        Err(e) => {
                            error!("HTTP error: {}", e);
                            Err(TelemetryError::HttpError(e))
                        }
                    }
                }
            })
            .buffer_unordered(cfg.normal_load_concurrency);

        bodies
            .for_each(|ret| async {
                if let Err(e) = ret {
                    error!("Got an error: {}", e);
                }
            })
            .await;
        sleep(cfg.normal_load_sleep).await;
    }

    sleep(TRANSITION_DELAY).await;

    info!("{} heavy load", cfg.banner);
    let start = Instant::now();
    while start.elapsed() < LOAD_PERIOD {
        let bodies = stream::iter(urls.iter())
            .map(|u| {
                let client = Arc::clone(&client);
                let stats = stats.clone();
                async move {
                    let resp = client.get(*u).send().await;
                    match resp {
                        Ok(r) => {
                            let bytes = r.bytes().await?;
                            stats.add(bytes.len());
                            Ok(())
                        }
                        Err(e) => {
                            error!("HTTP error: {}", e);
                            Err(TelemetryError::HttpError(e))
                        }
                    }
                }
            })
            .buffer_unordered(cfg.heavy_load_concurrency);

        bodies
            .for_each(|ret| async {
                if let Err(e) = ret {
                    error!("Got an error: {}", e);
                }
            })
            .await;
        sleep(cfg.heavy_load_sleep).await;
    }

    Ok(stats.total_bytes())
}

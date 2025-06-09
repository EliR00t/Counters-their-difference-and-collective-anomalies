use std::{io::{Error as IoError, ErrorKind}, net::{SocketAddr, ToSocketAddrs}, env, time::Duration};
use clap::builder::ValueParser;
use clap::Parser;
use csnmp::Snmp2cClient;
use humantime::parse_duration;
use crate::errors::TelemetryError;

pub const LOAD_PERIOD: Duration = Duration::new(60 * 60, 0);
pub const SNMP_POLL_INTERVAL: Duration = Duration::new(60, 0);

#[macro_export]
macro_rules! hostname {
    () => {
        "betelgeuse"
    };
}

#[derive(Debug, Parser)]
pub struct SnmpConfig {
    /// SNMP port
    #[arg(short('p'), long, default_value_t = 161)]
    pub snmp_port: u16,
    /// SNMP retries
    #[arg(short('r'), long, default_value_t = 3)]
    pub snmp_retries: usize,
    /// SNMP timeout
    #[arg(short('t'), 
        long, 
        default_value = "15s", 
        value_parser = ValueParser::new(|s: &str| parse_duration(s).map_err(|e| e.to_string())))]
    pub snmp_timeout: Duration,
}

#[derive(Debug, Parser)]
pub struct HTTPLoadConfig {
    /// Sleep interval for a normal load 
    #[arg(short('s'), 
        long,
        default_value = "3s",
        value_parser = ValueParser::new(|s: &str| parse_duration(s).map_err(|e| e.to_string())))]
    pub normal_load_sleep: Duration,
    /// Normal load concurrency 
    #[arg(short('c'), long, default_value_t = 1)]
    pub normal_load_concurrency: usize,
    /// Sleep interval for a heavy load 
    #[arg(short('e'), 
        long,
        default_value = "600ms",
        value_parser = ValueParser::new(|s: &str| parse_duration(s).map_err(|e| e.to_string())))]
    pub heavy_load_sleep: Duration,
    /// Heavy load concurrency 
    #[arg(short('u'), long, default_value_t = 4)]
    pub heavy_load_concurrency: usize,
    /// Banner
    #[arg(short('b'), long, default_value = "regular")]
    pub banner: String,
}

#[derive(Debug, Parser)]
#[command(name = "poller", version = "1.0")]
#[command(name = "Ilia Rassadin, ilia.rassadin@gmail.com")]
#[command(
    about = "Traffic generator, SNMP poller and Counter32/64 difference calculator."
)]
#[command(
    long_about = "Start HTTP server on one box and this program on another. This program 
generates a number of HTTP requests and at the same time collects SNMP telemetry data."
)]
#[command(help_template = "
{before-help}{name} {version}
{author-with-newline}{about-with-newline}
{usage-heading} {usage}

{all-args}{after-help}
")]
pub struct Config {
    #[command(flatten)]
    pub snmp: SnmpConfig,
    #[command(flatten)]
    pub http_load: HTTPLoadConfig,
}

impl SnmpConfig {
    fn get_target(&self) -> Result<SocketAddr, TelemetryError> {
        let addr_str = format!("{}:{}", hostname!(), self.snmp_port);
        let mut addr = addr_str.to_socket_addrs().map_err(|e| TelemetryError::DnsError(e))?;
        addr.next().ok_or_else(|| TelemetryError::DnsError(IoError::new(ErrorKind::Other, "No address resolved")))
    }

    pub async fn snmp_client(&self) -> Result<Snmp2cClient, TelemetryError> {
        let community: Vec<u8> = env::var("COMMUNITY").unwrap_or_else(|_| "public".to_string()).as_bytes().to_vec();
        let bind_addr: Option<SocketAddr> = "0.0.0.0:0".parse::<SocketAddr>().ok();
        let target: SocketAddr = self.get_target()?;
        Ok(Snmp2cClient::new(target, community, bind_addr, Some(self.snmp_timeout), self.snmp_retries).await?)
    }
}

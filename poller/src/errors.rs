use csnmp::{ObjectIdentifierConversionError, SnmpClientError};
use polars::prelude::PolarsError;
use reqwest::Error as ReqwestError;
use std::{error::Error, fmt, io, io::ErrorKind, net::AddrParseError};
use tokio::task::JoinError;

#[derive(Debug)]
pub enum TelemetryError {
    ClientError(SnmpClientError),
    ParseError(AddrParseError),
    OidError(ObjectIdentifierConversionError),
    TaskError(JoinError),
    DnsError(io::Error),
    HttpError(ReqwestError),
    PolarsError(PolarsError),
    InvalidInputError(io::Error),
}

impl Error for TelemetryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TelemetryError::ClientError(err) => Some(err),
            TelemetryError::ParseError(err) => Some(err),
            TelemetryError::OidError(err) => Some(err),
            TelemetryError::TaskError(err) => Some(err),
            TelemetryError::DnsError(err) => Some(err),
            TelemetryError::HttpError(err) => Some(err),
            TelemetryError::PolarsError(err) => Some(err),
            TelemetryError::InvalidInputError(err) => Some(err),
        }
    }
}

impl fmt::Display for TelemetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TelemetryError::ClientError(err) => write!(f, "SNMP client error: {}", err),
            TelemetryError::ParseError(err) => write!(f, "Parse address error: {}", err),
            TelemetryError::OidError(err) => write!(f, "OID error: {}", err),
            TelemetryError::TaskError(err) => write!(f, "Tokio task error: {}", err),
            TelemetryError::DnsError(err) => write!(f, "DNS error: {}", err),
            TelemetryError::HttpError(err) => write!(f, "HTTP reqwest error: {}", err),
            TelemetryError::PolarsError(err) => write!(f, "Polars error: {}", err),
            TelemetryError::InvalidInputError(err) => {
                write!(f, "Invalid input data for Telemetry error: {}", err)
            }
        }
    }
}

impl From<SnmpClientError> for TelemetryError {
    fn from(err: SnmpClientError) -> Self {
        TelemetryError::ClientError(err)
    }
}

impl From<AddrParseError> for TelemetryError {
    fn from(err: AddrParseError) -> Self {
        TelemetryError::ParseError(err)
    }
}

impl From<ObjectIdentifierConversionError> for TelemetryError {
    fn from(err: ObjectIdentifierConversionError) -> Self {
        TelemetryError::OidError(err)
    }
}

impl From<JoinError> for TelemetryError {
    fn from(err: JoinError) -> Self {
        TelemetryError::TaskError(err)
    }
}

impl From<io::Error> for TelemetryError {
    fn from(err: io::Error) -> Self {
        match err.kind() {
            ErrorKind::InvalidInput => TelemetryError::InvalidInputError(err),
            _ => TelemetryError::DnsError(err),
        }
    }
}

impl From<ReqwestError> for TelemetryError {
    fn from(err: ReqwestError) -> Self {
        TelemetryError::HttpError(err)
    }
}

impl From<PolarsError> for TelemetryError {
    fn from(err: PolarsError) -> Self {
        TelemetryError::PolarsError(err)
    }
}

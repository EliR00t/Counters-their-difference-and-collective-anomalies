use crate::{
    config::{SnmpConfig, LOAD_PERIOD, SNMP_POLL_INTERVAL},
    errors::TelemetryError,
};
use chrono;
use csnmp::{ObjectIdentifier, ObjectValue};
use polars::prelude::*;
use std::{collections::HashMap, io::Error, io::ErrorKind, str::FromStr};
use tokio::time::{sleep, Instant};
use tracing::{error, info};

const NUM_POINTS: usize = 100;

#[derive(Debug, Clone)]
enum MetricType {
    Counter32(Vec<u32>),
    Counter64(Vec<u64>),
}

macro_rules! ifIndex {
    () => {
        ".3"
    };
}
macro_rules! ifHCInOctets {
    () => {
        ".1.3.6.1.2.1.31.1.1.1.6"
    };
}
macro_rules! ifHCOutOctets {
    () => {
        ".1.3.6.1.2.1.31.1.1.1.10"
    };
}
macro_rules! ifInErrors {
    () => {
        ".1.3.6.1.2.1.2.2.1.14"
    };
}
macro_rules! ifOutErrors {
    () => {
        ".1.3.6.1.2.1.2.2.1.20"
    };
}
macro_rules! ifInDiscards {
    () => {
        ".1.3.6.1.2.1.2.2.1.13"
    };
}
macro_rules! ifOutDiscards {
    () => {
        ".1.3.6.1.2.1.2.2.1.19"
    };
}
macro_rules! ifHCInUcastPkts {
    () => {
        ".1.3.6.1.2.1.31.1.1.1.7"
    };
}
macro_rules! ifHCOutUcastPkts {
    () => {
        ".1.3.6.1.2.1.31.1.1.1.11"
    };
}

macro_rules! define_metrics {
    ($($variant:ident => $name:expr),* $(,)?) => {
        #[derive(Clone, Copy, Debug)]
        pub enum MetricName {
            $($variant),*
        }

        pub const METRIC_NAMES: &[&'static str] = &[$($name),*];

        impl MetricName {
            pub fn as_str(&self) -> &'static str {
                METRIC_NAMES[*self as usize]
            }
        }
    };
}

define_metrics! {
    IfHCInOctets => "ifHCInOctets",
    IfHCOutOctets => "ifHCOutOctets",
    IfInErrors => "ifInErrors",
    IfOutErrors => "ifOutErrors",
    IfInDiscards => "ifInDiscards",
    IfOutDiscards => "ifOutDiscards",
    IfHCInUcastPkts => "ifHCInUcastPkts",
    IfHCOutUcastPkts => "ifHCOutUcastPkts",
}

#[derive(Debug, Clone)]
struct Metric {
    name: &'static str,
    values: MetricType,
}

impl Metric {
    fn new(name: &'static str, values: MetricType) -> Self {
        Self {
            name,
            values: values,
        }
    }
}

#[derive(Debug)]
struct Telemetry {
    metrics: HashMap<ObjectIdentifier, Metric>,
    ts: Vec<i64>,
}

#[derive(Debug)]
struct TelemetryBuilder {
    if_hc_in_octets: Option<Vec<u64>>,
    if_hc_out_octets: Option<Vec<u64>>,
    if_in_errors: Option<Vec<u32>>,
    if_out_errors: Option<Vec<u32>>,
    if_in_discards: Option<Vec<u32>>,
    if_out_discards: Option<Vec<u32>>,
    if_hc_in_ucast_pkts: Option<Vec<u64>>,
    if_hc_out_ucast_pkts: Option<Vec<u64>>,
    ts: Option<Vec<i64>>,
}

impl TelemetryBuilder {
    fn new() -> Self {
        TelemetryBuilder {
            if_hc_in_octets: None,
            if_hc_out_octets: None,
            if_in_errors: None,
            if_out_errors: None,
            if_in_discards: None,
            if_out_discards: None,
            if_hc_in_ucast_pkts: None,
            if_hc_out_ucast_pkts: None,
            ts: None,
        }
    }

    #[cfg(test)]
    fn with_if_hc_in_octets(mut self, data: Vec<u64>) -> Self {
        self.if_hc_in_octets = Some(data);
        self
    }

    #[cfg(test)]
    fn with_if_hc_out_octets(mut self, data: Vec<u64>) -> Self {
        self.if_hc_out_octets = Some(data);
        self
    }

    #[cfg(test)]
    fn with_if_in_errors(mut self, data: Vec<u32>) -> Self {
        self.if_in_errors = Some(data);
        self
    }

    #[cfg(test)]
    fn with_if_out_errors(mut self, data: Vec<u32>) -> Self {
        self.if_out_errors = Some(data);
        self
    }

    #[cfg(test)]
    fn with_if_in_discards(mut self, data: Vec<u32>) -> Self {
        self.if_in_discards = Some(data);
        self
    }

    #[cfg(test)]
    fn with_if_out_discards(mut self, data: Vec<u32>) -> Self {
        self.if_out_discards = Some(data);
        self
    }

    #[cfg(test)]
    fn with_if_hc_in_ucast_pkts(mut self, data: Vec<u64>) -> Self {
        self.if_hc_in_ucast_pkts = Some(data);
        self
    }

    #[cfg(test)]
    fn with_if_hc_out_ucast_pkts(mut self, data: Vec<u64>) -> Self {
        self.if_hc_out_ucast_pkts = Some(data);
        self
    }

    #[cfg(test)]
    fn with_ts(mut self, ts: Vec<i64>) -> Self {
        self.ts = Some(ts);
        self
    }

    fn build(self) -> Result<Telemetry, TelemetryError> {
        let data64: Vec<&Vec<_>> = vec![
            self.if_hc_in_octets.as_ref(),
            self.if_hc_out_octets.as_ref(),
            self.if_hc_in_ucast_pkts.as_ref(),
            self.if_hc_out_ucast_pkts.as_ref(),
        ]
        .into_iter()
        .flatten()
        .collect();

        if !data64.is_empty() {
            let l = data64[0].len();
            if !data64.iter().all(|d| d.len() == l) {
                return Err(TelemetryError::InvalidInputError(Error::new(
                    ErrorKind::InvalidInput,
                    "All Counter64 data must have the same length",
                )));
            }
        }

        let data32: Vec<&Vec<_>> = vec![
            self.if_in_errors.as_ref(),
            self.if_out_errors.as_ref(),
            self.if_in_discards.as_ref(),
            self.if_out_discards.as_ref(),
        ]
        .into_iter()
        .flatten()
        .collect();

        if !data32.is_empty() {
            let l = data32[0].len();
            if !data32.iter().all(|d| d.len() == l) {
                return Err(TelemetryError::InvalidInputError(Error::new(
                    ErrorKind::InvalidInput,
                    "All Counter32 data must have the same length",
                )));
            }
        }

        let metrics = vec![
            (
                ObjectIdentifier::from_str(concat!(ifHCInOctets!(), ifIndex!()))
                    .map_err(TelemetryError::from),
                Metric::new(
                    MetricName::IfHCInOctets.as_str(),
                    MetricType::Counter64(
                        self.if_hc_in_octets
                            .unwrap_or_else(|| Vec::with_capacity(NUM_POINTS)),
                    ),
                ),
            ),
            (
                ObjectIdentifier::from_str(concat!(ifHCOutOctets!(), ifIndex!()))
                    .map_err(TelemetryError::from),
                Metric::new(
                    MetricName::IfHCOutOctets.as_str(),
                    MetricType::Counter64(
                        self.if_hc_out_octets
                            .unwrap_or_else(|| Vec::with_capacity(NUM_POINTS)),
                    ),
                ),
            ),
            (
                ObjectIdentifier::from_str(concat!(ifInErrors!(), ifIndex!()))
                    .map_err(TelemetryError::from),
                Metric::new(
                    MetricName::IfInErrors.as_str(),
                    MetricType::Counter32(
                        self.if_in_errors
                            .unwrap_or_else(|| Vec::with_capacity(NUM_POINTS)),
                    ),
                ),
            ),
            (
                ObjectIdentifier::from_str(concat!(ifOutErrors!(), ifIndex!()))
                    .map_err(TelemetryError::from),
                Metric::new(
                    MetricName::IfOutErrors.as_str(),
                    MetricType::Counter32(
                        self.if_out_errors
                            .unwrap_or_else(|| Vec::with_capacity(NUM_POINTS)),
                    ),
                ),
            ),
            (
                ObjectIdentifier::from_str(concat!(ifInDiscards!(), ifIndex!()))
                    .map_err(TelemetryError::from),
                Metric::new(
                    MetricName::IfInDiscards.as_str(),
                    MetricType::Counter32(
                        self.if_in_discards
                            .unwrap_or_else(|| Vec::with_capacity(NUM_POINTS)),
                    ),
                ),
            ),
            (
                ObjectIdentifier::from_str(concat!(ifOutDiscards!(), ifIndex!()))
                    .map_err(TelemetryError::from),
                Metric::new(
                    MetricName::IfOutDiscards.as_str(),
                    MetricType::Counter32(
                        self.if_out_discards
                            .unwrap_or_else(|| Vec::with_capacity(NUM_POINTS)),
                    ),
                ),
            ),
            (
                ObjectIdentifier::from_str(concat!(ifHCInUcastPkts!(), ifIndex!()))
                    .map_err(TelemetryError::from),
                Metric::new(
                    MetricName::IfHCInUcastPkts.as_str(),
                    MetricType::Counter64(
                        self.if_hc_in_ucast_pkts
                            .unwrap_or_else(|| Vec::with_capacity(NUM_POINTS)),
                    ),
                ),
            ),
            (
                ObjectIdentifier::from_str(concat!(ifHCOutUcastPkts!(), ifIndex!()))
                    .map_err(TelemetryError::from),
                Metric::new(
                    MetricName::IfHCOutUcastPkts.as_str(),
                    MetricType::Counter64(
                        self.if_hc_out_ucast_pkts
                            .unwrap_or_else(|| Vec::with_capacity(NUM_POINTS)),
                    ),
                ),
            ),
        ];

        let metrics: Vec<(ObjectIdentifier, Metric)> = metrics
            .into_iter()
            .map(|(result, metric)| result.map(|oid| (oid, metric)))
            .collect::<Result<Vec<_>, TelemetryError>>()?;

        Ok(Telemetry {
            metrics: HashMap::from_iter(metrics),
            ts: self.ts.unwrap_or_else(|| Vec::with_capacity(NUM_POINTS)),
        })
    }
}

impl Telemetry {
    fn builder() -> TelemetryBuilder {
        TelemetryBuilder::new()
    }

    fn process_record<'a, I>(&mut self, ts: i64, res: I)
    where
        I: IntoIterator<Item = (&'a ObjectIdentifier, &'a ObjectValue)>,
    {
        self.ts.push(ts);
        for (oid, v) in res {
            if let Some(metric) = self.metrics.get_mut(oid) {
                match (v, &mut metric.values) {
                    (ObjectValue::Counter32(x), MetricType::Counter32(v)) => v.push(*x),
                    (ObjectValue::Counter64(x), MetricType::Counter64(v)) => v.push(*x),
                    _ => error!("unexpected for oid {}: {:?}", oid, v),
                }
            } else {
                error!("unknown oid {}: {:?}", oid, v);
            }
        }
    }

    fn dataframe(&self) -> Result<DataFrame, TelemetryError> {
        let ts = Column::new("ts".into(), self.ts.as_slice());
        let mut data: Vec<Column> = self
            .metrics
            .values()
            .cloned()
            .map(|m| match m.values {
                MetricType::Counter32(x) => Column::new(m.name.into(), x),
                MetricType::Counter64(x) => Column::new(m.name.into(), x),
            })
            .collect();
        data.push(ts);

        let data_cols: Vec<String> = METRIC_NAMES.iter().map(|x| x.to_string()).collect();
        let mut df = DataFrame::new(data)?;

        df = df.lazy().with_column(col("ts").alias("tsDiff")).collect()?;

        info!("df = {:?}, {:?}", df.shape(), df.get_column_names());

        let mut diff_df = df
            .clone()
            .lazy()
            .with_columns(
                df.get_column_names()
                    .into_iter()
                    .filter_map(|c| {
                        let c_str = c.as_str();
                        match c_str {
                            "ts" => None,
                            _ => Some(col(c_str) - col(c_str).shift(lit(1))),
                        }
                    })
                    .collect::<Vec<_>>(),
            )
            .collect()?;
        info!(
            "diff_df after diff: {:?}, {:?}",
            diff_df.shape(),
            diff_df.get_column_names(),
        );
        diff_df = diff_df
            .lazy()
            .with_columns(
                data_cols
                    .iter()
                    .map(|c| {
                        (col(c.as_str()).cast(DataType::Float64)
                            / col("tsDiff".to_string()).cast(DataType::Float64))
                        .alias(&format!("{}_rate", c))
                    })
                    .collect::<Vec<_>>(),
            )
            .collect()?;
        //info!("diff_df after rate calculation: {}", diff_df);

        df = df
            .inner_join(&diff_df, ["ts"], ["ts"])?
            .drop_nulls(None::<&[String]>)?;

        df = df.select(&vec![
            "ts".to_string(),
            "ifHCInOctets_rate".to_string(),
            "ifHCOutOctets_rate".to_string(),
            "ifInErrors_rate".to_string(),
            "ifOutErrors_rate".to_string(),
            "ifInDiscards_rate".to_string(),
            "ifOutDiscards_rate".to_string(),
            "ifHCInUcastPkts_rate".to_string(),
            "ifHCOutUcastPkts_rate".to_string(),
            "ifHCInOctets".to_string(),
            "ifHCOutOctets".to_string(),
            "ifInErrors".to_string(),
            "ifOutErrors".to_string(),
            "ifInDiscards".to_string(),
            "ifOutDiscards".to_string(),
            "ifHCInUcastPkts".to_string(),
            "ifHCOutUcastPkts".to_string(),
        ])?;

        Ok(df)
    }
}

pub async fn snmp_telemetry(cfg: &SnmpConfig) -> Result<DataFrame, TelemetryError> {
    let mut tm = Telemetry::builder().build()?;
    let snmp_client = cfg.snmp_client().await?;
    info!("starting SNMP polling");
    let start = Instant::now();
    while start.elapsed() < LOAD_PERIOD + LOAD_PERIOD + SNMP_POLL_INTERVAL + SNMP_POLL_INTERVAL {
        let r = snmp_client.get_multiple(tm.metrics.keys().copied()).await;
        match r {
            Ok(results) => tm.process_record(chrono::Utc::now().timestamp_micros(), &results),
            Err(e) => error!("SNMP error: {}", e),
        }
        sleep(SNMP_POLL_INTERVAL).await;
    }
    tm.dataframe()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashSet, sync::Once};
    use tracing::info;
    use tracing_subscriber;

    static INIT: Once = Once::new();

    fn setup_tracing() {
        INIT.call_once(|| {
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::INFO)
                .init();
        });
    }

    #[test]
    fn test_differencing() -> Result<(), TelemetryError> {
        setup_tracing();
        let tm = Telemetry::builder()
            .with_if_hc_in_octets(vec![100u64, 200, 300])
            .with_if_hc_out_octets(vec![10u64, 20u64, 10])
            .with_if_in_errors(vec![2u32, 3, 4])
            .with_if_out_errors(vec![3u32, 5, 7])
            .with_if_in_discards(vec![5u32, 15, 30])
            .with_if_out_discards(vec![10u32, 15, 15])
            .with_if_hc_in_ucast_pkts(vec![0u64, 10_000, 10_000_000])
            .with_if_hc_out_ucast_pkts(vec![11, 111, 1111])
            .with_ts(vec![0i64, 1, 2])
            .build()?;
        let df = tm.dataframe()?;
        assert_eq!(df.width(), 17, "Expected 17 columns");
        assert_eq!(df.height(), 2, "Expected 2 columns");
        info!(
            "shape: {:?}, columns: {:?}",
            df.shape(),
            df.get_column_names()
        );
        info!("{}", df);
        let df_columns: HashSet<String> = df
            .get_column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let expected_results = vec![
            ("ifHCInOctets_rate", vec![Some(100.0), Some(100.0)]),
            (
                "ifHCOutOctets_rate",
                vec![Some(10.0), Some((u64::MAX - 10) as f64)],
            ),
            ("ifInErrors_rate", vec![Some(1.0), Some(1.0)]),
            ("ifOutErrors_rate", vec![Some(2.0), Some(2.0)]),
            ("ifInDiscards_rate", vec![Some(10.0), Some(15.0)]),
            ("ifOutDiscards_rate", vec![Some(5.0), Some(0.0)]),
            (
                "ifHCInUcastPkts_rate",
                vec![Some(10_000.0), Some(9_990_000.0)],
            ),
            ("ifHCOutUcastPkts_rate", vec![Some(100.0), Some(1000.0)]),
        ];
        for (col_name, expected_values) in expected_results {
            assert!(
                df_columns.contains(col_name),
                "Column {} should exist",
                col_name
            );

            let column = df.column(col_name)?;
            let expected = Column::new(col_name.into(), expected_values.clone());
            assert_eq!(
                column, &expected,
                "{} should match expected values: {:?}",
                col_name, expected_values
            );
        }

        Ok(())
    }
}

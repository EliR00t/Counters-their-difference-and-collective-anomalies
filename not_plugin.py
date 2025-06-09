import os
import logging
import numpy as np
import polars as pl
from influxdb_client_3 import InfluxDBClient3
import rpy2.robjects as ro
from rpy2.robjects.packages import importr
from rpy2.robjects.vectors import FloatVector, StrVector
from rpy2.robjects import globalenv
from rpy2.robjects.conversion import get_conversion

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logging.debug("Plugin execution started")
logging.info(f"Python executable: {sys.executable}")
logging.info(f"sys.path: {sys.path}")

# Set R environment
os.environ["R_HOME"] = "/usr/lib/R"
os.environ["R_LD_LIBRARY_PATH"] = os.environ.get("LD_LIBRARY_PATH", "")

def calculate_mahalanobis(data):
    if data.shape[0] < 50 or data.shape[1] == 0:
        logging.info(f"Insufficient data: {data.shape[0]} records found, minimum required is 50")
        return None
    data = data[~np.any(np.isnan(data) | np.isinf(data), axis=1)]
    if data.shape[0] < 50:
        logging.info(f"Insufficient valid data after cleaning: {data.shape[0]} records")
        return None
    try:
        cov = np.cov(data, rowvar=False)
        if np.any(np.isnan(cov) | np.isinf(cov)):
            logging.error("Covariance matrix contains NaN or inf values")
            return None
        eigenvalues = np.linalg.eigvals(cov)
        if np.any(eigenvalues <= 0):
            logging.error("Non-positive eigenvalues in covariance matrix")
            return None
        inv_cov = np.linalg.inv(cov)
        mean = np.mean(data, axis=0)
        diff = data - mean
        mahalanobis = np.sqrt(np.sum((diff @ inv_cov) * diff, axis=1))
        return mahalanobis
    except np.linalg.LinAlgError as e:
        logging.error(f"Mahalanobis calculation failed: {e}")
        return None

def process_scheduled_call(influxdb3_client = None, call_time=None, args=None):
    try:
        if influxdb3_client is None:
            influxdb3_client = InfluxDBClient3(
                host="http://127.0.0.1:8086",
                token=os.environ["INFLUXDB3_AUTH_TOKEN"],
                database="stats_db"
            )
        query = """
        SELECT time, "ifHCInOctets_rate", "ifHCOutOctets_rate",
        "ifInErrors_rate", "ifOutErrors_rate", "ifInDiscards_rate", "ifOutDiscards_rate",
        "ifHCInUcastPkts_rate", "ifHCOutUcastPkts_rate"
        FROM "snmp_stats"
        WHERE time >= now() - interval '5 minutes'
        """
        table = influxdb3_client.query(query=query, language="sql")
        df = pl.from_arrow(table)  # Convert pyarrow.Table to Polars DataFrame
        logging.info(f"Queried {len(df)} records from snmp_stats")
        if len(df) < 50:
            logging.info("Insufficient data for anomaly detection")
            return
        rate_columns = [
            "ifHCInOctets_rate", "ifHCOutOctets_rate", "ifInErrors_rate", "ifOutErrors_rate",
            "ifInDiscards_rate", "ifOutDiscards_rate", "ifHCInUcastPkts_rate", "ifHCOutUcastPkts_rate"
        ]
        data = df[rate_columns].to_numpy()
        mahalanobis = calculate_mahalanobis(data)
        if mahalanobis is None:
            logging.info("Mahalanobis calculation failed, skipping anomaly detection")
            return
        df = df.with_columns(pl.Series("mahalanobis", mahalanobis))
        with get_conversion().context():
            r = ro.r
            importr("anomaly", on_conflict="warn")
            importr("xts", on_conflict="warn")
            importr("bit64", on_conflict="warn")
            # Convert timestamps to microseconds since epoch
            ts = df["time"].dt.timestamp("us").to_numpy()
            r_ts_str = StrVector([str(x) for x in ts])
            globalenv['r_ts_str'] = r_ts_str
            r_mahalanobis = FloatVector(df["mahalanobis"].to_numpy())
            globalenv['r_mahalanobis'] = r_mahalanobis
            # Create R data frame
            r('r_ts <- bit64::as.integer64(as.character(r_ts_str))')
            globalenv['r_ts'] = r['r_ts']
            r('r_df <- data.frame(ts = r_ts, mahalanobis = r_mahalanobis)')
            globalenv['r_df'] = r['r_df']
            # Create xts object
            r('xts_dt <- xts::xts(r_df[, -1], order.by = as.POSIXct(as.numeric(r_df[, 1]) / 1e6, origin = "1970-01-01", tz = "UTC"))')
            globalenv['xts_dt'] = r['xts_dt']
            # Run CAPA
            r_code = '''
                detect_anomalies <- function(xts_data, min_seg_len = 30, max_seg_len = 300, type = "meanvar") {
                    res <- anomaly::capa(xts_data, min_seg_len = min_seg_len, max_seg_len = max_seg_len, type = type)
                    ret <- list(collective = anomaly::collective_anomalies(res), point = anomaly::point_anomalies(res))
                    return(ret)
                }
            '''
            r(r_code)
            result = r['detect_anomalies'](globalenv['xts_dt'])
            collective = np.array(result.rx2("collective"))
            point = np.array(result.rx2("point"))
            logging.info(f"Detected {len(collective)} collective anomalies, {len(point)} point anomalies")
    except Exception as e:
        logging.error(f"Error in process_scheduled_call: {e}")

if __name__ == "__main__":
    # Test with dummy data
    import polars as pl
    import numpy as np
    np.random.seed(42)
    times = pl.select(pl.datetime_range(
        start=pl.datetime(2025, 6, 8),
        end=pl.datetime(2025, 6, 8, 0, 5),
        interval="1s"
    ).alias("time")).to_series()
    n = len(times)
    df = pl.DataFrame({
        "time": times,
        "ifHCInOctets_rate": np.random.normal(1000, 100, n),
        "ifHCOutOctets_rate": np.random.normal(800, 80, n),
        "ifInErrors_rate": np.random.normal(10, 2, n),
        "ifOutErrors_rate": np.random.normal(8, 1.5, n),
        "ifInDiscards_rate": np.random.normal(5, 1, n),
        "ifOutDiscards_rate": np.random.normal(4, 0.8, n),
        "ifHCInUcastPkts_rate": np.random.normal(500, 50, n),
        "ifHCOutUcastPkts_rate": np.random.normal(450, 45, n)
    })
    df = df.with_columns([
        pl.col(col).map_elements(lambda x: x * 10 if np.random.rand() < 0.01 else x, return_dtype=pl.Float64)
        for col in df.columns if col.endswith("_rate")
    ])
    logging.info(f"Testing with {len(df)} dummy records")
    rate_columns = [
        "ifHCInOctets_rate", "ifHCOutOctets_rate", "ifInErrors_rate", "ifOutErrors_rate",
        "ifInDiscards_rate", "ifOutDiscards_rate", "ifHCInUcastPkts_rate", "ifHCOutUcastPkts_rate"
    ]
    data = df[rate_columns].to_numpy()
    mahalanobis = calculate_mahalanobis(data)
    if mahalanobis is not None:
        df = df.with_columns(pl.Series("mahalanobis", mahalanobis))
        with get_conversion().context():
            r = ro.r
            importr("anomaly", on_conflict="warn")
            importr("xts", on_conflict="warn")
            importr("bit64", on_conflict="warn")
            # Convert timestamps to microseconds since epoch
            ts = df["time"].dt.timestamp("us").to_numpy()
            r_ts_str = StrVector([str(x) for x in ts])
            globalenv['r_ts_str'] = r_ts_str
            r_mahalanobis = FloatVector(df["mahalanobis"].to_numpy())
            globalenv['r_mahalanobis'] = r_mahalanobis
            # Create R data frame
            r('r_ts <- bit64::as.integer64(as.character(r_ts_str))')
            globalenv['r_ts'] = r['r_ts']
            r('r_df <- data.frame(ts = r_ts, mahalanobis = r_mahalanobis)')
            globalenv['r_df'] = r['r_df']
            # Create xts object
            r('xts_dt <- xts::xts(r_df[, -1], order.by = as.POSIXct(as.numeric(r_df[, 1]) / 1e6, origin = "1970-01-01", tz = "UTC"))')
            globalenv['xts_dt'] = r['xts_dt']
            # Run CAPA
            r_code = '''
                detect_anomalies <- function(xts_data, min_seg_len = 30, max_seg_len = 300, type = "meanvar") {
                    res <- anomaly::capa(xts_data, min_seg_len = min_seg_len, max_seg_len = max_seg_len, type = type)
                    ret <- list(collective = anomaly::collective_anomalies(res), point = anomaly::point_anomalies(res))
                    return(ret)
                }
            '''
            r(r_code)
            result = r['detect_anomalies'](globalenv['xts_dt'])
            collective = np.array(result.rx2("collective"))
            point = np.array(result.rx2("point"))
            logging.info(f"Test detected {len(collective)} collective anomalies, {len(point)} point anomalies")
    process_scheduled_call()

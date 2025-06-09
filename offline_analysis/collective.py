import os
import numpy as np
import matplotlib.pyplot as plt
import polars as pl
from rpy2.robjects import r, globalenv, FloatVector, IntVector, conversion
from rpy2.robjects.packages import importr
from rpy2.robjects.vectors import StrVector, DataFrame as RDataFrame
from pathlib import Path

os.environ['R_HOME'] = '/Library/Frameworks/R.framework/Resources'
xts = importr('xts', on_conflict="warn")
anomaly = importr('anomaly', on_conflict="warn")
dplyr = importr('dplyr', on_conflict="warn")
bit64 = importr('bit64')
#converter = conversion.get_conversion()

def calculate_mahalanobis(df):
    data = df.select(pl.col(pl.Float64))
    print(f"{data.shape} {data.columns}")
    data = data.to_numpy()
    # Compute mean and covariance
    mean = np.mean(data, axis=0)
    cov = np.cov(data, rowvar=False)
    print(f"covariance matrix shape = {cov.shape}")

    # Check eigenvalues
    eigenvalues = np.linalg.eigvals(cov)
    print(f"eigenvalues = {eigenvalues}")

    # Regularize covariance matrix
    if np.any(eigenvalues < 1e-10):
        print("Warning: Small eigenvalues detected,"
              "regularizing covariance matrix")
        cov += np.eye(cov.shape[0]) * 1e-6

    # Compute Mahalanobis distances
    inv_cov = np.linalg.inv(cov)
    center = data - mean
    mahalanobis = np.sqrt(np.sum((center @ inv_cov) * center, axis=1))
    #print(f"mahalanobis distances = {mahalanobis}")
    print(f"mahalanobis shape = {mahalanobis.shape}")
    df = df.with_columns(pl.Series("mahalanobis", mahalanobis))
    return df


with conversion.get_conversion().context():
    def detect_anomalies(df):
        ts = df["ts"].to_numpy()
        mahalanobis = df["mahalanobis"].to_numpy()

        r_ts_str = StrVector([str(x) for x in ts])
        globalenv['r_ts_str'] = r_ts_str
        r_mahalanobis = FloatVector(mahalanobis)
        r('r_ts <- bit64::as.integer64(as.character(r_ts_str))')
        globalenv['r_ts'] = r['r_ts']
        globalenv['r_mahalanobis'] = r_mahalanobis
        r('r_df <- data.frame(ts = r_ts, mahalanobis = r_mahalanobis)')
        globalenv['r_df'] = r['r_df']
        r('xts_dt <- xts::xts(r_df[, -1], order.by = as.POSIXct(as.numeric(r_df[, 1]) / 1e6, origin = "1970-01-01", tz = "UTC"))')
        globalenv['xts_dt'] = r['xts_dt']
        r_code = '''
            detect_anomalies <- function(xts_data, min_seg_len = 30, max_seg_len = 300, type = "meanvar") {
                res <- anomaly::capa(xts_data, min_seg_len = min_seg_len, max_seg_len = max_seg_len, type = type)
                summary(res)
                # Save plot to a temporary PNG file
                png(filename = "capa_plot.png", width = 800, height = 600)
                plot(res)
                dev.off()
                ret <- list(collective = anomaly::collective_anomalies(res), point = anomaly::point_anomalies(res))
                return(ret)
            }
        '''
        r(r_code)
        result = r['detect_anomalies'](globalenv['xts_dt'])
        r_df = RDataFrame(result)
        converter = conversion.get_conversion()
        collective_df = RDataFrame(r_df.rx2('collective'))
        collective_columns = ['start', 'end', 'variate', 'start.lag', 'end.lag',
                          'mean.change', 'variance.change']
        collective_data = {
            col: pl.Series(col, np.array(list(converter.rpy2py(collective_df.rx2(col)))))  # Convert to NumPy array via list
            for col in collective_columns if col in collective_df.names
        }
        pl_collective = pl.DataFrame(collective_data)
        pl_collective = pl_collective.with_columns([
            pl.col("start").cast(pl.UInt32),
            pl.col("end").cast(pl.UInt32),
            pl.col("start.lag").cast(pl.UInt32),
            pl.col("end.lag").cast(pl.UInt32)
        ])
        point_df = RDataFrame(r_df.rx2('point'))
        point_columns = ['location', 'variate', 'strength']
        # Convert to NumPy array via list
        point_data = {
            col: pl.Series(col, np.array(list(converter.rpy2py(point_df.rx2(col)))))
            for col in point_columns if col in point_df.names
        }
        pl_point = pl.DataFrame(point_data)
        pl_point = pl_point.with_columns([
            pl.col("location").cast(pl.UInt32)
        ])
        # Display the saved plot
        plot_path = Path("capa_plot.png")
        if plot_path.exists():
            img = plt.imread(plot_path)
            plt.figure(figsize=(10, 7))
            plt.imshow(img)
            plt.axis('off')  # Hide axes
            plt.title("CAPA Plot from R")
            plt.show()

        return {'collective': pl_collective, 'point': pl_point}


if __name__ == '__main__':
    directory = "data"
    fn_regular = os.path.join(directory,"rates_regular.csv")
    fn5, fn25 = os.path.join(directory,"rates_anomaly.csv"), os.path.join(directory,"rates_anomaly_25.csv")
    cols = ["ts", "ifHCInOctets_rate", "ifHCOutOctets_rate",
            "ifInErrors_rate", "ifOutErrors_rate", "ifInDiscards_rate",
            "ifOutDiscards_rate", "ifHCInUcastPkts_rate",
            "ifHCOutUcastPkts_rate"]
    data_cols = [c for c in cols if c != "ts"]

    df_regular = pl.read_csv(fn_regular, columns=cols)
    stats_regular = df_regular.select([
           pl.col(col).min().alias(f"{col}_min") for col in data_cols
    ] + [
           pl.col(col).max().alias(f"{col}_max") for col in data_cols
    ] + [
           pl.col(col).std().pow(2).alias(f"{col}_var") for col in data_cols
    ] + [
        pl.col(col).mean().alias(f"{col}_mean") for col in data_cols
    ]).to_dicts()[0]
#   print("Regular HTTP load")
#   for k, v in stats_regular.items():
#       print(f"{k} => {v}")

    df5 = pl.read_csv(fn5, columns=cols)
    stats5 = df5.select([
           pl.col(col).min().alias(f"{col}_min") for col in data_cols
    ] + [
           pl.col(col).max().alias(f"{col}_max") for col in data_cols
    ] + [
           pl.col(col).std().pow(2).alias(f"{col}_var") for col in data_cols
    ] + [
        pl.col(col).mean().alias(f"{col}_mean") for col in data_cols
    ]).to_dicts()[0]
#   print("\n5% packet loss HTTP load")
#   for k, v in stats5.items():
#       print(f"{k} => {v}")

    df25 = pl.read_csv(fn25, columns=cols)
    stats25 = df25.select([
           pl.col(col).min().alias(f"{col}_min") for col in data_cols
    ] + [
           pl.col(col).max().alias(f"{col}_max") for col in data_cols
    ] + [
           pl.col(col).std().pow(2).alias(f"{col}_var") for col in data_cols
    ] + [
        pl.col(col).mean().alias(f"{col}_mean") for col in data_cols
    ]).to_dicts()[0]
#   print("\n25% packet loss HTTP load")
#   for k, v in stats25.items():
#       print(f"{k} => {v}")

    total_df = pl.concat([df_regular, df5, df25]).sort("ts")

    stats = total_df.select([
           pl.col(col).min().alias(f"{col}_min") for col in data_cols
    ] + [
           pl.col(col).max().alias(f"{col}_max") for col in data_cols
    ] + [
           pl.col(col).std().pow(2).alias(f"{col}_var") for col in data_cols
    ] + [
        pl.col(col).mean().alias(f"{col}_mean") for col in data_cols
    ]).to_dicts()[0]
#    print("\nTotal data")
#    for k, v in stats.items():
#        print(f"{k} => {v}")

    variances = total_df.select([pl.col(col).std().pow(2)
                                 for col in data_cols]).to_numpy().flatten()
    print(f"variances = {variances}")
    valid_cols = [col for col, var in zip(data_cols, variances)
                  if var > np.finfo(np.float64).eps]
    print(f"valid columns = {valid_cols}")
    data_df = total_df.select(["ts"] + valid_cols)
    print(f"{data_df.shape}, {data_df.columns}")

    # Calculate max/min ratios
    ratios = {col: stats[f"{col}_max"] / stats[f"{col}_min"]
              if stats[f"{col}_min"] != 0 else float('inf')
              for col in valid_cols}
    for k, v in ratios.items():
        print(f"{k} max/min ratio => {v}")
    log_scale_cols = [col for col in valid_cols if ratios[col] > 1e3]
    print(f"log scale columns (max/min > 1e3) = {log_scale_cols}")

    spearman_corr_matrix = data_df.select(
          [pl.corr(col1, col2,
           method="spearman").alias(f"spearman_{col1}_vs_{col2}") for i, col1
           in enumerate(valid_cols) for j, col2
           in enumerate(valid_cols) if i < j]).to_dict(as_series=False)
    for k, v in spearman_corr_matrix.items():
        print(f"{k} => {v[0]}")

    # Visualize histograms
    for col in valid_cols:
        plt.figure(figsize=(8, 5))
        plt.hist(data_df[col].to_numpy(), bins=30, alpha=0.5,
                 label="Original")
        plt.hist(np.log1p(data_df[col].to_numpy()), bins=30, alpha=0.5,
                 label="Log")
        plt.title(f"{col} Distribution")
        plt.legend()
        plt.show()

    # Apply scaling
    data_scaled = data_df.with_columns(
            [pl.col(col).log1p().alias(f"{col}_scaled")
             if col in log_scale_cols else pl.col(col) for col in valid_cols])
    data_scaled = data_scaled.drop(log_scale_cols)
    data_scaled = data_scaled.rename({
        f"{col}_scaled": col for col in log_scale_cols})
    data_scaled = data_scaled.with_columns([
        (pl.col(col) - pl.col(col).min()) /
        (pl.col(col).max() - pl.col(col).min()).fill_null(1).alias(col)
        for col in valid_cols])
    data_scaled = data_scaled.rename({
        col: col.replace("_rate", "") for col in valid_cols})
    print(f"{data_scaled.head()}")

    data = calculate_mahalanobis(data_scaled)
    data.write_csv("mahalanobis_3.csv")
    print(f"{data.head()}")
    a = detect_anomalies(data)
    for k, v in a.items():
        print(f"{k} => {v}")

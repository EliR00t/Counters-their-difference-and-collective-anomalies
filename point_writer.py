import os
import time
from collections import deque
import csv
import numpy as np
from influxdb_client_3 import (
  InfluxDBClient3, InfluxDBError, Point, WritePrecision,
  WriteOptions, write_client_options)

WRITE_PRECISION = 'us'
HOST = "http://betelgeuse:8086/"
HOSTNAME = "betelgeuse"
TOKEN = os.getenv('INFLUXDB3_AUTH_TOKEN')
DATABASE = "stats_db"
TABLE = "snmp_stats"
IFNAME = "wlp113s0"

def success(self, data: str):
    print(f"Successfully wrote batch: data: {data}")

def error(self, data: str, exception: InfluxDBError):
    print(f"Failed writing batch: config: {self}, data: {data} due: {exception}")

def retry(self, data: str, exception: InfluxDBError):
    print(f"Failed retry writing batch: config: {self}, data: {data} retry: {exception}")


def process_file(fname, client):
    point = {
            "measurement": TABLE,
            "tags": {"ifName": IFNAME, "hostname": HOSTNAME},
            "fields": {"ifHCInOctets_rate": 0.0,
                       "ifHCOutOctets_rate": 0.0,
                       "ifInErrors_rate": 0.0,
                       "ifOutErrors_rate": 0.0,
                       "ifInDiscards_rate": 0.0,
                       "ifOutDiscards_rate": 0.0,
                       "ifHCInUcastPkts_rate": 0.0,
                       "ifHCOutUcastPkts_rate": 0.0,
                       "ifHCInOctets": 0,
                       "ifHCOutOctets": 0,
                       "ifInErrors": 0,
                       "ifOutErrors": 0,
                       "ifInDiscards": 0,
                       "ifOutDiscards": 0,
                       "ifHCInUcastPkts": 0,
                       "ifHCOutUcastPkts": 0,
                       },
            "time": 0,
            }

    with open(fname) as f:
        rd = csv.DictReader(f)
        for row in rd:
            line = (
                f"{TABLE},hostname={HOSTNAME},ifName={IFNAME} "
                f"ifHCInOctets={int(row['ifHCInOctets'])}u,"
                f"ifHCInOctets_rate={float(row['ifHCInOctets_rate'])},"
                f"ifHCOutOctets={int(row['ifHCOutOctets'])}u,"
                f"ifHCOutOctets_rate={float(row['ifHCOutOctets_rate'])},"
                f"ifInErrors={int(row['ifInErrors'])}u,"
                f"ifInErrors_rate={float(row['ifInErrors_rate'])},"
                f"ifOutErrors={int(row['ifOutErrors'])}u,"
                f"ifOutErrors_rate={float(row['ifOutErrors_rate'])},"
                f"ifInDiscards={int(row['ifInDiscards'])}u,"
                f"ifInDiscards_rate={float(row['ifInDiscards_rate'])},"
                f"ifOutDiscards={int(row['ifOutDiscards'])}u,"
                f"ifOutDiscards_rate={float(row['ifOutDiscards_rate'])},"
                f"ifHCInUcastPkts={int(row['ifHCInUcastPkts'])}u,"
                f"ifHCInUcastPkts_rate={float(row['ifHCInUcastPkts_rate'])},"
                f"ifHCOutUcastPkts={int(row['ifHCOutUcastPkts'])}u,"
                f"ifHCOutUcastPkts_rate={float(row['ifHCOutUcastPkts_rate'])} "
                f"{int(row['ts'])}"
            )
            client.write(record=line, write_precision=WRITE_PRECISION)
            time.sleep(2)


if __name__ == '__main__':
    write_options = WriteOptions(batch_size=1, flush_interval=5,
                                 jitter_interval=20, retry_interval=50,
                                 max_retries=5, max_retry_delay=30_000,
                                 exponential_base=2)
    wco = write_client_options(success_callback=success, error_callback=error,
                               retry_callback=retry,
                               write_options=write_options,
                               enable_gzip=False, debug=True)
    fnames = ['rates_regular.csv', 'rates_anomaly.csv', 'rates_anomaly_25.csv']
    with InfluxDBClient3(host=HOST, token=TOKEN, database=DATABASE,
                         write_client_options=wco) as client:
        for fn in fnames:
            process_file(fn, client)
            time.sleep(5)

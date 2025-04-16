import time
import os
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

# CONFIGURATION
INFLUXDB_URL = "http://YOUR_IP_HERE:8086"
INFLUXDB_TOKEN = "YOUR_INFLUXDB_TOKEN"
INFLUXDB_ORG = "YOUIR_ORG_HERE"
INFLUXDB_BUCKET = "YOUR_BUCKET_HERE"
LOG_DIRECTORY = "/var/log/chrony"

# Connect to InfluxDB
client = InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG
)
write_api = client.write_api(write_options=SYNCHRONOUS)

# File states for tracking file positions
file_positions = {}

def tail_file(file_path):
    with open(file_path, "r") as f:
        f.seek(file_positions.get(file_path, 0))
        lines = f.readlines()
        file_positions[file_path] = f.tell()
    return lines

def parse_and_send_tracking(line):
    parts = line.strip().split()
    if len(parts) >= 13:
        point = Point("chrony_tracking") \
            .tag("ip", parts[2]) \
            .field("freq_ppm", float(parts[4])) \
            .field("skew_ppm", float(parts[5])) \
            .field("offset", float(parts[6])) \
            .field("offset_sd", float(parts[7])) \
            .field("root_delay", float(parts[9])) \
            .field("root_disp", float(parts[10])) \
            .field("max_error", float(parts[11])) \
            .time(f"{parts[0]}T{parts[1]}Z")
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)

def parse_and_send_statistics(line):
    parts = line.strip().split()
    if len(parts) >= 13:
        point = Point("chrony_statistics") \
            .tag("ip", parts[2]) \
            .field("std_dev", float(parts[3])) \
            .field("est_offset", float(parts[4])) \
            .field("offset_sd", float(parts[5])) \
            .field("diff_freq", float(parts[6])) \
            .field("est_skew", float(parts[7])) \
            .field("stress", float(parts[8])) \
            .time(f"{parts[0]}T{parts[1]}Z")
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)

def parse_and_send_measurement(line):
    parts = line.strip().split()
    if len(parts) >= 15:
        point = Point("chrony_measurement") \
            .tag("ip", parts[2]) \
            .field("score", float(parts[9])) \
            .field("offset", float(parts[10])) \
            .field("peer_delay", float(parts[11])) \
            .field("peer_disp", float(parts[12])) \
            .field("root_delay", float(parts[13])) \
            .field("root_disp", float(parts[14])) \
            .time(f"{parts[0]}T{parts[1]}Z")
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)

# Main loop
def main():
    log_files = {
        "tracking.log": parse_and_send_tracking,
        "statistics.log": parse_and_send_statistics,
        "measurement.log": parse_and_send_measurement
    }

    # Initialize file positions
    for log_file in log_files:
        full_path = os.path.join(LOG_DIRECTORY, log_file)
        if os.path.exists(full_path):
            with open(full_path, "r") as f:
                f.seek(0, os.SEEK_END)
                file_positions[full_path] = f.tell()

    while True:
        for log_file, parser in log_files.items():
            full_path = os.path.join(LOG_DIRECTORY, log_file)
            if os.path.exists(full_path):
                lines = tail_file(full_path)
                for line in lines:
                    try:
                        parser(line)
                    except Exception as e:
                        print(f"Error parsing line in {log_file}: {e}")
        time.sleep(5)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3

import os
import time
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
INFLUXDB_URL = "http://0.0.0.0:8086" # Adjust if needed
INFLUXDB_TOKEN = "YOUR_TOKEN_HERE" # Replace with your real token
INFLUXDB_ORG = "ORG" # Your Influx DB Org
INFLUXDB_BUCKET = "BUCKET" # Your InfluxDB Bucket

LOG_DIRECTORY = "/var/log/chrony"

# If your logs are named differently, change these:
TRACKING_LOG = "tracking.log"
STATISTICS_LOG = "statistics.log"
MEASUREMENT_LOG = "measurement.log"

# --------------------------------------------------
# SETUP
# --------------------------------------------------
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Keep track of file positions so we only read new lines
file_positions = {}

def safe_float(val: str):
    """Convert val to float or None if invalid or 'N'."""
    if val.upper() == 'N':
        return None
    try:
        return float(val)
    except ValueError:
        return None

def tail_file(file_path: str):
    """Return new lines from the file since last read."""
    with open(file_path, "r") as f:
        f.seek(file_positions.get(file_path, 0))
        lines = f.readlines()
        file_positions[file_path] = f.tell()
    return lines

# --------------------------------------------------
# PARSE & SEND: TRACKING
# --------------------------------------------------
def parse_and_send_tracking(line: str):
    """
    Format example:
    Date(UTC) Time IP L St freq_ppm skew_ppm offset offset_sd ??? root_delay root_disp max_error ...
    Indices: 0 1 2 3 4 5 6 7 8 9 10 11
    """
    parts = line.strip().split()
    if len(parts) < 12:
        return # Skip invalid lines

    # Build the timestamp
    # e.g. 2023-06-14 07:24:02 => "2023-06-14T07:24:02Z"
    timestamp = f"{parts[0]}T{parts[1]}Z"

    # Extract fields
    ip = parts[2]
    freq_ppm = safe_float(parts[4])
    skew_ppm = safe_float(parts[5])
    offset = safe_float(parts[6])
    offset_sd = safe_float(parts[7])
    root_delay = safe_float(parts[9])
    root_disp = safe_float(parts[10])
    max_error = safe_float(parts[11])

    # Construct the point
    point = Point("chrony_tracking").time(timestamp)
    point = point.tag("ip", ip)

    # Fields (only add if not None)
    if freq_ppm is not None:
        point = point.field("freq_ppm", freq_ppm)
    if skew_ppm is not None:
        point = point.field("skew_ppm", skew_ppm)
    if offset is not None:
        point = point.field("offset", offset)
    if offset_sd is not None:
        point = point.field("offset_sd", offset_sd)
    if root_delay is not None:
        point = point.field("root_delay", root_delay)
    if root_disp is not None:
        point = point.field("root_disp", root_disp)
    if max_error is not None:
        point = point.field("max_error", max_error)

    # Send to Influx
    write_api.write(bucket=INFLUXDB_BUCKET, record=point)

# --------------------------------------------------
# PARSE & SEND: STATISTICS
# --------------------------------------------------
def parse_and_send_statistics(line: str):
    """
    Format example:
    Date(UTC) Time IP std_dev est_offset offset_sd diff_freq est_skew stress ...
    Indices: 0 1 2 3 4 5 6 7 8 ...
    """
    parts = line.strip().split()
    if len(parts) < 9:
        return

    timestamp = f"{parts[0]}T{parts[1]}Z"
    ip = parts[2]

    std_dev = safe_float(parts[3])
    est_offset = safe_float(parts[4])
    offset_sd = safe_float(parts[5])
    diff_freq = safe_float(parts[6])
    est_skew = safe_float(parts[7])
    stress = safe_float(parts[8])

    point = Point("chrony_statistics").time(timestamp)
    point = point.tag("ip", ip)
    if std_dev is not None:
        point = point.field("std_dev", std_dev)
    if est_offset is not None:
        point = point.field("est_offset", est_offset)
    if offset_sd is not None:
        point = point.field("offset_sd", offset_sd)
    if diff_freq is not None:
        point = point.field("diff_freq", diff_freq)
    if est_skew is not None:
        point = point.field("est_skew", est_skew)
    if stress is not None:
        point = point.field("stress", stress)

    write_api.write(bucket=INFLUXDB_BUCKET, record=point)

# --------------------------------------------------
# PARSE & SEND: MEASUREMENT
# --------------------------------------------------
def parse_and_send_measurement(line: str):
    """
    Format example:
    Date(UTC) Time IP L St ??? ??? ??? ??? ??? score offset peer_del peer_disp root_del root_disp ...
    Indices: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14
    """
    parts = line.strip().split()
    if len(parts) < 15:
        return

    timestamp = f"{parts[0]}T{parts[1]}Z"
    ip = parts[2]
    score = safe_float(parts[9])
    offset = safe_float(parts[10])
    peer_delay = safe_float(parts[11])
    peer_disp = safe_float(parts[12])
    root_delay = safe_float(parts[13])
    root_disp = safe_float(parts[14])

    point = Point("chrony_measurement").time(timestamp)
    point = point.tag("ip", ip)

    if score is not None:
        point = point.field("score", score)
    if offset is not None:
        point = point.field("offset", offset)
    if peer_delay is not None:
        point = point.field("peer_delay", peer_delay)
    if peer_disp is not None:
        point = point.field("peer_disp", peer_disp)
    if root_delay is not None:
        point = point.field("root_delay", root_delay)
    if root_disp is not None:
        point = point.field("root_disp", root_disp)

    write_api.write(bucket=INFLUXDB_BUCKET, record=point)

# --------------------------------------------------
# MAIN LOOP
# --------------------------------------------------
def main():
    # Map each file to its parsing function
    log_files = {
        TRACKING_LOG: parse_and_send_tracking,
        STATISTICS_LOG: parse_and_send_statistics,
        MEASUREMENT_LOG: parse_and_send_measurement
    }

    # Initialize file positions
    for lf_name in log_files:
        full_path = os.path.join(LOG_DIRECTORY, lf_name)
        if os.path.exists(full_path):
            with open(full_path, "r") as f:
                f.seek(0, os.SEEK_END)
                file_positions[full_path] = f.tell()

    while True:
        for lf_name, parser_func in log_files.items():
            full_path = os.path.join(LOG_DIRECTORY, lf_name)
            if os.path.exists(full_path):
                lines = tail_file(full_path)
                for line in lines:
                    try:
                        parser_func(line)
                    except Exception as e:
                        print(f"Error parsing line in {lf_name}: {e}")
        time.sleep(5)

if __name__ == "__main__":
    main()

import pandas as pd
import time
from datetime import datetime

CSV_PATH = "data/processed/logs_cleaned.csv"
OUTPUT_LOG_FILE = "data/logs/streamed_logs.log"

CHUNK_SIZE = 50000
DELAY_BETWEEN_LOGS = 0.1  # secondes


def format_log(row):
    """Format de log réaliste type serveur / SOC"""

    # Source (application)
    if "dvwa__src_ip" in row:
        source = "DVWA"
        prefix = "dvwa__"
    elif "boa__src_ip" in row:
        source = "BOA"
        prefix = "boa__"
    else:
        source = "UNKNOWN"
        prefix = ""

    # Timestamp
    timestamp = row.get(f"{prefix}timestamp", datetime.now())
    timestamp = pd.to_datetime(timestamp)

    # Network
    src = f"{row.get(f'{prefix}src_ip')}:{int(row.get(f'{prefix}src_port', 0))}"
    dst = f"{row.get(f'{prefix}dst_ip')}:{int(row.get(f'{prefix}dst_port', 0))}"
    proto = row.get(f"{prefix}protocol", "UNK")

    # Traffic
    duration = round(float(row.get(f"{prefix}duration", 0)), 3)
    packets = int(row.get(f"{prefix}packets_count", 0))
    bytes_ = int(row.get(f"{prefix}total_payload_bytes", 0))

    # System / container metrics
    cpu = round(float(row.get(f"{prefix}container_cpu_usage_seconds_rate", 0)), 2)
    mem = int(float(row.get(f"{prefix}container_memory_usage_bytes", 0)) / (1024 * 1024))
    net_rx = int(float(row.get(f"{prefix}container_network_receive_bytes_rate", 0)) / 1024)

    log_entry = (
        f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] [WARN] [{source}]\n"
        f"src={src} dst={dst} proto={proto}\n"
        f"duration={duration}s packets={packets} bytes={bytes_}\n"
        f"cpu={cpu} mem={mem}MB net_rx={net_rx}KB/s\n"
    )

    return log_entry


def stream_logs():
    with open(OUTPUT_LOG_FILE, "a") as logfile:
        for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE):
            chunk = chunk.fillna("")
            for _, row in chunk.iterrows():
                log = format_log(row)

                print(log)
                logfile.write(log + "\n")

                time.sleep(DELAY_BETWEEN_LOGS)


if __name__ == "__main__":
    stream_logs()

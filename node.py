"""
Sensor Node 

Caleb Binfet 1/22/2026

This module implements the sensor-node runtime for a distributed
diagnostics system deployed in remote environments. It is responsible for:

- Periodic sampling of local sensors and anomaly detection
- Write-ahead logging of telemetry to local non-volatile storage (NVM)
- Reliable replay of unacknowledged telemetry after outages
- Applying configuration updates pushed by the host
- Running on-demand diagnostics and returning results to the host

This file is intentionally kept thin and delegates logic to:
  - node.handlers (message dispatch)
  - node.nvm_buffer (persistent buffering)
  - common.protocol (wire framing)
"""
import socket
import time
import signal

from sensor import sensor
from telemetryHandling import build_telemetry
from configUpdate import apply_cfg
from nvm_buffer import NvmBuffer
from sensor import run_rail_diagnostic

from common.protocol import send_jsonl, recv_jsonl_nonblocking
from common.timeutil import utc_ts
from node_lib.handlers import NodeIds, RailSensors, handle_host_msgs
from common.constants import HOST_IP, HOST_PORT


running = True


def _sig_handler(signum, frame):
    global running
    print(f"[node] {utc_ts()} shutdown requested (signal {signum})", flush=True)
    running = False


signal.signal(signal.SIGINT, _sig_handler)
if hasattr(signal, "SIGTERM"):
    signal.signal(signal.SIGTERM, _sig_handler)


# -------------------- Node Config --------------------
cfg = {
    "cfg_ver": 0,
    "verbose_anomaly": False,
    "anomalyLimits": {
        "temp_v":    {"min": 2.0, "max": 2.25},
        "humidity":  {"min":  30, "max": 70.0},
        "vibration": {"min":  0.005, "max": 0.08},
    },
    "railLimits": {
        "rail_3v3": {"min": 3.20, "max": 3.40},
        "rail_5v":  {"min": 4.90, "max": 5.10},
    }
}

# -------------------- Sensors --------------------
temp = sensor("Temp_v", value=2.15, step_std=0.005, precision=3,
              min_val=0.0)  # Assuming kelvin so no negative
humidity = sensor("Humidity", value=45.2, step_std=0.3, precision=1,
                  min_val=0.0)  # Assuming Percentage so no negative
# Assuming vibration is magnitude and not signed
vibration = sensor("Vibration", value=0.02, step_std=0.01, min_val=0.0)

# rails treated similarly to sensors and assumed to have ADC attached in practice
rail_3v3 = sensor("rail_3v3", value=3.30, step_std=0.003, precision=3)
rail_5v = sensor("rail_5v",  value=5.00, step_std=0.004, precision=3)

node_id = "N1"
boot_id = "B17"

ids = NodeIds(node_id=node_id, boot_id=boot_id)
rails = RailSensors(rail_3v3=rail_3v3, rail_5v=rail_5v)

# -------------------- Local "NVM" buffer --------------------
buf = NvmBuffer(
    log_path=f"{node_id}_{boot_id}.jsonl",
    state_path=f"{node_id}_{boot_id}.state",
)
seq = buf.load()

# -------------------- Connection state --------------------
sock = None
rx_buffer = b""
reconnect_at = 0.0
reconnect_delay_s = 1.0


def connect():
    global sock, rx_buffer
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1.0)
    s.connect((HOST_IP, HOST_PORT))
    s.settimeout(None)
    sock = s
    rx_buffer = b""
    buf.reset_replay()  # replay all unacked after reconnect
    print("[node] connected", flush=True)


def disconnect():
    global sock
    try:
        if sock:
            sock.close()
    finally:
        sock = None


# -------------------- Main loop --------------------
period_s = 0.1  # 10 Hz
next_t = time.monotonic()

while running:
    next_t += period_s

    # Get Telemetry data from sensor
    sensorData = {
        "temp_v": temp.read(),
        "humidity": humidity.read(),
        "vibration": vibration.read(),
    }
    # Create telemetry message
    msg = build_telemetry(
        node_id=node_id,
        boot_id=boot_id,
        seq=seq,
        values=sensorData,
        limits=cfg["anomalyLimits"],
        cfg_ver=cfg["cfg_ver"],
        verbose_anomaly=cfg["verbose_anomaly"],
    )

    # Store in non-volatile mem
    buf.append(msg)
    seq += 1

    # connent and reconnect logic
    now = time.time()
    if sock is None and now >= reconnect_at:
        try:
            connect()
        except OSError:
            reconnect_at = now + reconnect_delay_s
            sock = None

    # --- Handles sending replayed data
    if sock is not None:
        try:
            # ---- SEND: replay/catch-up batch ----
            for m in buf.iter_unacked_batch(max_msgs=200):
                send_jsonl(sock, m)

            # ---- RECV: ack/cfg/diag ----
            incoming, rx_buffer = recv_jsonl_nonblocking(sock, rx_buffer)
            handle_host_msgs(
                incoming,
                ids=ids,
                cfg=cfg,
                buf=buf,
                sock=sock,
                rails=rails,
                apply_cfg=apply_cfg,
                run_rail_diagnostic=run_rail_diagnostic,
            )

        except (OSError, ConnectionResetError) as e:
            print(
                f"[node] {utc_ts()} LINK DOWN (will reconnect) err={type(e).__name__}", flush=True)
            disconnect()
            reconnect_at = time.time() + reconnect_delay_s

    # Timing for node
    sleep_s = next_t - time.monotonic()
    if sleep_s > 0:
        time.sleep(sleep_s)
    else:
        next_t = time.monotonic()

# Handle user input from terminal if Ctrl+C
print(f"[node] {utc_ts()} exiting", flush=True)
disconnect()

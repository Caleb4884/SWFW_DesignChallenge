"""
Host Controller 
Caleb Binfet 1/22/2026

Note: I am still learning python so i did use chat gpt to help me with this code.
      I mostly needed help with the tcp data transmission, connection and reconnection 
      as my expierence is more firmware based I focused on the sensor data, anomally detection,
      diagnotistic requests, and handling overall logic as well as adding usefull features
      such as a verbose mode for debugging puposes. I also did my best to organize
      everything professionaly and make it user friendly to run the host and node.


This module implements the host-side controller for the distributed
diagnostics system. It is responsible for:

- Accepting TCP connections from sensor nodes
- Receiving and logging telemetry data
- De-duplicating telemetry using (node_id, boot_id, seq)
- Issuing cumulative ACKs to allow nodes to clear buffered data
- Periodically sending configuration updates (cfg messages)
- Periodically issuing diagnostic requests and retrying them across outages



This file intentionally contains only orchestration logic.
Scheduling and protocol helpers live in:
  - common.protocol
  - common.timeutil
  - host.scheduler
"""
import socket
import json
import time
import random

from common.protocol import (
    TYPE_TELEMETRY,
    TYPE_ACK,
    TYPE_DIAG_RESP,
    send_jsonl,
    stream_key,
)
from common.timeutil import utc_ts_filename, utc_ts
from host_lib.scheduler import StreamCtrl, tick_control
from common.constants import HOST_IP, HOST_PORT


def utc_ts_for_filename():
    return utc_ts_filename()


def main():

    bind_ip = HOST_IP
    port = HOST_PORT

    # how many messages before sending an ack back
    ACK_EVERY_N = 50
    ACK_EVERY_S = 0.5

    # time periods for updates from the host
    CFG_PERIOD_S = 5.0  # change cfg every 5s
    DIAG_PERIOD_S = 12.0  # run diag every 12s
    DIAG_RETRY_S = 2.0
    DIAG_TIMEOUT_S = 10.0  # if no diag response within 10s give up to prevent infinite retry

    # ---- fresh log file per host run with date and time of start----
    log_name = f"telemetry_log_{utc_ts_for_filename()}.jsonl"
    print(f"[host] log file: {log_name}", flush=True)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((bind_ip, port))
    server.listen(1)

    print(f"[host] listening on {bind_ip}:{port}", flush=True)

    conn = None
    logf = open(log_name, "a", encoding="utf-8")  # open once per host run

    # Persist across reconnects (within this host process)
    last_seen = {}       # (node_id, boot_id) -> highest seq seen
    last_acked = {}      # (node_id, boot_id) -> last acked seq
    last_ack_time = {}   # (node_id, boot_id) -> time.time()

    # Control-plane state per stream
    ctrl = {}  # (node_id, boot_id) -> StreamCtrl

    try:
        while True:
            conn, addr = server.accept()
            print(f"[host] connected from {addr}", flush=True)
            conn.settimeout(0.1)

            buf = b""

            # ---- fault injection: schedule a "host reboot" ----
            reboot_at = time.time() + random.uniform(20.0, 40.0)

            try:
                while True:
                    if time.time() >= reboot_at:
                        print(
                            "[host][fault] simulating reboot: closing connection", flush=True)
                        break

                    # --- recv with timeout ---
                    try:
                        data = conn.recv(4096)
                        if data == b"":
                            print("[host] client disconnected", flush=True)
                            break
                        buf += data
                    except socket.timeout:
                        data = None
                    except ConnectionResetError:
                        print("[host] connection reset by peer", flush=True)
                        break

                    # --- parse jsonl frames ---
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        if not line.strip():
                            continue
                        try:
                            msg = json.loads(line.decode("utf-8"))
                        except json.JSONDecodeError:
                            print("[host] bad json line", flush=True)
                            continue

                        mtype = msg.get("type")

                        if mtype == TYPE_TELEMETRY:
                            key = stream_key(msg)
                            if key is None:
                                continue
                            seq = msg.get("seq")
                            if not isinstance(seq, int):
                                continue

                            # Make sure stream control exists
                            ctrl.setdefault(key, StreamCtrl())

                            prev = last_seen.get(key, -1)

                            # de-dup: only commit if new
                            if seq > prev:
                                last_seen[key] = seq
                                logf.write(json.dumps(msg) + "\n")
                                logf.flush()

                            # block ACK
                            now = time.time()
                            last_ack = last_acked.get(key, -1)
                            last_t = last_ack_time.get(key, 0.0)

                            need_ack_by_count = (
                                last_seen[key] - last_ack) >= ACK_EVERY_N
                            need_ack_by_time = (now - last_t) >= ACK_EVERY_S

                            if last_seen[key] > last_ack and (need_ack_by_count or need_ack_by_time):
                                ack = {
                                    "type": TYPE_ACK,
                                    "node_id": key[0],
                                    "boot_id": key[1],
                                    "ack_seq": last_seen[key],
                                }
                                send_jsonl(conn, ack)
                                last_acked[key] = last_seen[key]
                                last_ack_time[key] = now

                        elif mtype == TYPE_DIAG_RESP:
                            key = stream_key(msg)
                            logf.write(json.dumps(msg) + "\n")
                            logf.flush()

                            # clear outstanding diag if it matches
                            if key is not None and key in ctrl:
                                st = ctrl[key]
                                rid = msg.get("request_id")
                                if st.diag_outstanding and rid == st.diag_request_id:
                                    st.diag_outstanding = False
                                    print(
                                        f"[host] got diag_resp from {key} request_id={rid}", flush=True)

                        else:
                            # log other msg types
                            logf.write(json.dumps(msg) + "\n")
                            logf.flush()

                    # ---- periodically send cfg + diag ----
                    now = time.time()
                    for key, st in list(ctrl.items()):
                        tick_control(
                            conn, key, st,
                            now=now,
                            cfg_period_s=CFG_PERIOD_S,
                            diag_period_s=DIAG_PERIOD_S,
                            diag_retry_s=DIAG_RETRY_S,
                            diag_timeout_s=DIAG_TIMEOUT_S,
                        )

            finally:
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass
                conn = None
                print("[host] connection closed; waiting for reconnect", flush=True)

    except KeyboardInterrupt:
        print(f"[host] {utc_ts()} shutdown requested (Ctrl+C)", flush=True)

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        try:
            server.close()
        except Exception:
            pass
        try:
            logf.close()
        except Exception:
            pass

        print(f"[host] {utc_ts()} exiting", flush=True)


if __name__ == "__main__":
    main()

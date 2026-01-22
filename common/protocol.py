from __future__ import annotations

import json
import select
import socket
from typing import Any, Dict, List, Tuple, Optional


# ---- Message types
TYPE_TELEMETRY = "telemetry"
TYPE_ACK = "ack"
TYPE_CFG = "cfg"
TYPE_DIAG_REQ = "diag_req"
TYPE_DIAG_RESP = "diag_resp"


# --- Send Json object ------
def send_jsonl(sock: socket.socket, obj: Dict[str, Any]) -> None:
    sock.sendall((json.dumps(obj) + "\n").encode("utf-8"))


def recv_jsonl_nonblocking(
    sock: socket.socket,
    rx_buf: bytes,
    *,
    max_bytes: int = 4096,
) -> Tuple[List[Dict[str, Any]], bytes]:

    msgs: List[Dict[str, Any]] = []

    r, _, _ = select.select([sock], [], [], 0.0)  # Check if data available
    if not r:
        return msgs, rx_buf

    # Read data and raise error if disconnect occured
    data = sock.recv(max_bytes)
    if not data:
        raise OSError("peer disconnected")

    rx_buf += data  # make continous buffer

    while b"\n" in rx_buf:  # Split messages by new line character
        line, rx_buf = rx_buf.split(b"\n", 1)
        if not line.strip():
            continue
        try:
            obj = json.loads(line.decode("utf-8"))
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            msgs.append(obj)

    return msgs, rx_buf


# ---- Returns node ID and bood ID -------
def stream_key(msg: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    nid = msg.get("node_id")
    bid = msg.get("boot_id")
    if isinstance(nid, str) and nid and isinstance(bid, str) and bid:
        return (nid, bid)
    return None

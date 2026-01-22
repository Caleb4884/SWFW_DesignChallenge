from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Tuple, Optional

from common.protocol import TYPE_CFG, TYPE_DIAG_REQ, send_jsonl
from common.timeutil import utc_ts, utc_ts_filename

# Create unique string for diagnostic requests


def make_request_id() -> str:
    return f"diag-{utc_ts_filename()}-{random.randint(1000, 9999)}"


@dataclass
class StreamCtrl:
    cfg_ver: int = 0
    last_cfg_sent: float = 0.0

    last_diag_started: float = 0.0
    diag_outstanding: bool = False
    diag_request_id: Optional[str] = None
    diag_sent_time: float = 0.0

# Used to randomly send diagnostic and config changes


def tick_control(
    conn: Any,
    key: Tuple[str, str],
    ctrl: StreamCtrl,
    *,
    now: float,
    cfg_period_s: float = 5.0,
    diag_period_s: float = 12.0,
    diag_retry_s: float = 2.0,
    diag_timeout_s: float = 10.0,
) -> None:

    _maybe_send_cfg(conn, key, ctrl, now, cfg_period_s)
    _maybe_send_or_retry_diag(conn, key, ctrl, now,
                              diag_period_s, diag_retry_s, diag_timeout_s)

# Determine if enough time has passed to send new cfg


def _maybe_send_cfg(conn, key, ctrl: StreamCtrl, now: float, cfg_period_s: float) -> None:
    if (now - ctrl.last_cfg_sent) < cfg_period_s:
        return

    # CFG increases because node wont accept a lower config version than what it has currently
    ctrl.cfg_ver += 1

    # randomly change vib max to prove constant changes
    vib_max = round(random.uniform(0.06, 0.08), 3)
    # when in verbose mode make temp likely to have error
    temp_max = 2.3 if (ctrl.cfg_ver % 2 == 0) else 2.1

    cfg_msg = {
        "type": TYPE_CFG,
        "node_id": key[0],
        "boot_id": key[1],
        "cfg_ver": ctrl.cfg_ver,
        "params": {
            # enable verbose mode when odd config version
            "verbose_anomaly": (ctrl.cfg_ver % 2 == 1),
            "anomalyLimits": {
                "temp_v": {"min": 2.0, "max": temp_max},
                "humidity": {"min": 28.0, "max": 75.0},
                "vibration": {"min": 0.005, "max": vib_max},
            },
            # node apply_cfg currently ignores railLimits
            "railLimits": {
                "rail_3v3": {"min": 3.10, "max": 3.50},
                "rail_5v": {"min": 4.75, "max": 5.25},
            },
        },
    }

    send_jsonl(conn, cfg_msg)
    ctrl.last_cfg_sent = now


def _maybe_send_or_retry_diag(
    conn,
    key,
    ctrl: StreamCtrl,
    now: float,
    diag_period_s: float,
    diag_retry_s: float,
    diag_timeout_s: float,
) -> None:
    # Start a new diagnostic periodically if none outstanding
    if not ctrl.diag_outstanding:
        if (now - ctrl.last_diag_started) < diag_period_s:
            return
        ctrl.diag_outstanding = True
        ctrl.diag_request_id = make_request_id()
        ctrl.diag_sent_time = 0.0  # force immediate send
        ctrl.last_diag_started = now

    # Timeout
    if (now - ctrl.last_diag_started) > diag_timeout_s:
        ctrl.diag_outstanding = False
        ctrl.diag_request_id = None
        ctrl.diag_sent_time = 0.0
        return

    # Retry cadence
    if (now - ctrl.diag_sent_time) < diag_retry_s:
        return

    diag_req = {
        "type": TYPE_DIAG_REQ,
        "node_id": key[0],
        "boot_id": key[1],
        "request_id": ctrl.diag_request_id,
        "ts": utc_ts(),
    }
    send_jsonl(conn, diag_req)
    ctrl.diag_sent_time = now

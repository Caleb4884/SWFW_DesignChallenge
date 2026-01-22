
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Callable, Optional

from common.protocol import (
    TYPE_ACK,
    TYPE_CFG,
    TYPE_DIAG_REQ,
    TYPE_DIAG_RESP,
    send_jsonl,
)
from common.timeutil import iso_ts

# Class for node id and boot id


@dataclass
class NodeIds:
    node_id: str
    boot_id: str

# Class for the voltage rails of the node for diagnostic


@dataclass
class RailSensors:
    rail_3v3: Any
    rail_5v: Any


# ---- Determine correct response depending on message type
def handle_host_msg(
    msg: Dict[str, Any],
    *,
    ids: NodeIds,
    cfg: Dict[str, Any],
    buf: Any,   # Buffer
    sock: Any,  # connected socket
    rails: RailSensors,
    apply_cfg: Callable[[Dict[str, Any], Dict[str, Any], str, str], bool],
    run_rail_diagnostic: Callable[..., Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
   # Return diag response if requested
    mtype = msg.get("type")

    # ---- ACK ----
    if mtype == TYPE_ACK:
        if msg.get("node_id") != ids.node_id or msg.get("boot_id") != ids.boot_id:
            return None
        ack_seq = msg.get("ack_seq")
        if isinstance(ack_seq, int):
            buf.mark_acked(ack_seq)
        return None

    # ---- CFG ----
    if mtype == TYPE_CFG:
        # apply_cfg already validates node_id/boot_id and cfg_ver monotonicity
        apply_cfg(msg, cfg, ids.node_id, ids.boot_id)
        return None

    # ---- DIAG REQ ----
    if mtype == TYPE_DIAG_REQ:
        if msg.get("node_id") != ids.node_id or msg.get("boot_id") != ids.boot_id:
            return None

        diag = run_rail_diagnostic(
            cfg=cfg,
            verbose=cfg.get("verbose_anomaly", False),
            rail_3v3=rails.rail_3v3,
            rail_5v=rails.rail_5v,
        )

        resp = {
            "type": TYPE_DIAG_RESP,
            "node_id": ids.node_id,
            "boot_id": ids.boot_id,
            "request_id": msg.get("request_id"),
            "ts": iso_ts(),
            "cfg_ver": cfg.get("cfg_ver", 0),
            "status": diag.get("status"),
            "details": diag.get("details"),
        }
        send_jsonl(sock, resp)
        return resp

    return None


# ---- Organize Messages
def handle_host_msgs(
    msgs: list[Dict[str, Any]],
    *,
    ids: NodeIds,
    cfg: Dict[str, Any],
    buf: Any,
    sock: Any,
    rails: RailSensors,
    apply_cfg: Callable[[Dict[str, Any], Dict[str, Any], str, str], bool],
    run_rail_diagnostic: Callable[..., Dict[str, Any]],
) -> None:
    for m in msgs:
        handle_host_msg(
            m,
            ids=ids,
            cfg=cfg,
            buf=buf,
            sock=sock,
            rails=rails,
            apply_cfg=apply_cfg,
            run_rail_diagnostic=run_rail_diagnostic,
        )

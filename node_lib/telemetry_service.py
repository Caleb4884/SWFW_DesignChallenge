# node/telemetry_service.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Callable


@dataclass
class NodeIds:
    node_id: str
    boot_id: str


@dataclass
class NodeSensors:
    temp: Any
    humidity: Any
    vibration: Any


# ------ Read from sensor objects ------
def read_telemetry_values(sensors: NodeSensors) -> Dict[str, Any]:
    return {
        "temp_v": sensors.temp.read(),
        "humidity": sensors.humidity.read(),
        "vibration": sensors.vibration.read(),
    }


def make_telemetry_msg(
    *,
    ids: NodeIds,
    seq: int,
    cfg: Dict[str, Any],
    sensors: NodeSensors,
    build_telemetry: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Create one telemetry message dict.
    Parameters:
    ids:
      NodeIds(node_id, boot_id)
    seq:
      Telemetry sequence number
    cfg:
      Node config dict.
        - cfg_ver
        - verbose_anomaly
        - anomalyLimits
    sensors:
      NodeSensors with .read() methods
    build_telemetry:

    Return:
    dictionary suitable for JSONL send and buffering
    """
    values = read_telemetry_values(sensors)

    return build_telemetry(
        node_id=ids.node_id,
        boot_id=ids.boot_id,
        seq=seq,
        values=values,
        limits=cfg["anomalyLimits"],
        cfg_ver=cfg.get("cfg_ver", 0),
        verbose_anomaly=cfg.get("verbose_anomaly", False),
    )

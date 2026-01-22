import time
from datetime import datetime, timezone


def limit_check(values: dict, limits: dict):
    reasons = []
    for name, x in values.items():
        lim = limits.get(name)
        if not lim:
            continue
        lo = lim.get("min")
        hi = lim.get("max")
        if lo is not None and x < lo:
            reasons.append(f"{name}_below_min")
        if hi is not None and x > hi:
            reasons.append(f"{name}_above_max")
    return (len(reasons) > 0), reasons


def build_telemetry(node_id: str, boot_id: str, seq: int,
                    values: dict, limits: dict,
                    cfg_ver: int, verbose_anomaly: bool) -> dict:
    anomaly_flag, reasons = limit_check(values, limits)

    msg = {
        "type": "telemetry",
        "node_id": node_id,
        "boot_id": boot_id,
        "seq": seq,
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "cfg_ver": cfg_ver,

        # ✅ flat sensor fields
        **values,

        # ✅ always include boolean
        "anomaly": anomaly_flag,
    }

    # ✅ add extra info only when verbose
    if verbose_anomaly:
        msg["anomaly_reasons"] = reasons
        msg["anomalyLimits"] = limits  # include limits only in verbose

    return msg

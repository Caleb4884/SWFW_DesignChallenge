
def update(dst: dict, src: dict) -> None:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            update(dst[k], v)
        else:
            dst[k] = v


# ---- Parse Config Request and update dictionary storing configuration
def apply_cfg(msg: dict, cfg: dict, node_id: str, boot_id: str) -> bool:
    if msg.get("type") != "cfg":
        return False
    if msg.get("node_id") != node_id or msg.get("boot_id") != boot_id:
        return False

    new_ver = msg.get("cfg_ver")
    params = msg.get("params")
    if not isinstance(new_ver, int) or not isinstance(params, dict):
        return False
    if new_ver <= cfg.get("cfg_ver", -1):
        return False

    # Apply verbose flag if present
    if "verbose_anomaly" in params:
        if not isinstance(params["verbose_anomaly"], bool):
            return False
        cfg["verbose_anomaly"] = params["verbose_anomaly"]

    # Apply anomalyLimits if present
    if "anomalyLimits" in params:
        if not isinstance(params["anomalyLimits"], dict):
            return False
        cfg.setdefault("anomalyLimits", {})
        update(cfg["anomalyLimits"], params["anomalyLimits"])

    cfg["cfg_ver"] = new_ver
    print(f"[node] applied cfg_ver={new_ver} params={params}")
    return True

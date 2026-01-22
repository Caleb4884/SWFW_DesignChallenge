
# --- Used to perform checks on various sensors and make reasons for error to be reported in verbose logging mode
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


# Perform diagnostic Checks similiar to anomaly detection
def run_diagnostics(latest_values: dict, cfg: dict, verbose: bool) -> dict:
    rail_keys = cfg.get("railLimits", {}).keys()
    rail_values = {k: latest_values.get(k)
                   for k in rail_keys if k in latest_values}

    rail_fault, rail_reasons = limit_check(
        rail_values, cfg.get("railLimits", {}))

    details = {
        "rail_ok": (not rail_fault),
        "rails": rail_values,  # helpful even when not verbose
    }
    if verbose and rail_reasons:
        details["rail_reasons"] = rail_reasons

    status = "ok" if (not rail_fault) else "fail"
    return {"status": status, "details": details}

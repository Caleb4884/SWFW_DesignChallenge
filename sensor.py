import random
from datetime import datetime, timezone
from telemetryHandling import limit_check


class sensor:

    def __init__(self, name, value=0.0, step_std=0.01, precision=2, min_val=None, max_val=None):
        self.name = name
        self.value = float(value)
        self.step_std = float(step_std)
        self.min_val = min_val
        self.max_val = max_val
        self.precision = precision

    # Method to emulate sensor read, I have assumed that it has already been converted from raw ADC values to standard units
    def read(self):
        # Random gausian noise walk update
        self.value += random.gauss(0.0, self.step_std)

        # Clamp at max or min if desired
        if self.min_val is not None:
            self.value = max(self.min_val, self.value)
        if self.max_val is not None:
            self.value = min(self.max_val, self.value)

        # Round to decimal precision
        self.value = round(self.value, self.precision)

        return self.value


def iso_ts():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def run_rail_diagnostic(cfg: dict, verbose: bool, rail_3v3, rail_5v) -> dict:
    rail_values = {
        "rail_3v3": rail_3v3.read(),
        "rail_5v":  rail_5v.read(),
    }

    rail_fault, rail_reasons = limit_check(
        rail_values, cfg.get("railLimits", {}))

    details = {
        "rail_ok": (not rail_fault),
        "rails": rail_values,  # include sampled values (useful)
    }
    if verbose and rail_reasons:
        details["rail_reasons"] = rail_reasons

    return {
        "status": "ok" if (not rail_fault) else "fail",
        "details": details,
    }

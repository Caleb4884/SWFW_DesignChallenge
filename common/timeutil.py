# common/timeutil.py
from __future__ import annotations

from datetime import datetime, timezone

# ----Get UTC time and format----


def utc_ts_iso(*, timespec: str = "seconds") -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec=timespec)
        .replace("+00:00", "Z")
    )

# ---- Use Time to create log file ------


def utc_ts_filename() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_ts() -> str:
    return utc_ts_iso()


def iso_ts() -> str:
    return utc_ts_iso()

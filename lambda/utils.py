from datetime import datetime, timezone
from typing import Optional, Any

import pandas as pd
from dateutil.parser import isoparse


def parse_iso_utc(s: str) -> datetime:
    """Parse ISO/date-only to an aware UTC datetime."""
    s = s.strip()
    if len(s) == 10:  # 'YYYY-MM-DD'
        return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    dt = isoparse(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def ms(dt_obj: datetime) -> str:
    # ensure aware UTC before converting to epoch ms
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    return str(int(dt_obj.astimezone(timezone.utc).timestamp() * 1000))


def _parse_hs_datetime(v: Any) -> Optional[pd.Timestamp]:
    """
    - If numeric-like -> treat as epoch ms
    - Else try parse ISO
    """
    if v is None or v == "":
        return None
    sv = str(v)
    if sv.isdigit():
        try:
            return pd.to_datetime(int(sv), unit="ms", utc=True, errors="coerce")
        except Exception:
            return None
    ts = pd.to_datetime(sv, utc=True, errors="coerce")
    return None if pd.isna(ts) else ts

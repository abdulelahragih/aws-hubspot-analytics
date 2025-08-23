import logging
from datetime import datetime, timezone
from typing import Optional, Any

import pandas as pd
from dateutil.parser import isoparse
import awswrangler as wr

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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


def read_parquet(path: str) -> Optional[pd.DataFrame]:
    """
    Read a Parquet file from S3 and return a DataFrame.
    """
    try:
        df = wr.s3.read_parquet(path=path, dataset=True, dtype_backend="pyarrow")
        return df
    except Exception as e:
        LOG.error(f"Failed to read Parquet from {path}: {e}")
        return None

if __name__ == "__main__":
    # Example usage
    print(utc_now_iso())
    print(parse_iso_utc(utc_now_iso()).date())
    dt = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    print(dt)
    print(_parse_hs_datetime("2025-03-10T13:47:37.635Z"))
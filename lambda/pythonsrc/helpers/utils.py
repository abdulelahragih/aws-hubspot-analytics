import logging
import os
from datetime import datetime, timezone
from typing import Optional, Any, Union, Iterable, Mapping, Callable, Hashable, Dict, List, TypeVar
from dateutil.relativedelta import relativedelta
import pandas as pd
from dateutil.parser import isoparse
import awswrangler as wr

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

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


def parse_hs_datetime(v: Any) -> Optional[pd.Timestamp]:
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


def format_as_hs_datetime(dt: Optional[Union[pd.Timestamp, datetime, str]]) -> Optional[str]:
    if dt is None or (isinstance(dt, pd.Timestamp) and pd.isna(dt)):
        return None
    if isinstance(dt, str):
        dt = parse_iso_utc(dt)
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"


def pick_date(
        candidate: Optional[Union[str, datetime]] = None,
        env_var: Optional[str] = None,
        fallback_months: int = 3
) -> str:
    """
    Pick a date from three sources (in priority order):
    1. Candidate value if provided.
    2. Environment variable (if env_var is set).
    3. Fallback to today's date minus fallback_months in UTC.

    Args:
        candidate (Optional[Union[str, datetime]]): Explicit date (string or datetime).
        env_var (Optional[str]): Environment variable name to check.
        fallback_months (int): How many months to go back if nothing provided.

    Returns:
        str: The chosen date string in iso format.
    """
    # 1. Explicit value
    if candidate:
        if isinstance(candidate, datetime):
            return candidate.isoformat()
        elif isinstance(candidate, str):
            parsed_date = parse_iso_utc(candidate)
            return parsed_date.isoformat()

    # 2. Environment variable
    if env_var:
        env_value = os.environ.get(env_var)
        if env_value:
            parsed_date = parse_iso_utc(env_value)
            return parsed_date.isoformat()

    # 3. Fallback
    fallback = datetime.now(timezone.utc) - relativedelta(months=fallback_months)
    return fallback.isoformat()


T = TypeVar("T", bound=Mapping[str, Any])


def merge_dedupe(
        *iterables: Iterable[T],
        key: str | Callable[[T], Hashable] = "id",
        dt_key: Optional[datetime] = None,
        resolver: Optional[Callable[[T, T], T]] = None,
) -> List[T]:
    """
    Merge multiple iterables of mapping-like items and deduplicate by an ID key.
    - By default: 'last write wins' (later items override earlier ones).
    - If ts_key is provided: keep the item with the maximum ts_key value.
      (Assumes comparable values: ints/floats/ISO strings already aligned, etc.)
    - If resolver is provided: called as resolver(old_item, new_item) -> chosen_item.

    Args:
        *iterables: Any number of iterables of items (e.g., created, modified).
        key: Field name or function to extract the dedupe key (default "id").
        dt_key: Optional field name used to choose the newest item per key.
        resolver: Optional custom conflict resolver.

    Returns:
        A list of unique items (order not guaranteed). Convert to dict if needed.
    """

    def get_key(item: T) -> Hashable:
        return key(item) if callable(key) else item.get(key)

    items_by_id: Dict[Hashable, T] = {}

    for it in iterables:
        for item in it:
            k = get_key(item)
            if k is None:
                continue  # skip items without a key

            if k not in items_by_id:
                items_by_id[k] = item
                continue

            # Conflict resolution
            if resolver is not None:
                items_by_id[k] = resolver(items_by_id[k], item)
            elif dt_key is not None:
                prev, curr = items_by_id[k], item
                # Keep the one with the larger timestamp value
                curr_dt = curr.get(dt_key)
                prev_dt = prev.get(dt_key)
                curr_dt = parse_iso_utc(curr_dt).timestamp() if curr_dt else None
                prev_dt = parse_iso_utc(prev_dt).timestamp() if prev_dt else None
                if curr_dt is not None and (prev_dt is None or curr_dt > prev_dt):
                    items_by_id[k] = curr
            else:
                # Last write wins
                items_by_id[k] = item

    return list(items_by_id.values())


if __name__ == "__main__":
    # Example usage
    now = datetime.now(timezone.utc)
    print(utc_now_iso())
    print(parse_iso_utc(now.isoformat()).date())
    print(parse_iso_utc(now.isoformat()))
    print(format_as_hs_datetime(now))
    print(format_as_hs_datetime(now.isoformat()))
    print(parse_hs_datetime("2025-03-10T13:47:37.635Z"))
    print(pick_date(env_var="NON_EXISTENT_ENV_VAR", fallback_months=12))

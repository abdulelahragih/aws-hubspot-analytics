from datetime import datetime, timezone

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

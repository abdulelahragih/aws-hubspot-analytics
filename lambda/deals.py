import logging
import os
from typing import Any, Dict, List

import pandas as pd

from hubspot_client import get_client, utc_now_iso
from storage import write_parquet, ensure_bucket_env


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


def fetch_deals_between(
    from_iso: str, to_iso: str, page_limit: int = 100
) -> List[Dict[str, Any]]:
    client = get_client()
    props = ["dealname", "hubspot_owner_id", "amount", "createdate", "dealstage"]
    rows = client.search_between(
        object_type="deals",
        properties=props,
        from_iso=from_iso,
        to_iso=to_iso,
        page_limit=page_limit,
        primary_prop="createdate",
        fallback_prop="createdate",
    )
    return rows


def deals_handler(_event, _context):
    LOG.info("Running deals ingest")
    ensure_bucket_env()
    from_iso = os.environ.get("DEALS_FROM", os.environ.get("START_DATE", "2024-01-01"))
    to_iso = utc_now_iso()
    deals = fetch_deals_between(from_iso, to_iso)
    # Normalize to a minimal curated schema
    if not deals:
        return {"written": 0}
    records: List[Dict[str, Any]] = []
    for d in deals:
        p = d.get("properties", {})
        records.append(
            {
                "deal_id": d.get("id"),
                "owner_id": p.get("hubspot_owner_id"),
                "created_at": p.get("createdate"),
                "amount": p.get("amount"),
                "dealstage": p.get("dealstage"),
            }
        )
    df = pd.DataFrame.from_records(records)
    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
        df["dt"] = df["created_at"].dt.strftime("%Y-%m-%d")
    written = 0
    if not df.empty:
        written = write_parquet(df, table="deals")
    return {"written": int(written)}

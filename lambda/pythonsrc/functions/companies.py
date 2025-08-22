import logging
import os
from typing import Any, Dict, List

import awswrangler as wr
import pandas as pd

from hubspot_client import get_client
from helpers.utils import utc_now_iso
from helpers.storage import ensure_bucket_env

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

START_DATE = os.environ.get("START_DATE", "2024-01-01")
S3_BUCKET = os.environ.get("S3_BUCKET")


def companies_handler(_event, _context):
    """Ingest companies as a dimension (id, name, created, last modified)."""
    ensure_bucket_env()

    client = get_client()
    props = ["name", "createdate", "hs_lastmodifieddate", "domain"]
    rows: List[Dict[str, Any]] = client.paginated_request(
        method="GET",
        endpoint="/crm/v3/objects/companies",
        params={
            "properties": ",".join(props),
            "limit": 100,
        },
    )

    if not rows:
        LOG.info("No companies to write")
        return {"written": 0}

    recs: List[Dict[str, Any]] = []
    for r in rows:
        p = r.get("properties", {})
        recs.append(
            {
                "company_id": r.get("id"),
                "name": p.get("name"),
                "created_at": p.get("createdate"),
                "last_modified_at": p.get("hs_lastmodifieddate"),
            }
        )

    df = pd.DataFrame.from_records(recs)
    if df.empty:
        return {"written": 0}
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["last_modified_at"] = pd.to_datetime(
        df["last_modified_at"], utc=True, errors="coerce"
    )
    # Partition by day of last_modified for incremental reads
    df["dt"] = df["last_modified_at"].fillna(df["created_at"]).dt.strftime("%Y-%m-%d")

    path = f"s3://{S3_BUCKET}/dim/companies/"
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        compression="snappy",
        partition_cols=["dt"],
        mode="overwrite_partitions",
    )
    LOG.info("Wrote %s companies to %s", len(df), path)
    return {"written": int(len(df))}

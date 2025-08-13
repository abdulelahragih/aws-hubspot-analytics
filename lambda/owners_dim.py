import logging
import os
from typing import Dict, List

import awswrangler as wr
import pandas as pd

from owners import get_owners
from storage import ensure_bucket_env


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET")


def owners_dim_handler(_event, _context):
    """Ingest owners as a small dimension table."""
    ensure_bucket_env()
    owners_map: Dict[str, Dict[str, str]] = get_owners()
    if not owners_map:
        LOG.info("No owners found")
        return {"written": 0}
    rows: List[Dict[str, str]] = []
    for owner_id, data in owners_map.items():
        rows.append(
            {
                "owner_id": str(owner_id),
                "owner_name": data.get("name") or "Unknown",
                "owner_email": data.get("email") or None,
            }
        )
    df = pd.DataFrame.from_records(rows)
    path = f"s3://{S3_BUCKET}/dim/owners/"
    wr.s3.to_parquet(df=df, path=path, dataset=True, compression="snappy")
    LOG.info("Wrote %s owners to %s", len(df), path)
    return {"written": int(len(df))}

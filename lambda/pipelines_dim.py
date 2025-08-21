import logging
import os
from typing import Any, Dict, List

import awswrangler as wr
import pandas as pd

from hubspot_client import get_client
from storage import ensure_bucket_env


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET")


def pipelines_dim_handler(_event, _context):
    """Ingest deal pipelines and stages into a dim table: dim_stage."""
    ensure_bucket_env()
    client = get_client()

    data = client.request(
        method="GET",
        endpoint="/crm/v3/pipelines/deals",
    )
    results = data.get("results", [])
    rows: List[Dict[str, Any]] = []
    for pipe in results:
        pipeline_id = pipe.get("id")
        pipeline_label = pipe.get("label")
        for st in pipe.get("stages", []) or []:
            rows.append(
                {
                    "pipeline_id": pipeline_id,
                    "pipeline_label": pipeline_label,
                    "stage_id": st.get("id"),
                    "stage_label": st.get("label"),
                    "display_order": st.get("displayOrder"),
                    "is_closed": (st.get("metadata", {}) or {}).get("isClosed"),
                    "probability": (st.get("metadata", {}) or {}).get("probability"),
                }
            )

    if not rows:
        LOG.info("No pipelines/stages returned")
        return {"written": 0}

    df = pd.DataFrame.from_records(rows)
    path = f"s3://{S3_BUCKET}/dim/stage/"
    wr.s3.to_parquet(df=df, path=path, dataset=True, compression="snappy")
    LOG.info("Wrote %s stage rows to %s", len(df), path)
    return {"written": int(len(df))}

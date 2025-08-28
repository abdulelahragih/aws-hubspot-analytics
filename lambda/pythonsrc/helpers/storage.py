import logging
import os
from datetime import datetime

import awswrangler as wr
import pandas as pd

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET")


def ensure_bucket_env() -> None:
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET not set")


def write_parquet(df: pd.DataFrame, table: str) -> int:
    if df.empty:
        LOG.info("No rows to write for table=%s", table)
        return 0
    ensure_bucket_env()
    dt = datetime.utcnow().strftime("%Y-%m-%d")
    df["dt"] = dt
    path = f"s3://{S3_BUCKET}/curated/{table}/"
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        compression="snappy",
        partition_cols=["dt"],
    )
    LOG.info("Wrote %s rows to %s", len(df), path)
    return len(df)



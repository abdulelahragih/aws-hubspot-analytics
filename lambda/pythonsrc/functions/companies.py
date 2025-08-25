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

    from helpers.sync_state import get_sync_manager

    client = get_client()
    sync_manager = get_sync_manager()

    # Determine sync strategy and date range
    from_date, to_date = sync_manager.get_sync_dates("companies")
    props = ["name", "createdate", "hs_lastmodifieddate", "domain"]

    if from_date:
        # Incremental sync using dual-fetch strategy
        LOG.info(f"Performing incremental sync from {from_date} to {to_date}")

        # Fetch newly created companies
        created_companies: List[Dict[str, Any]] = client.search_between_chunked(
            object_type="companies",
            properties=props,
            from_iso=from_date,
            to_iso=to_date,
            search_prop="createdate",
            sort_direction="ASCENDING",
        )

        # Fetch modified companies
        modified_companies: List[Dict[str, Any]] = client.search_between_chunked(
            object_type="companies",
            properties=props,
            from_iso=from_date,
            to_iso=to_date,
            search_prop="hs_lastmodifieddate",
            sort_direction="ASCENDING",
        )

        # Merge and deduplicate by company ID (keep the most recent version)
        companies_by_id: Dict[str, Dict[str, Any]] = {}
        for company in created_companies + modified_companies:
            company_id = company.get("id")
            if company_id:
                companies_by_id[company_id] = company

        rows = list(companies_by_id.values())
        LOG.info(
            f"Fetched {len(created_companies)} created and {len(modified_companies)} modified companies, deduplicated to {len(rows)} unique companies"
        )
    else:
        # Full sync using paginated request
        LOG.info("Performing full sync")
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
        # Update sync state even if no data to track that sync ran
        try:
            sync_manager.update_sync_state("companies", records_processed=0)
        except Exception as e:
            LOG.warning(f"Failed to update sync state: {e}")
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
        # Update sync state even if no data to track that sync ran
        try:
            sync_manager.update_sync_state("companies", records_processed=0)
        except Exception as e:
            LOG.warning(f"Failed to update sync state: {e}")
        return {"written": 0}
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["last_modified_at"] = pd.to_datetime(
        df["last_modified_at"], utc=True, errors="coerce"
    )

    # Extract date bounds for sync state tracking before partitioning transformation
    max_created, max_modified = sync_manager.extract_date_bounds_from_data(df)

    # Partition by day of last_modified for incremental reads
    df["dt"] = df["last_modified_at"].fillna(df["created_at"]).dt.strftime("%Y-%m-%d")

    path = f"s3://{S3_BUCKET}/dim/companies/"

    # Use the reusable merge strategy from sync_state manager
    sync_manager.write_with_merge_strategy(
        df=df, s3_path=path, partition_cols=["dt"], primary_key_col="company_id"
    )

    # Update sync state with the latest dates from the processed data
    try:
        sync_manager.update_sync_state(
            "companies",
            last_created_at=max_created,
            last_modified_at=max_modified,
            records_processed=len(df),
        )
    except Exception as e:
        LOG.warning(f"Failed to update sync state: {e}")

    LOG.info("Wrote %s companies to %s", len(df), path)
    return {"written": int(len(df))}

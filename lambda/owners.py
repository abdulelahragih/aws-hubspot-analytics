import logging
import os
from typing import Dict, List

import awswrangler as wr
import pandas as pd

from hubspot_client import get_client

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET")


def owners_handler(_event, _context):
    """Ingest owners as a small dimension table."""
    hub_client = get_client()

    try:
        result = hub_client.paginated_request(
            "GET",
            "/crm/v3/owners",
            params={"limit": 100},
        )
        LOG.info(f"Found {len(result)} owners.")

        # owners = {
        #     str(o.get("id")): {
        #         "name": f"{o.get('firstName') or ''} {o.get('lastName') or ''}".strip()
        #                 or "Unknown",
        #         "email": o.get("email") or "No email",
        #     }
        #     for o in result
        #     if o.get("id", False)
        # }
        if not result:
            LOG.info("No owners found")
            return {"written": 0}

        owners: List[Dict[str, str]] = []
        for owner in result:
            name = f"{owner.get('firstName') or ''} {owner.get('lastName') or ''}".strip()
            owners.append(
                {
                    "owner_id": owner.get("id"),
                    "owner_name": name or "Unknown",
                    "owner_email": owner.get("email") or None,
                    "created_at": owner.get("createdAt"),
                    "last_modified_at": owner.get("updatedAt"),
                }
            )
        df = pd.DataFrame.from_records(owners)
        path = f"s3://{S3_BUCKET}/dim/owners/"

        wr.s3.to_parquet(
            df=df,
            path=path,
            dataset=True,
            compression="snappy"
        )
        LOG.info("Wrote %s owners to %s", len(df), path)
        return {"written": int(len(df))}
    except Exception as e:
        LOG.error(f"Error getting owners: {e}")
        raise RuntimeError(f"Error getting owners: {e}") from e

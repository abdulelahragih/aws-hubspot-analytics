import logging
import os
import re
from typing import Any, Dict, List, Optional

import awswrangler as wr
import pandas as pd

from utils import _parse_hs_datetime
from hubspot_client import get_client, utc_now_iso
from storage import ensure_bucket_env

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET")

STG = {
    "op": "appointmentscheduled",
    "prep": "1067388789",
    "sent": "presentationscheduled",
    "won": "closedwon",
    "lost": "closedlost",
}

BASE_PROPS = [
    "dealname",
    "dealstage",
    "hubspot_owner_id",
    "amount",
    "createdate",
    "closedate",
]

STAGE_PROPS: List[str] = []
for sid in STG.values():
    STAGE_PROPS.extend([f"hs_date_entered_{sid}", f"hs_v2_date_entered_{sid}"])

SOURCE_PROPS = [
    "deal_source",
    "source",
    "lead_source",
    "hs_analytics_source",
    "hs_analytics_source_data_1",
]

ALL_PROPS = [*BASE_PROPS, *STAGE_PROPS, *SOURCE_PROPS]

SOURCE_KEY_RE = re.compile(r"(source|type|origin)", re.IGNORECASE)


def _pick_first(*values: Any) -> Any:
    for v in values:
        if v not in (None, ""):
            return v
    return None


def _get_associations_id(associations, association_key: str) -> Optional[str]:
    """Helper to get the first association ID from a list of associations."""
    if not associations:
        return None

    results = associations.get(association_key, {}).get("results", [])
    if not results:
        return None
    # Return the first ID from the results
    return results[0].get("id")


def _stage_ts(props: Dict[str, Any], code: str) -> Optional[pd.Timestamp]:
    """
    JS precedence: hs_v2_date_entered_* OR hs_date_entered_* (parse ms or ISO)
    """
    v = props.get(f"hs_v2_date_entered_{code}") or props.get(f"hs_date_entered_{code}")
    return _parse_hs_datetime(v)


def deals_handler(_event, _context):
    """Ingest a richer Raw Deal dataset (parity with fetchDealData.gs props).

    This uses CRM v3 search with createdate window (START_DATE..now) to bound volume.
    Associations (company/contact names) are not resolved here; join in Athena if needed.
    """
    LOG.info("Running raw deals ingest")
    ensure_bucket_env()

    client = get_client()

    result: List[Dict[str, Any]] = client.paginated_request(
        method="GET",
        endpoint="/crm/v3/objects/deals",
        params={
            "properties": ",".join(ALL_PROPS),
            "limit": 100,
            "associations": "company,contact",
        },
    )

    if not result:
        LOG.info("No deals to write")
        return {"written": 0}

    deals: List[Dict[str, Any]] = []
    for deal in result:
        properties = deal.get("properties", {})
        associations = deal.get("associations", {})
        company_id = _get_associations_id(associations, "companies")
        contact_id = _get_associations_id(associations, "contacts")
        parsed_deal = {
            "deal_id": deal.get("id"),
            "deal_name": properties.get("dealname", ""),
            "owner_id": properties.get("hubspot_owner_id"),
            "company_id": company_id,
            "contact_id": contact_id,
            "deal_stage": properties.get("dealstage"),
            "created_at": _parse_hs_datetime(properties.get("createdate")),
            "closed_at": _parse_hs_datetime(properties.get("closedate")),
            "last_modified_at": _parse_hs_datetime(properties.get("hs_lastmodifieddate")),
            "amount": properties.get("amount"),
            # stage dates in ms
            "op_detected_at": _stage_ts(properties, STG["op"]),
            "proposal_prep_at": _stage_ts(properties, STG["prep"]),
            "proposal_sent_at": _stage_ts(properties, STG["sent"]),
            "closed_won_at": _stage_ts(properties, STG["won"]),
            "closed_lost_at": _stage_ts(properties, STG["lost"]),
            # sources
            "source": _pick_first(
                properties.get("deal_source"),
                properties.get("source"),
                properties.get("lead_source"),
                properties.get("hs_analytics_source"),
                properties.get("hs_analytics_source_data_1"),
            ),
            "updated_at": pd.Timestamp.now(tz="UTC"),
        }
        deals.append(parsed_deal)

    df = pd.DataFrame.from_records(deals)
    if df.empty:
        LOG.info("No deals to write after normalization")
        return {"written": 0}

    df["dt"] = df["created_at"].dt.strftime("%Y-%m-%d")

    path = f"s3://{S3_BUCKET}/curated/deals/"
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        compression="snappy",
        partition_cols=["dt"],
    )
    LOG.info("Wrote %s raw deal rows to %s", len(df), path)
    return {"written": int(len(df))}

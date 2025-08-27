import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd
from helpers.sync_state import get_sync_manager

from helpers.utils import parse_hs_datetime, utc_now_iso
from hubspot_client import get_client
from helpers.storage import ensure_bucket_env

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
    return parse_hs_datetime(v)


def deals_handler(_event, _context):
    """Ingest a Raw Deal dataset (parity with fetchDealData.gs props).

    This uses CRM v3 search.
    Associations (company/contact names) are not resolved here; join in Athena if needed.
    """
    LOG.info("Running raw deals ingest")
    ensure_bucket_env()

    client = get_client()
    sync_manager = get_sync_manager()

    sync_state = sync_manager.get_sync_dates("deals")
    created_from_date = sync_state.new_records_checkpoint.isoformat() if sync_state.new_records_checkpoint else utc_now_iso()
    modified_from_date = sync_state.modified_records_check_point.isoformat() if sync_state.modified_records_check_point else utc_now_iso()
    to_date = utc_now_iso()

    if sync_state.is_incremental_sync_enabled:
        # Incremental sync using dual-fetch strategy
        LOG.info(f"Performing incremental sync from new:{created_from_date} modified:{modified_from_date} to {to_date}")

        # Fetch newly created deals
        created_deals: List[Dict[str, Any]] = client.search_between_chunked(
            object_type="deals",
            properties=ALL_PROPS + ["hs_lastmodifieddate", "createdate"],
            from_iso=created_from_date,
            to_iso=to_date,
            search_prop="createdate",
            sort_direction="ASCENDING",
        )

        # Fetch modified deals
        modified_deals: List[Dict[str, Any]] = client.search_between_chunked(
            object_type="deals",
            properties=ALL_PROPS + ["hs_lastmodifieddate", "createdate"],
            from_iso=modified_from_date,
            to_iso=to_date,
            search_prop="hs_lastmodifieddate"
        )

        # Merge and deduplicate by deal ID (keep the most recent version)
        deals_by_id: Dict[str, Dict[str, Any]] = {}
        combined_deals = created_deals + modified_deals
        for deal in combined_deals:
            deal_id = deal.get("id")
            if deal_id:
                deals_by_id[deal_id] = deal

        result = list(deals_by_id.values())
        LOG.info(
            f"Fetched {len(created_deals)} created and {len(modified_deals)} modified deals, deduplicated to {len(result)} unique deals"
        )
    else:
        # Full sync using paginated request
        LOG.info("Performing full sync")
        result: List[Dict[str, Any]] = client.paginated_request(
            method="GET",
            endpoint="/crm/v3/objects/deals",
            params={
                "properties": ",".join(ALL_PROPS + ["hs_lastmodifieddate"]),
                "limit": 100,
                "associations": "company,contact",
            },
        )

    if not result:
        LOG.info("No deals to write")
        sync_manager.update_sync_state("deals", records_processed=0)
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
            "created_at": parse_hs_datetime(properties.get("createdate")),
            "closed_at": parse_hs_datetime(properties.get("closedate")),
            "last_modified_at": parse_hs_datetime(properties.get("hs_lastmodifieddate")),
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
            )
        }
        deals.append(parsed_deal)

    df = pd.DataFrame.from_records(deals)
    if df.empty:
        LOG.info("No deals to write after normalization")
        sync_manager.update_sync_state("deals", records_processed=0)
        return {"written": 0}

    df["dt"] = df["created_at"].dt.strftime("%Y-%m-%d")
    out_df = df.drop_duplicates(
        keep="last"
    )

    # Extract date bounds for sync state tracking
    max_created, max_modified = sync_manager.extract_date_bounds_from_data(out_df)

    path = f"s3://{S3_BUCKET}/curated/deals/"

    sync_manager.write_with_merge_strategy(
        df=out_df,
        s3_path=path,
        partition_cols=["dt"],
        primary_key_col="deal_id"
    )

    sync_manager.update_sync_state(
        "deals",
        last_created_at=max_created,
        last_modified_at=max_modified,
        records_processed=len(out_df),
    )

    LOG.info("Wrote %s raw deal rows to %s", len(out_df), path)
    return {"written": int(len(out_df))}

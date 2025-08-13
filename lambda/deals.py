import logging
import os
from typing import Any, Dict, List

import awswrangler as wr
import pandas as pd

from hubspot_client import get_client, utc_now_iso
from storage import ensure_bucket_env


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

START_DATE = os.environ.get("START_DATE", "2025-01-01")
S3_BUCKET = os.environ.get("S3_BUCKET")


def _pick_first(*values: Any) -> Any:
    for v in values:
        if v not in (None, ""):
            return v
    return None


def deals_handler(_event, _context):
    """Ingest a richer Raw Deal dataset (parity with fetchDealData.gs props).

    This uses CRM v3 search with createdate window (START_DATE..now) to bound volume.
    Associations (company/contact names) are not resolved here; join in Athena if needed.
    """
    LOG.info("Running raw deals ingest")
    ensure_bucket_env()

    from_iso = os.environ.get("DEALS_RAW_FROM", START_DATE)
    to_iso = utc_now_iso()

    client = get_client()
    # Stage IDs (match Apps Script constants)
    STG = {
        "op": "appointmentscheduled",
        "prep": "1067388789",  # custom stage id for Proposal Prep
        "sent": "presentationscheduled",
        "won": "closedwon",
        "lost": "closedlost",
    }
    stage_props = []
    for sid in STG.values():
        stage_props.append(f"hs_date_entered_{sid}")
        stage_props.append(f"hs_v2_date_entered_{sid}")

    props = [
        "dealname",
        "dealstage",
        "hubspot_owner_id",
        "amount",
        "createdate",
        "hs_lastmodifieddate",
        "closedate",
        # sources
        "deal_source",
        "source",
        "lead_source",
        "hs_analytics_source",
        "hs_analytics_source_data_1",
        *stage_props,
    ]

    rows: List[Dict[str, Any]] = client.search_between(
        object_type="deals",
        properties=props,
        from_iso=from_iso,
        to_iso=to_iso,
        page_limit=100,
        primary_prop="createdate",
        fallback_prop="createdate",
    )

    if not rows:
        LOG.info("No deals to write")
        return {"written": 0}

    records: List[Dict[str, Any]] = []
    deal_ids: List[str] = []
    for d in rows:
        p = d.get("properties", {})
        if d.get("id"):
            deal_ids.append(str(d.get("id")))

        # Stage dates helper
        def _stage_ms(code: str) -> int | None:
            v = p.get(f"hs_v2_date_entered_{code}") or p.get(f"hs_date_entered_{code}")
            if v is None:
                return None
            try:
                sv = str(v)
                if sv.isdigit():
                    return int(sv)
                ts = pd.to_datetime(sv, utc=True, errors="coerce")
                if pd.isna(ts):
                    return None
                return int(ts.value // 10**6)
            except Exception:
                return None

        records.append(
            {
                "deal_id": d.get("id"),
                "deal_name": p.get("dealname"),
                "owner_id": p.get("hubspot_owner_id"),
                "dealstage": p.get("dealstage"),
                "created_at": p.get("createdate"),
                "closed_at": p.get("closedate"),
                "last_modified_at": p.get("hs_lastmodifieddate"),
                "amount": p.get("amount"),
                # stage dates in ms
                "op_detected_ms": _stage_ms(STG["op"]),
                "proposal_prep_ms": _stage_ms(STG["prep"]),
                "proposal_sent_ms": _stage_ms(STG["sent"]),
                "closed_won_ms": _stage_ms(STG["won"]),
                "closed_lost_ms": _stage_ms(STG["lost"]),
                # sources
                "source_primary": _pick_first(
                    p.get("deal_source"),
                    p.get("source"),
                    p.get("lead_source"),
                    p.get("hs_analytics_source"),
                ),
                "source_secondary": p.get("hs_analytics_source_data_1"),
            }
        )

    # Resolve companies via associations v4 (deals -> companies)
    company_map: Dict[str, List[str]] = {}
    contact_map: Dict[str, List[str]] = {}
    try:
        if deal_ids:
            company_map = client.batch_read_associations_v4(
                from_object="deals", to_object="companies", from_ids=deal_ids
            )
            contact_map = client.batch_read_associations_v4(
                from_object="deals", to_object="contacts", from_ids=deal_ids
            )
    except Exception as e:
        LOG.warning("Skipping company association enrichment: %s", e)

    df = pd.DataFrame.from_records(records)
    if df.empty:
        LOG.info("No deals to write after normalization")
        return {"written": 0}

    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["closed_at"] = pd.to_datetime(df["closed_at"], utc=True, errors="coerce")
    df["last_modified_at"] = pd.to_datetime(
        df["last_modified_at"], utc=True, errors="coerce"
    )

    # Convert ms to timestamps
    for col in [
        "op_detected_ms",
        "proposal_prep_ms",
        "proposal_sent_ms",
        "closed_won_ms",
        "closed_lost_ms",
    ]:
        out_col = col.replace("_ms", "_at")
        df[out_col] = pd.to_datetime(df[col], unit="ms", utc=True, errors="coerce")

    df["dt"] = df["created_at"].dt.strftime("%Y-%m-%d")

    # Attach first associated company_id if present
    if not df.empty:
        df["company_id"] = df["deal_id"].map(
            lambda did: (company_map.get(str(did)) or [None])[0]
        )
        df["contact_id"] = df["deal_id"].map(
            lambda did: (contact_map.get(str(did)) or [None])[0]
        )

    out_cols = [
        "deal_id",
        "deal_name",
        "company_id",
        "contact_id",
        "owner_id",
        "dealstage",
        "created_at",
        "closed_at",
        "last_modified_at",
        "amount",
        "op_detected_at",
        "proposal_prep_at",
        "proposal_sent_at",
        "closed_won_at",
        "closed_lost_at",
        "source_primary",
        "source_secondary",
        "dt",
    ]
    out_df = df[out_cols].copy()

    path = f"s3://{S3_BUCKET}/curated/deals/"
    wr.s3.to_parquet(
        df=out_df,
        path=path,
        dataset=True,
        compression="snappy",
        partition_cols=["dt"],
    )
    LOG.info("Wrote %s raw deal rows to %s", len(out_df), path)
    return {"written": int(len(out_df))}

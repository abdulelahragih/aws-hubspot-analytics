import logging
import os
import time
from typing import Any, Dict, List
import pandas as pd
from hubspot_client import get_client, utc_now_iso, to_epoch_ms
from normalization import map_specific_type, extract_metadata
from storage import ensure_bucket_env
import awswrangler as wr


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


START_DATE = os.environ.get("START_DATE", "2025-01-01")
S3_BUCKET = os.environ.get("S3_BUCKET")


def activities_handler(_event, _context):
    LOG.info("Running activities ingest")
    ensure_bucket_env()

    from_iso = os.environ.get("ACTIVITIES_FROM", START_DATE)
    to_iso = utc_now_iso()

    # Per-object properties mirroring Apps Script
    props_by_obj = {
        "communications": [
            "hs_communication_channel_type",
            "hs_createdate",
            "hs_lastmodifieddate",
            "hubspot_owner_id",
            "hs_body_preview",
            "hs_communication_body",
        ],
        "tasks": [
            "hs_createdate",
            "hs_lastmodifieddate",
            "hubspot_owner_id",
            "hs_task_subject",
            "hs_task_body",
            "hs_task_status",
            "hs_task_type",
        ],
        "calls": [
            "hs_createdate",
            "hs_lastmodifieddate",
            "hubspot_owner_id",
            "hs_call_title",
            "hs_call_body",
            "hs_call_duration",
            "hs_call_outcome",
        ],
        "meetings": [
            "hs_createdate",
            "hs_lastmodifieddate",
            "hubspot_owner_id",
            "hs_meeting_title",
            "hs_meeting_body",
            "hs_meeting_outcome",
        ],
        "emails": [
            "hs_createdate",
            "hs_lastmodifieddate",
            "hubspot_owner_id",
            "hs_email_subject",
            "hs_email_text",
            "hs_email_direction",
        ],
        "notes": [
            "hs_createdate",
            "hs_lastmodifieddate",
            "hubspot_owner_id",
            "hs_note_body",
        ],
    }

    all_rows: List[Dict[str, Any]] = []
    prefer_prop = {
        "emails": "createdate",
        "notes": "createdate",
        "tasks": "createdate",
        "calls": "hs_timestamp",
        "meetings": "hs_timestamp",
        "communications": "hs_timestamp",
    }

    client = get_client()
    for obj in ["emails", "calls", "meetings", "tasks", "notes", "communications"]:
        try:
            primary = prefer_prop.get(obj, "hs_timestamp")
            fallback = "createdate" if primary != "createdate" else "hs_timestamp"
            res = client.search_between(
                object_type=obj,
                properties=props_by_obj[obj],
                from_iso=from_iso,
                to_iso=to_iso,
                page_limit=100,
                primary_prop=primary,
                fallback_prop=fallback,
            )
            # Convert each result to the engagement-like shape used in Apps Script
            converted = []
            for obj_row in res:
                props = obj_row.get("properties", {})
                created_raw = (
                    props.get("hs_createdate")
                    or props.get("createdate")
                    or props.get("hs_timestamp")
                )
                # normalize to epoch ms
                created_ms = None
                if isinstance(created_raw, (int, float)):
                    created_ms = int(created_raw)
                elif isinstance(created_raw, str):
                    if created_raw.isdigit():
                        created_ms = int(created_raw)
                    else:
                        # assume ISO string
                        created_ms = to_epoch_ms(created_raw)

                if obj == "communications":
                    type_field = props.get(
                        "hs_communication_channel_type"
                    ) or props.get("hs_communications_channel")
                elif obj == "emails":
                    type_field = props.get("hs_email_direction")
                else:
                    type_field = None

                activity_type = map_specific_type(
                    type_field,
                    {
                        "emails": "EMAIL",
                        "calls": "CALL",
                        "meetings": "MEETING",
                        "tasks": "TASK",
                        "notes": "NOTE",
                        "communications": "NOTE",
                    }[obj],
                )

                converted.append(
                    {
                        "engagement": {
                            "id": obj_row.get("id"),
                            "type": activity_type,
                            "createdAt": int(created_ms) if created_ms else None,
                            "lastModified": (
                                lambda rm: (
                                    int(rm)
                                    if isinstance(rm, (int, float))
                                    or (isinstance(rm, str) and rm.isdigit())
                                    else (
                                        to_epoch_ms(rm)
                                        if isinstance(rm, str)
                                        else (created_ms or 0)
                                    )
                                )
                            )(props.get("hs_lastmodifieddate")),
                            "ownerId": props.get("hubspot_owner_id") or None,
                        },
                        "metadata": extract_metadata(props, obj),
                        "associations": {"contactIds": [], "companyIds": []},
                    }
                )

            all_rows.extend(converted)
            LOG.info("Fetched %s items for %s", len(res), obj)
        except Exception as e:
            LOG.warning("%s fetch failed: %s", obj, e)
        time.sleep(0.25)

    # Fallback: If CRM v3 returned no data, try V1 engagements endpoints
    if not all_rows:
        LOG.info("No CRM v3 activities found; attempting V1 fallback")

        def _v1_convert(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            out: List[Dict[str, Any]] = []
            for it in items:
                eng = it.get("engagement", {})
                if not eng:
                    continue
                created_ms = eng.get("createdAt")
                try:
                    created_ms = int(created_ms) if created_ms is not None else None
                except Exception:
                    created_ms = None
                last_ms = eng.get("lastModified") or created_ms or 0
                try:
                    last_ms = int(last_ms)
                except Exception:
                    last_ms = 0
                out.append(
                    {
                        "engagement": {
                            "id": eng.get("id"),
                            "type": eng.get("type"),
                            "createdAt": created_ms,
                            "lastModified": last_ms,
                            "ownerId": eng.get("ownerId"),
                        },
                        "metadata": it.get("metadata") or {},
                        "associations": {"contactIds": [], "companyIds": []},
                    }
                )
            return out

        start_ms = to_epoch_ms(from_iso)
        end_ms = to_epoch_ms(to_iso)
        now_ms = int(time.time() * 1000)
        thirty_days_ago_ms = now_ms - 30 * 24 * 60 * 60 * 1000
        use_recent = start_ms >= thirty_days_ago_ms

        v1_items: List[Dict[str, Any]] = []
        try:
            if use_recent:
                LOG.info("V1 endpoint: /recent/modified")
                # Paginate recent/modified
                offset = None
                while True:
                    params: Dict[str, Any] = {"count": 100, "since": start_ms}
                    if offset:
                        params["offset"] = offset
                    data = client.request(
                        method="GET",
                        endpoint="/engagements/v1/engagements/recent/modified",
                        params=params,
                        timeout=60,
                    )
                    results = data.get("results", [])
                    # Filter by createdAt range
                    for r in results:
                        ts = r.get("engagement", {}).get("createdAt")
                        if isinstance(ts, str) and ts.isdigit():
                            ts = int(ts)
                        if isinstance(ts, (int, float)) and start_ms <= ts <= end_ms:
                            v1_items.append(r)
                    if not data.get("hasMore"):
                        break
                    offset = data.get("offset")
            else:
                LOG.info("V1 endpoint: /paged")
                # Paginate paged
                offset = 0
                limit = 250
                requests_without_inrange = 0
                while True:
                    params = {"limit": limit, "offset": offset}
                    data = client.request(
                        method="GET",
                        endpoint="/engagements/v1/engagements/paged",
                        params=params,
                        timeout=60,
                    )
                    results = data.get("results", [])
                    inrange = 0
                    for r in results:
                        ts = r.get("engagement", {}).get("createdAt")
                        if isinstance(ts, str) and ts.isdigit():
                            ts = int(ts)
                        if isinstance(ts, (int, float)) and start_ms <= ts <= end_ms:
                            v1_items.append(r)
                            inrange += 1
                    if inrange == 0:
                        requests_without_inrange += 1
                    else:
                        requests_without_inrange = 0
                    if not data.get("hasMore"):
                        break
                    offset = data.get("offset")
                    # safety stop
                    if requests_without_inrange >= 50:
                        break
        except Exception as e:
            LOG.warning("V1 fallback failed: %s", e)

        if v1_items:
            converted_v1 = _v1_convert(v1_items)
            all_rows.extend(converted_v1)
            LOG.info("V1 fallback fetched %s activities", len(converted_v1))

    if not all_rows:
        LOG.info("No activities to write")
        return {"written": 0}

    # Convert to a flat, analytics-friendly schema
    df = pd.DataFrame.from_records(all_rows)
    if df.empty:
        LOG.info("No activities to write after normalization")
        return {"written": 0}

    # Extract core fields from nested 'engagement'
    def _get_nested(dct, key):
        return dct.get(key) if isinstance(dct, dict) else None

    df["activity_id"] = df["engagement"].apply(lambda e: _get_nested(e, "id"))
    df["activity_type"] = df["engagement"].apply(lambda e: _get_nested(e, "type"))
    df["owner_id"] = df["engagement"].apply(lambda e: _get_nested(e, "ownerId"))
    df["occurred_at_ms"] = df["engagement"].apply(lambda e: _get_nested(e, "createdAt"))
    df["last_modified_ms"] = df["engagement"].apply(
        lambda e: _get_nested(e, "lastModified")
    )

    # Parse timestamps and derive partition column
    df["occurred_at"] = pd.to_datetime(
        df["occurred_at_ms"], unit="ms", errors="coerce", utc=True
    )
    df["last_modified_at"] = pd.to_datetime(
        df["last_modified_ms"], unit="ms", errors="coerce", utc=True
    )
    df["dt"] = df["occurred_at"].dt.strftime("%Y-%m-%d")

    # Pull a few metadata fields needed for downstream categorization
    def _get_meta(dct, key):
        return dct.get(key) if isinstance(dct, dict) else None

    df["email_direction"] = df["metadata"].apply(lambda m: _get_meta(m, "direction"))

    # Select a compact set of columns for Parquet
    out_cols = [
        "activity_id",
        "activity_type",
        "owner_id",
        "occurred_at",
        "last_modified_at",
        "email_direction",
        "dt",
    ]
    out_df = df[out_cols].copy()

    # Drop rows where occurred_at could not be parsed
    before = len(out_df)
    out_df = out_df[out_df["occurred_at"].notna()]
    LOG.info(
        "Filtered %s rows without occurred_at (kept %s)",
        before - len(out_df),
        len(out_df),
    )

    path = f"s3://{S3_BUCKET}/curated/activities/"
    wr.s3.to_parquet(
        df=out_df,
        path=path,
        dataset=True,
        compression="snappy",
        partition_cols=["dt"],
    )
    LOG.info("Wrote %s activity rows to %s", len(out_df), path)
    return {"written": int(len(out_df))}

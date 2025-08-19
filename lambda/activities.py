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
    common_props = ["hs_createdate", "hs_lastmodifieddate", "hubspot_owner_id"]
    activity_props = {
        "communications": [
            "hs_communication_channel_type",
            "hs_body_preview",
            "hs_communication_body",
        ],
        "tasks": [
            "hs_task_subject",
            "hs_task_body",
            "hs_task_status",
            "hs_task_type",
        ],
        "calls": [
            "hs_call_title",
            "hs_call_body",
            "hs_call_duration",
            "hs_call_outcome",
        ],
        "meetings": [
            "hs_meeting_title",
            "hs_meeting_body",
            "hs_meeting_outcome",
        ],
        "emails": [
            "hs_email_subject",
            "hs_email_direction",
            "hs_email_headers"
        ],
        "notes": [
            "hs_note_body",
        ],
    }

    all_rows: List[Dict[str, Any]] = []

    client = get_client()
    for obj in ["emails", "calls", "meetings", "tasks", "notes", "communications"]:
        try:
            LOG.info(f"Fetching {obj} from {from_iso} to {to_iso}")
            res = client.search_between_chunked(
                object_type=obj,
                properties=common_props + activity_props.get(obj, []),
                from_iso=from_iso,
                to_iso=to_iso,
                search_prop="hs_createdate",
                sort_direction="DESCENDING",
            )

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
                    type_value = props.get("hs_communication_channel_type")
                elif obj == "emails":
                    type_value = props.get("hs_email_direction")
                else:
                    type_value = None

                activity_type = map_specific_type(
                    type_value,
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

    if not all_rows:
        LOG.info("No activities to write")
        return {"written": 0}

    # Convert to a flat, analytics-friendly schema
    LOG.info("Example activity row: %s", all_rows[0] if all_rows else "None")
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

    def _get_meta(dct, key):
        return dct.get(key) if isinstance(dct, dict) else None

    # Email metadata for sophisticated direction detection
    df["email_direction"] = df["metadata"].apply(lambda m: _get_meta(m, "direction"))
    df["email_sent"] = df["metadata"].apply(lambda m: _get_meta(m, "sent"))
    df["email_subject"] = df["metadata"].apply(lambda m: _get_meta(m, "subject"))

    # Note metadata for LinkedIn/WhatsApp detection
    df["note_subject"] = df["metadata"].apply(lambda m: _get_meta(m, "subject"))
    df["note_body"] = df["metadata"].apply(lambda m: _get_meta(m, "body"))

    # Communication metadata for source-based mapping
    df["communication_source"] = df["metadata"].apply(lambda m: _get_meta(m, "source"))
    df["communication_body"] = df["metadata"].apply(lambda m: _get_meta(m, "body"))

    # Task/Call/Meeting metadata
    df["task_subject"] = df["metadata"].apply(lambda m: _get_meta(m, "subject"))
    df["call_title"] = df["metadata"].apply(lambda m: _get_meta(m, "subject"))
    df["meeting_title"] = df["metadata"].apply(lambda m: _get_meta(m, "subject"))

    # Select columns for Parquet (expanded for Google Apps Script-style mapping)
    out_cols = [
        "activity_id",
        "activity_type",
        "owner_id",
        "occurred_at",
        "last_modified_at",
        "dt",
        # Email metadata for sophisticated direction detection
        "email_direction",
        "email_sent",
        "email_subject",
        # Note metadata for LinkedIn/WhatsApp detection
        "note_subject",
        "note_body",
        # Communication metadata for source-based mapping
        "communication_source",
        "communication_body",
        # Other activity metadata
        "task_subject",
        "call_title",
        "meeting_title",
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

import logging
import os
import time
from typing import Any, Dict, List
import pandas as pd
from hubspot_client import get_client
from helpers.normalization import map_specific_type, extract_metadata
from helpers.storage import ensure_bucket_env
from helpers.utils import utc_now_iso, _parse_hs_datetime
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

    activities: List[Dict[str, Any]] = []

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
                        "activity_id": obj_row.get("id"),
                        "activity_type": activity_type,
                        "owner_id": props.get("hubspot_owner_id") or None,
                        "created_at": _parse_hs_datetime(props.get("hs_createdate")),
                        "last_modified_at": _parse_hs_datetime(
                            props.get("hs_lastmodifieddate") or props.get("hs_createdate")),
                        **extract_metadata(props, obj),
                    }
                )

            activities.extend(converted)
            LOG.info("Fetched %s items for %s", len(res), obj)
        except Exception as e:
            LOG.warning("%s fetch failed: %s", obj, e)
        time.sleep(0.25)

    if not activities:
        LOG.info("No activities to write")
        return {"written": 0}

    df = pd.DataFrame.from_records(activities)
    if df.empty:
        LOG.info("No activities to write after normalization")
        return {"written": 0}

    # Extract core fields from nested 'engagement'
    def _get_nested(dct, key):
        return dct.get(key) if isinstance(dct, dict) else None

    def _get_meta(dct, key):
        return dct.get(key) if isinstance(dct, dict) else None

    # Note metadata for LinkedIn/WhatsApp detection

    # Drop rows where created_at could not be parsed
    before = len(df)
    out_df = df[df["created_at"].notna()]
    LOG.info(
        "Filtered %s rows without created_at (kept %s)",
        before - len(out_df),
        len(out_df),
    )

    out_df["dt"] = df["created_at"].dt.strftime("%Y-%m-%d")
    before = len(out_df)
    out_df = out_df.drop_duplicates(
        keep="last"
    )
    LOG.info(
        "Dropped %s duplicate rows (kept %s)",
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
        mode="overwrite_partitions",
    )
    LOG.info("Wrote %s activity rows to %s", len(out_df), path)
    return {"written": int(len(out_df))}

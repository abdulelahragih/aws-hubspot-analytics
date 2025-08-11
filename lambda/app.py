import os
import json
import logging
import time
from datetime import datetime, timezone
import requests
import boto3
import pandas as pd
import awswrangler as wr

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET")
HUBSPOT_SECRET_ARN = os.environ.get("HUBSPOT_SECRET_ARN")
START_DATE = os.environ.get("START_DATE", "2024-01-01")

HS_BASE = "https://api.hubapi.com"


def _get_hubspot_token() -> str:
    token = os.environ.get("HUBSPOT_TOKEN")
    if token:
        return token
    if not HUBSPOT_SECRET_ARN:
        raise RuntimeError("HUBSPOT_SECRET_ARN not set and no HUBSPOT_TOKEN present")
    sm = boto3.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=HUBSPOT_SECRET_ARN)
    # Secret may be a plain string or a JSON map
    if "SecretString" in resp:
        val = resp["SecretString"]
        try:
            obj = json.loads(val)
            return obj.get("HUBSPOT_TOKEN") or obj.get("token") or val
        except Exception:
            return val
    raise RuntimeError("Secret binary not supported for HUBSPOT token")


def _hs_headers():
    token = _get_hubspot_token()
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_epoch_ms(iso_str: str) -> int:
    # Accept YYYY-MM-DD or ISO8601
    try:
        if len(iso_str) == 10:
            dt = datetime.strptime(iso_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        # Fallback to now
        return int(datetime.now(timezone.utc).timestamp() * 1000)


def _post_with_retry(url: str, headers: dict, payload: dict, max_retries: int = 5, base_sleep: float = 0.5):
    for attempt in range(max_retries + 1):
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
        if r.status_code == 429 and attempt < max_retries:
            time.sleep(base_sleep * (2 ** attempt))
            continue
        return r
    return r


def hs_search(
    object_type: str,
    properties: list[str],
    from_iso: str,
    to_iso: str,
    page_limit: int = 100,
    primary_prop: str = "hs_timestamp",
    fallback_prop: str = "createdate",
) -> list[dict]:
    """Generic HubSpot CRM v3 search with BETWEEN filter and 429 backoff, 400 fallback."""
    url = f"{HS_BASE}/crm/v3/objects/{object_type}/search"
    headers = _hs_headers()

    frm = _to_epoch_ms(from_iso)
    to = _to_epoch_ms(to_iso)

    out: list[dict] = []
    after = None
    prop = primary_prop
    used_fallback = False
    while True:
        payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": prop,
                            "operator": "BETWEEN",
                            "value": str(frm),
                            "highValue": str(to),
                        }
                    ]
                }
            ],
            "sorts": [{"propertyName": prop, "direction": "ASCENDING"}],
            "properties": properties,
            "limit": page_limit,
        }
        if after:
            payload["after"] = after
        r = _post_with_retry(url, headers, payload)
        if r.status_code == 400:
            if not used_fallback and fallback_prop and prop != fallback_prop:
                # switch to fallback property and retry this page
                prop = fallback_prop
                used_fallback = True
                continue
            r.raise_for_status()
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    return out


def normalize_activities(object_type: str, rows: list[dict]) -> list[dict]:
    mapped: list[dict] = []
    for it in rows:
        props = it.get("properties", {})
        occurred = props.get("hs_timestamp") or props.get("createdate") or props.get("hs_createdate")
        owner_id = props.get("hubspot_owner_id") or props.get("hs_created_by")
        # Subtype for communications if available
        channel = props.get("hs_communications_channel") or props.get("hs_channel")

        if object_type == "emails":
            type_name = "Email"
        elif object_type == "calls":
            type_name = "Call"
        elif object_type == "meetings":
            type_name = "Meeting"
        elif object_type == "tasks":
            type_name = "Task"
        elif object_type == "notes":
            type_name = "Note"
        elif object_type == "communications":
            # Map channels when present
            if channel:
                ch = channel.lower()
                if "linkedin" in ch:
                    type_name = "LinkedIn Message"
                elif "whatsapp" in ch:
                    type_name = "WhatsApp"
                elif "sms" in ch:
                    type_name = "SMS"
                else:
                    type_name = "Communication"
            else:
                type_name = "Communication"
        else:
            type_name = object_type.capitalize()

        mapped.append(
            {
                "activity_id": it.get("id"),
                "activity_type": type_name,
                "object_type": object_type,
                "channel": channel,
                "owner_id": owner_id,
                "occurred_at": occurred,
            }
        )
    return mapped


def fetch_deals_sample(limit: int = 25) -> list[dict]:
    url = f"{HS_BASE}/crm/v3/objects/deals?limit={limit}&properties=dealname,hubspot_owner_id,amount,createdate,dealstage"
    r = requests.get(url, headers=_hs_headers(), timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("results", [])


def normalize_deals(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    records = []
    for d in rows:
        p = d.get("properties", {})
        records.append(
            {
                "deal_id": d.get("id"),
                "deal_name": p.get("dealname"),
                "owner_id": p.get("hubspot_owner_id"),
                "amount": float(p.get("amount")) if p.get("amount") else None,
                "created_at": p.get("createdate"),
                "dealstage": p.get("dealstage"),
            }
        )
    df = pd.DataFrame.from_records(records)
    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    return df


def write_parquet(df: pd.DataFrame, table: str) -> int:
    if df.empty:
        LOG.info("No rows to write for table=%s", table)
        return 0
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


def deals_handler(event, context):
    LOG.info("Running deals ingest")
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET not set")
    deals = fetch_deals_sample(limit=25)
    df = normalize_deals(deals)
    written = write_parquet(df, table="deals")
    return {"written": int(written)}


def activities_handler(event, context):
    LOG.info("Running activities ingest")
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET not set")

    from_iso = os.environ.get("ACTIVITIES_FROM", START_DATE)
    to_iso = _utc_now_iso()

    # Minimal property set common across objects
    common_props = [
        "hs_timestamp",
        "createdate",
        "hubspot_owner_id",
        "hs_communications_channel",
    ]

    all_rows: list[dict] = []
    # Prefer property per object; fallback will be tried automatically on 400
    prefer_prop = {
        "emails": "createdate",
        "notes": "createdate",
        "tasks": "createdate",
        "calls": "hs_timestamp",
        "meetings": "hs_timestamp",
        "communications": "hs_timestamp",
    }

    for obj in ["emails", "calls", "meetings", "tasks", "notes", "communications"]:
        try:
            primary = prefer_prop.get(obj, "hs_timestamp")
            fallback = "createdate" if primary != "createdate" else "hs_timestamp"
            res = hs_search(
                obj,
                common_props,
                from_iso,
                to_iso,
                primary_prop=primary,
                fallback_prop=fallback,
            )
            all_rows.extend(normalize_activities(obj, res))
            LOG.info("Fetched %s items for %s", len(res), obj)
        except Exception as e:
            LOG.warning("%s fetch failed: %s", obj, e)
        time.sleep(0.25)

    if not all_rows:
        LOG.info("No activities to write")
        return {"written": 0}

    df = pd.DataFrame.from_records(all_rows)
    if not df.empty:
        df["occurred_at"] = pd.to_datetime(df["occurred_at"], errors="coerce", utc=True)
        df["dt"] = df["occurred_at"].dt.strftime("%Y-%m-%d")
    path = f"s3://{S3_BUCKET}/curated/activities/"
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        compression="snappy",
        partition_cols=["dt"],
    )
    LOG.info("Wrote %s activity rows to %s", len(df), path)
    return {"written": int(len(df))}


def handler(event, context):
    """Dispatcher entrypoint. Selects task based on TASK env var.

    This allows multiple Lambda functions to reuse the same image
    and choose behavior via environment variable.
    """
    task = os.environ.get("TASK", "deals").lower()
    if task == "deals":
        return deals_handler(event, context)
    if task == "activities":
        return activities_handler(event, context)
    LOG.warning("Unknown TASK '%s' — defaulting to deals", task)
    return deals_handler(event, context)



import logging
import os
from typing import Any, Dict, List

import awswrangler as wr
import pandas as pd

from helpers.utils import _parse_hs_datetime
from hubspot_client import get_client
from helpers.utils import utc_now_iso
from helpers.storage import ensure_bucket_env


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

START_DATE = os.environ.get("START_DATE", "2024-01-01")
S3_BUCKET = os.environ.get("S3_BUCKET")


def _to_epoch_ms(value: Any) -> int | None:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value)
        s = str(value)
        if s.isdigit():
            return int(s)
        # ISO string parse via pandas
        ts = pd.to_datetime(s, utc=True, errors="coerce")
        if pd.isna(ts):
            return None
        return int(ts.value // 10**6)
    except Exception:
        return None


def contacts_handler(_event, _context):
    LOG.info("Running contacts ingest")
    ensure_bucket_env()

    from_iso = os.environ.get("CONTACTS_FROM", START_DATE)
    to_iso = utc_now_iso()

    client = get_client()
    props = [
        "hubspot_owner_id",
        "createdate",
        "lastmodifieddate",
        "hs_object_id",
        "firstname",
        "lastname",
        "email",
    ]

    # Fetch by createdate
    created_rows: List[Dict[str, Any]] = client.search_between_chunked(
        object_type="contacts",
        properties=props,
        from_iso=from_iso,
        to_iso=to_iso,
        search_prop="createdate",
    )

    # Fetch by lastmodifieddate to capture older contacts worked recently
    modified_rows: List[Dict[str, Any]] = client.search_between_chunked(
        object_type="contacts",
        properties=props,
        from_iso=from_iso,
        to_iso=to_iso,
        search_prop="lastmodifieddate",
    )

    by_id: Dict[str, Dict[str, Any]] = {}
    for row in created_rows + modified_rows:
        cid = row.get("id")
        p = row.get("properties", {})
        if not cid:
            continue
        cur = by_id.get(cid, {})
        # merge, prefer existing if present
        cur_props = cur.get("properties", {})
        cur_props.update({k: v for k, v in p.items() if v is not None})
        cur["id"] = cid
        cur["properties"] = cur_props
        by_id[cid] = cur

    if not by_id:
        LOG.info("No contacts to write")
        return {"written": 0}

    records: List[Dict[str, Any]] = []
    for cid, row in by_id.items():
        props = row.get("properties", {})
        created_ms = _to_epoch_ms(props.get("createdate"))
        modified_ms = _to_epoch_ms(props.get("lastmodifieddate"))
        records.append(
            {
                "contact_id": cid,
                "owner_id": props.get("hubspot_owner_id"),
                "created_at_ms": created_ms,
                "last_modified_ms": modified_ms,
            }
        )

    df = pd.DataFrame.from_records(records)
    if df.empty:
        LOG.info("No contacts to write after normalization")
        return {"written": 0}

    df["created_at"] = pd.to_datetime(
        df["created_at_ms"], unit="ms", utc=True, errors="coerce"
    )
    df["last_modified_at"] = pd.to_datetime(
        df["last_modified_ms"], unit="ms", utc=True, errors="coerce"
    )
    # Partition by date(coalesce(created,last_modified))
    df["dt"] = df["created_at"].fillna(df["last_modified_at"]).dt.strftime("%Y-%m-%d")

    out_cols = ["contact_id", "owner_id", "created_at", "last_modified_at", "dt"]
    out_df = df[out_cols].copy()

    path = f"s3://{S3_BUCKET}/curated/contacts/"
    wr.s3.to_parquet(
        df=out_df,
        path=path,
        dataset=True,
        compression="snappy",
        partition_cols=["dt"],
    )
    LOG.info("Wrote %s contact rows to %s", len(out_df), path)
    return {"written": int(len(out_df))}


def contacts_dim_handler(_event, _context):
    """Ingest contacts as a dimension table with basic attributes.

    Output: s3://{bucket}/dim/contacts/ as a single, non-partitioned snapshot (overwrite each run)
    Columns: contact_id, owner_id, firstname, lastname, email, created_at, last_modified_at
    """
    LOG.info("Running contacts dim ingest")
    ensure_bucket_env()

    client = get_client()
    props = [
        "hubspot_owner_id",
        "createdate",
        "lastmodifieddate",
        "hs_object_id",
        "firstname",
        "lastname",
        "email",
    ]

    # Full scan via GET /crm/v3/objects/contacts with pagination
    contacts: List[Dict[str, Any]] = client.paginated_request(
        method="GET",
        endpoint="/crm/v3/objects/contacts",
        params={
            "properties": ",".join(props),
            "limit": 100,
            "archived": "false",
        },
        result_key="results",
    )

    if not contacts:
        LOG.info("No contacts to write for dim")
        return {"written": 0}

    recs: List[Dict[str, Any]] = []
    for contact in contacts:
        p = contact.get("properties", {})
        recs.append(
            {
                "contact_id": contact.get("id"),
                "owner_id": p.get("hubspot_owner_id"),
                "firstname": p.get("firstname"),
                "lastname": p.get("lastname"),
                "email": p.get("email"),
                "created_at": _parse_hs_datetime(p.get("createdate")),
                "last_modified_at": _parse_hs_datetime(p.get("lastmodifieddate")),
            }
        )

    df = pd.DataFrame.from_records(recs)
    if df.empty:
        LOG.info("No contacts to write after normalization for dim")
        return {"written": 0}

    df["dt"] = df["created_at"].dt.strftime("%Y-%m-%d")
    path = f"s3://{S3_BUCKET}/dim/contacts/"
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        compression="snappy",
        partition_cols=["dt"],
        mode="overwrite_partitions",
    )
    LOG.info("Wrote %s contacts to %s", len(df), path)
    return {"written": int(len(df))}

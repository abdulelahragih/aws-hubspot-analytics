import logging
import os
from typing import Any, Dict, List

import pandas as pd

from helpers.utils import parse_hs_datetime, utc_now_iso
from hubspot_client import get_client
from helpers.storage import ensure_bucket_env
from helpers.sync_state import get_sync_manager

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET")

def contacts_handler(_event, _context):
    """Ingest contacts as a dimension table with basic attributes.
    Output: s3://{bucket}/dim/contacts/
    Columns: contact_id, owner_id, firstname, lastname, email, created_at, last_modified_at
    """
    LOG.info("Running contacts dim ingest")
    ensure_bucket_env()

    client = get_client()
    sync_manager = get_sync_manager()

    # Determine sync strategy and date range
    sync_state = sync_manager.get_sync_dates("contacts")
    created_from_date = sync_state.new_records_checkpoint.isoformat() if sync_state.new_records_checkpoint else utc_now_iso()
    modified_from_date = sync_state.modified_records_check_point.isoformat() if sync_state.modified_records_check_point else utc_now_iso()
    to_date = utc_now_iso()

    props = [
        "hubspot_owner_id",
        "createdate",
        "lastmodifieddate",
        "hs_object_id",
        "firstname",
        "lastname",
        "email",
    ]

    if sync_state.is_incremental_sync_enabled:
        # Incremental sync using search API
        LOG.info(f"Performing incremental sync from new:{created_from_date} modified:{modified_from_date} to {to_date}")

        # Dual-fetch strategy: get both created and modified contacts
        created_contacts: List[Dict[str, Any]] = client.search_between_chunked(
            object_type="contacts",
            properties=props,
            from_iso=created_from_date,
            to_iso=to_date,
            search_prop="createdate"
        )

        modified_contacts: List[Dict[str, Any]] = client.search_between_chunked(
            object_type="contacts",
            properties=props,
            from_iso=modified_from_date,
            to_iso=to_date,
            search_prop="lastmodifieddate"
        )

        # Merge and deduplicate by contact ID (keep the most recent version)
        contacts_by_id: Dict[str, Dict[str, Any]] = {}
        for contact in created_contacts + modified_contacts:
            contact_id = contact.get("id")
            if contact_id:
                contacts_by_id[contact_id] = contact

        contacts = list(contacts_by_id.values())
        LOG.info(
            f"Fetched {len(created_contacts)} created and {len(modified_contacts)} modified contacts, deduplicated to {len(contacts)} unique contacts"
        )
    else:
        # Full scan via GET /crm/v3/objects/contacts with pagination
        LOG.info("Performing full sync")
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
        sync_manager.update_sync_state("contacts", records_processed=0)
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
                "created_at": parse_hs_datetime(p.get("createdate")),
                "last_modified_at": parse_hs_datetime(p.get("lastmodifieddate")),
            }
        )

    df = pd.DataFrame.from_records(recs)

    # Extract date bounds for sync state tracking
    max_created, max_modified = sync_manager.extract_date_bounds_from_data(df)

    df["dt"] = df["created_at"].dt.strftime("%Y-%m-%d")
    path = f"s3://{S3_BUCKET}/dim/contacts/"

    # Use the reusable merge strategy from sync_state manager
    sync_manager.write_with_merge_strategy(
        df=df,
        s3_path=path,
        partition_cols=["dt"],
        primary_key_col="contact_id"
    )

    sync_manager.update_sync_state(
        "contacts_dim",
        last_created_at=max_created,
        last_modified_at=max_modified,
        records_processed=len(df),
    )
    LOG.info("Wrote %s contacts to %s", len(df), path)
    return {"written": int(len(df))}

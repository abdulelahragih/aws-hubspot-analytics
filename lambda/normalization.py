from typing import Any, Dict, List

import pandas as pd


def normalize_deals(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    records = []
    for deal in rows:
        props = deal.get("properties", {})
        records.append(
            {
                "deal_id": deal.get("id"),
                "deal_name": props.get("dealname"),
                "owner_id": props.get("hubspot_owner_id"),
                "amount": float(props.get("amount")) if props.get("amount") else None,
                "created_at": props.get("createdate"),
                "dealstage": props.get("dealstage"),
            }
        )
    df = pd.DataFrame.from_records(records)
    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    return df


def normalize_activities(
    object_type: str, rows: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    mapped: List[Dict[str, Any]] = []
    for item in rows:
        props = item.get("properties", {})
        occurred = (
            props.get("hs_timestamp")
            or props.get("createdate")
            or props.get("hs_createdate")
        )
        owner_id = props.get("hubspot_owner_id") or props.get("hs_created_by")
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
            if channel:
                ch = str(channel).lower()
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
                "activity_id": item.get("id"),
                "activity_type": type_name,
                "object_type": object_type,
                "channel": channel,
                "owner_id": owner_id,
                "occurred_at": occurred,
            }
        )
    return mapped


def map_specific_type(value: Any, default_type: str) -> str:
    if not value:
        return default_type
    v = str(value).strip().upper()

    if v == "INCOMING_EMAIL":
        return "INCOMING_EMAIL"
    if v == "FORWARDED_EMAIL":
        return "FORWARDED_EMAIL"

    channel_map = {
        "EMAIL": "EMAIL",
        "INCOMING_EMAIL": "INCOMING_EMAIL",
        "FORWARDED_EMAIL": "FORWARDED_EMAIL",
        "LINKEDIN_MESSAGE": "LINKEDIN_MESSAGE",
        "SMS": "SMS",
        "WHATS_APP": "WHATS_APP",
        "CALL": "CALL",
        "MEETING": "MEETING",
        "TASK": "TASK",
        "NOTE": "NOTE",
    }
    return channel_map.get(v, default_type)


def extract_metadata(props: Dict[str, Any], object_type: str) -> Dict[str, Any]:
    p = props or {}
    if object_type == "communications":
        return {
            "body": p.get("hs_communication_body") or p.get("hs_body_preview") or "",
            "source": "CRM_v3_Communications",
        }
    if object_type == "tasks":
        return {
            "subject": p.get("hs_task_subject") or "",
            "body": p.get("hs_task_body") or "",
            "status": p.get("hs_task_status") or "",
        }
    if object_type == "calls":
        return {
            "subject": p.get("hs_call_title") or "",
            "body": p.get("hs_call_body") or "",
            "duration": p.get("hs_call_duration") or "",
            "outcome": p.get("hs_call_outcome") or "",
        }
    if object_type == "meetings":
        return {
            "subject": p.get("hs_meeting_title") or "",
            "body": p.get("hs_meeting_body") or "",
            "outcome": p.get("hs_meeting_outcome") or "",
        }
    if object_type == "emails":
        return {
            "subject": p.get("hs_email_subject") or "",
            "body": p.get("hs_email_text") or "",
            "direction": p.get("hs_email_direction") or "",
        }
    if object_type == "notes":
        return {"body": p.get("hs_note_body") or ""}
    return {"body": ""}

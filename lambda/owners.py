import logging
from typing import Dict

from hubspot_client import get_client


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


def get_owners() -> Dict[str, Dict[str, str]]:
    """
    Helper function to get owners from HubSpot.
    Returns: dict[str, dict] -> { owner_id: {"name": ..., "email": ...}, ... }
    """
    hub_client = get_client()

    try:
        result = hub_client.paginated_request(
            "GET",
            "/crm/v3/owners",
            params={"limit": 100},
        )
        LOG.info(f"Found {len(result)} owners.")

        owners_map = {
            str(o.get("id")): {
                "name": f"{o.get('firstName') or ''} {o.get('lastName') or ''}".strip()
                or "Unknown",
                "email": o.get("email") or "No email",
            }
            for o in result
            if o.get("id", False)
        }
        return owners_map
    except Exception as e:
        LOG.error(f"Error getting owners: {e}")
        raise RuntimeError(f"Error getting owners: {e}") from e

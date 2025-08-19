import os
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from utils import parse_iso_utc

import boto3
import requests

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

HS_BASE_URL = "https://api.hubapi.com"
HUBSPOT_SECRET_ARN = os.environ.get("HUBSPOT_SECRET_ARN")
TOKEN_TTL_SECONDS = int(os.environ.get("HUBSPOT_TOKEN_TTL_SECONDS", "300"))
_CACHED_TOKEN: Optional[str] = None
_CACHED_AT: float = 0.0


def _get_hubspot_token() -> str:
    global _CACHED_TOKEN, _CACHED_AT
    env_token = os.environ.get("HUBSPOT_TOKEN")
    if env_token:
        return env_token

    now = time.time()
    if _CACHED_TOKEN and (now - _CACHED_AT) < TOKEN_TTL_SECONDS:
        return _CACHED_TOKEN

    if not HUBSPOT_SECRET_ARN:
        raise RuntimeError("HUBSPOT_SECRET_ARN not set and no HUBSPOT_TOKEN present")
    sm = boto3.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=HUBSPOT_SECRET_ARN)
    if "SecretString" in resp:
        val = resp["SecretString"]
        try:
            obj = json.loads(val)
            _CACHED_TOKEN = obj.get("HUBSPOT_TOKEN") or obj.get("token") or val
        except Exception:
            _CACHED_TOKEN = val
        _CACHED_AT = now
        return _CACHED_TOKEN or ""
    raise RuntimeError("Secret binary not supported for HUBSPOT token")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_epoch_ms(iso_str: str) -> int:
    try:
        if len(iso_str) == 10:
            dt = datetime.strptime(iso_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return int(datetime.now(timezone.utc).timestamp() * 1000)


class HubSpotClient:
    def __init__(self, token: Optional[str] = None, rate_limit_pause: float = 0.25):
        self.token = token or _get_hubspot_token()
        if not self.token:
            raise RuntimeError("HUBSPOT_TOKEN is not configured.")
        self.base_url = HS_BASE_URL
        self.session = requests.Session()
        self.rate_limit_pause = rate_limit_pause
        self._last_req_at: float = 0.0

    def request(
            self,
            method: str,
            endpoint: str,
            params: Optional[Dict[str, Any]] = None,
            data: Optional[Any] = None,
            json: Optional[Dict[str, Any]] = None,
            timeout: int = 30,
            **kwargs: Any,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        now = time.time()
        elapsed = now - self._last_req_at
        if elapsed < self.rate_limit_pause:
            time.sleep(self.rate_limit_pause - elapsed)
        resp = self.session.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            data=data,
            json=json,
            timeout=timeout,
            **kwargs,
        )
        self._last_req_at = time.time()
        if not resp.ok:
            raise RuntimeError(f"HubSpot API error {resp.status_code}: {resp.text}")
        return resp.json() if resp.text else {}

    def paginated_request(
            self,
            method: str,
            endpoint: str,
            params: Optional[Dict[str, Any]] = None,
            result_key: str = "results",
    ) -> List[Dict[str, Any]]:
        params = params.copy() if params else {}
        params.setdefault("limit", 100)
        all_results: List[Dict[str, Any]] = []
        after: Optional[str] = None
        while True:
            if after:
                params["after"] = after
            data = self.request(method, endpoint, params=params)
            page = data.get(result_key, [])
            all_results.extend(page)
            next_page = (data.get("paging") or {}).get("next")
            if not next_page:
                break
            after = next_page.get("after")
            time.sleep(self.rate_limit_pause)
        return all_results

    def search_between(
            self,
            object_type: str,
            properties: List[str],
            from_iso: str,
            to_iso: str,
            page_limit: int = 100,
            search_prop: str = "hs_createdate",
            sort_prop: str = None,
            sort_direction: str = "ASCENDING",
    ) -> List[Dict[str, Any]]:
        """Search for objects of a type `object_type` between two ISO timestamps.
        Uses POST /crm/v3/objects/{object_type}/search with a filter for the given
        :param search_prop` between `from_iso` and `to_iso`.
        :param object_type: Type of HubSpot object (e.g., "contacts", "deals").
        :param properties: List of properties to fetch for each object.
        :param from_iso: Start ISO timestamp (inclusive).
        :param to_iso: End ISO timestamp (inclusive).
        :param page_limit: Maximum number of results per page (default 100, max 200).
        :param search_prop: Property to filter on (default "hs_createdate").
        :param sort_prop: Property to sort results by (default same as `search_prop`
        :param sort_direction: Sort direction, either "ASCENDING" or "DESCENDING".
        """
        url = f"/crm/v3/objects/{object_type}/search"
        frm = to_epoch_ms(from_iso)
        to = to_epoch_ms(to_iso)
        out: List[Dict[str, Any]] = []
        after: Optional[str] = None
        sort_prop = sort_prop or search_prop

        # Enforce API max per-page
        page_limit = min(page_limit, 200)

        while True:
            payload: Dict[str, Any] = {
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": search_prop,
                                "operator": "BETWEEN",
                                "value": str(frm),
                                "highValue": str(to),
                            }
                        ]
                    }
                ],
                "sorts": [{"propertyName": sort_prop, "direction": sort_direction}],
                "properties": properties,
                "limit": page_limit,
            }
            if after:
                payload["after"] = after

            # backoff on 429
            base_sleep = 0.5
            max_retries = 5
            data = None
            for attempt in range(max_retries + 1):
                try:
                    response = self.request("POST", url, json=payload, timeout=60)
                    data = response
                    break
                except RuntimeError as e:
                    msg = str(e)
                    if "429" in msg and attempt < max_retries:
                        time.sleep(base_sleep * (2 ** attempt))
                        continue
                    if "10000" in msg or "10,000" in msg:
                        LOG.warning(
                            "Hit 10k results limit for %s; stopping pagination.",
                            object_type,
                        )
                        return out
                    raise
            if not data:
                raise RuntimeError(
                    f"Failed to fetch data for {object_type} after retries"
                )

            out.extend(data.get("results", []))
            # Stop at 10k results cap
            if len(out) >= 10000:
                LOG.warning("Reached 10k results cap for %s", object_type)
                return out
            next_page = (data.get("paging") or {}).get("next")
            if not next_page:
                break
            after = next_page.get("after")
            time.sleep(self.rate_limit_pause)
        return out

    def search_between_chunked(
            self,
            object_type: str,
            properties: list,
            from_iso: str,
            to_iso: str,
            search_prop: str = "hs_createdate",
            sort_direction: str = "ASCENDING",
            max_total_per_chunk: int = 9500,
            max_days: int = 14,
            min_days: int = 1,
    ) -> list:
        """Search for objects of a type `object_type` between two ISO timestamps
        using a chunked approach to avoid hitting API limits.
        This method breaks the date range into smaller chunks to ensure that
        the total number of results does not exceed `max_total_per_chunk`.
        :param object_type: Type of HubSpot object (e.g., "contacts", "deals").
        :param properties: List of properties to fetch for each object.
        :param from_iso: Start ISO timestamp (inclusive).
        :param to_iso: End ISO timestamp (inclusive).
        :param search_prop: Property to filter on (default "hs_createdate").
        :param sort_direction: Sort direction, either "ASCENDING" or "DESCENDING".
        :param max_total_per_chunk: Maximum number of results per chunk (default 9500).
        :param max_days: Maximum number of days to try for each chunk (default 14).
        :param min_days: Minimum number of days to try for each chunk (default 1).
        """
        all_results = []

        def get_total(start, end):
            payload = {
                "filterGroups": [{
                    "filters": [{
                        "propertyName": search_prop,
                        "operator": "BETWEEN",
                        "value": str(int(start.timestamp() * 1000)),
                        "highValue": str(int(end.timestamp() * 1000)),
                    }]
                }],
                "limit": 1,
                "properties": []
            }
            res = self.request(
                method="POST",
                endpoint=f"/crm/v3/objects/{object_type}/search",
                json=payload
            )
            return res.get("total", 0)

        def fetch_chunk(start, end):
            results = []
            after = None

            while True:
                payload = {
                    "filterGroups": [{
                        "filters": [{
                            "propertyName": search_prop,
                            "operator": "BETWEEN",
                            "value": str(int(start.timestamp() * 1000)),
                            "highValue": str(int(end.timestamp() * 1000)),
                        }]
                    }],
                    "properties": properties,
                    "limit": 100,
                    "sorts": [{"propertyName": search_prop, "direction": sort_direction}]
                }
                if after:
                    payload["after"] = after

                res = self.request(
                    method="POST",
                    endpoint=f"/crm/v3/objects/{object_type}/search",
                    json=payload
                )
                results.extend(res.get("results", []))

                paging = res.get("paging", {})
                after = paging.get("next", {}).get("after")
                if not after:
                    break

            return results

        start = parse_iso_utc(from_iso)
        end = parse_iso_utc(to_iso)
        cursor = start

        while cursor < end:
            try_days = max_days
            while try_days >= min_days:
                chunk_end = min(cursor + timedelta(days=try_days), end)
                total = get_total(cursor, chunk_end)
                if total < max_total_per_chunk:
                    LOG.info(f"Fetching {total} results for {object_type} from {cursor.date()} to {chunk_end.date()}")
                    chunk = fetch_chunk(cursor, chunk_end)
                    all_results.extend(chunk)
                    cursor = chunk_end
                    break
                else:
                    LOG.warning(
                        f"Too many results ({total}) for range {cursor.date()} to {chunk_end.date()}, shrinking...")
                    try_days = try_days // 2
            else:
                raise RuntimeError(f"Could not reduce chunk below {min_days} days for {cursor}")

        return all_results

    def batch_read_associations_v4(
            self,
            from_object: str,
            to_object: str,
            from_ids: List[str],
            batch_size: int = 100,
    ) -> Dict[str, List[str]]:
        """Batch-read associations (v4) and return mapping from from_id -> list of to_ids.

        Uses POST /crm/v4/associations/{from}/{to}/batch/read
        """
        association_map: Dict[str, List[str]] = {}
        if not from_ids:
            return association_map
        # Chunk inputs to avoid payload limits
        for i in range(0, len(from_ids), batch_size):
            chunk = [fid for fid in from_ids[i: i + batch_size] if fid]
            if not chunk:
                continue
            try:
                data = self.request(
                    method="POST",
                    endpoint=f"/crm/v4/associations/{from_object}/{to_object}/batch/read",
                    json={"inputs": [{"id": fid} for fid in chunk]},
                    timeout=60,
                )
            except Exception as e:
                # If v4 is unavailable for some accounts, skip gracefully
                LOG.warning(
                    "Association batch read failed for %s -> %s: %s",
                    from_object,
                    to_object,
                    e,
                )
                continue

            results = data.get("results", []) if isinstance(data, dict) else []
            for item in results:
                # Flexible parsing across possible shapes
                frm_obj = item.get("from") if isinstance(item, dict) else None
                frm_id = None
                if isinstance(frm_obj, dict):
                    frm_id = (
                            frm_obj.get("id")
                            or frm_obj.get("objectId")
                            or frm_obj.get("fromObjectId")
                    )
                frm_id = (
                        frm_id
                        or item.get("fromId")
                        or item.get("fromObjectId")
                        or item.get("id")
                )
                if not frm_id:
                    continue
                to_list = item.get("to") or item.get("toObjects") or []
                collected: List[str] = []
                if isinstance(to_list, list):
                    for t in to_list:
                        if isinstance(t, dict):
                            tid = (
                                    t.get("id") or t.get("toObjectId") or t.get("objectId")
                            )
                            if tid:
                                collected.append(str(tid))
                        elif isinstance(t, (str, int)):
                            collected.append(str(t))
                association_map[str(frm_id)] = collected
        return association_map


_CLIENT: Optional[HubSpotClient] = None
_CLIENT_AT: float = 0.0


def get_client() -> HubSpotClient:
    global _CLIENT, _CLIENT_AT
    now = time.time()
    if _CLIENT and (now - _CLIENT_AT) < TOKEN_TTL_SECONDS:
        return _CLIENT
    _CLIENT = HubSpotClient(token=_get_hubspot_token())
    _CLIENT_AT = now
    return _CLIENT

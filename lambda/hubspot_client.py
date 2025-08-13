import os
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
import requests


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

HS_BASE = "https://api.hubapi.com"
HUBSPOT_SECRET_ARN = os.environ.get("HUBSPOT_SECRET_ARN")
TOKEN_TTL_SECONDS = int(
    os.environ.get("HUBSPOT_TOKEN_TTL_SECONDS", "300")
)  # 5 minutes default
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


def hs_headers() -> Dict[str, str]:
    token = _get_hubspot_token()
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


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


def post_with_retry(
    url: str,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    max_retries: int = 5,
    base_sleep: float = 0.5,
):
    for attempt in range(max_retries + 1):
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
        if r.status_code == 429 and attempt < max_retries:
            time.sleep(base_sleep * (2**attempt))
            continue
        return r
    return r


def hs_search(
    object_type: str,
    properties: List[str],
    from_iso: str,
    to_iso: str,
    page_limit: int = 100,
    primary_prop: str = "hs_timestamp",
    fallback_prop: str = "createdate",
) -> List[Dict[str, Any]]:
    url = f"{HS_BASE}/crm/v3/objects/{object_type}/search"
    headers = hs_headers()

    frm = to_epoch_ms(from_iso)
    to = to_epoch_ms(to_iso)

    out: List[Dict[str, Any]] = []
    after: Optional[str] = None
    prop = primary_prop
    used_fallback = False
    while True:
        payload: Dict[str, Any] = {
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
        r = post_with_retry(url, headers, payload)
        if r.status_code == 400:
            if not used_fallback and fallback_prop and prop != fallback_prop:
                prop = fallback_prop
                used_fallback = True
                continue
            r.raise_for_status()
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("results", []))
        paging = data.get("paging", {})
        after = paging.get("next", {}).get("after")
        if not after:
            break
    return out


class HubSpotClient:
    def __init__(self, token: Optional[str] = None, rate_limit_pause: float = 0.25):
        self.token = token or _get_hubspot_token()
        if not self.token:
            raise RuntimeError("HUBSPOT_TOKEN is not configured.")
        self.base_url = HS_BASE
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
        primary_prop: str = "hs_timestamp",
        fallback_prop: str = "createdate",
    ) -> List[Dict[str, Any]]:
        """POST search with BETWEEN filter, 429 backoff and 400 fallback."""
        url = f"/crm/v3/objects/{object_type}/search"
        frm = to_epoch_ms(from_iso)
        to = to_epoch_ms(to_iso)
        out: List[Dict[str, Any]] = []
        after: Optional[str] = None
        prop = primary_prop
        used_fallback = False
        # Enforce API max per-page
        page_limit = min(page_limit, 200)
        while True:
            payload: Dict[str, Any] = {
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

            # backoff on 429
            base_sleep = 0.5
            max_retries = 5
            for attempt in range(max_retries + 1):
                try:
                    r = self.request("POST", url, json=payload, timeout=60)
                    data = r
                    break
                except RuntimeError as e:
                    msg = str(e)
                    if "429" in msg and attempt < max_retries:
                        time.sleep(base_sleep * (2**attempt))
                        continue
                    if (
                        "400" in msg
                        and not used_fallback
                        and fallback_prop
                        and prop != fallback_prop
                    ):
                        prop = fallback_prop
                        used_fallback = True
                        data = None
                        break
                    if "10000" in msg or "10,000" in msg:
                        LOG.warning(
                            "Hit 10k results limit for %s; stopping pagination.",
                            object_type,
                        )
                        return out
                    raise

            if data is None:
                # switched to fallback; retry loop
                continue

            out.extend(data.get("results", []))
            # Stop at 10k results cap
            if len(out) >= 10000:
                LOG.warning(
                    "Reached 10k results cap for %s; truncating results.", object_type
                )
                return out
            next_page = (data.get("paging") or {}).get("next")
            if not next_page:
                break
            after = next_page.get("after")
            time.sleep(self.rate_limit_pause)
        return out

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
            chunk = [fid for fid in from_ids[i : i + batch_size] if fid]
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

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

@dataclass
class CRMConfig:
    base_url: str
    token: str

class CRMClient:
    """
    Generic REST CRM client skeleton.
    Replace endpoints with your CRM endpoints (Zoho/HubSpot/etc.).
    """

    def __init__(self, cfg: CRMConfig, timeout: int = 30):
        self.cfg = cfg
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {cfg.token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        last_exc: Optional[Exception] = None
        for attempt in range(5):
            try:
                resp = self.session.request(method, url, timeout=self.timeout, **kwargs)
                if resp.status_code in (429, 500, 502, 503, 504):
                    time.sleep(1.5 * (attempt + 1))
                    continue
                return resp
            except Exception as e:
                last_exc = e
                time.sleep(1.5 * (attempt + 1))
        raise RuntimeError(f"CRM request failed after retries: {last_exc}")

    def find_by_key(self, unique_key_name: str, unique_key_value: str) -> Optional[Dict[str, Any]]:
        # Example: GET /contacts?email=...
        url = f"{self.cfg.base_url.rstrip('/')}/contacts"
        resp = self._request("GET", url, params={unique_key_name: unique_key_value})
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") if isinstance(data, dict) else data
        if not items:
            return None
        return items[0]

    def update_record(self, record_id: str, payload: Dict[str, Any]) -> None:
        # Example: PATCH /contacts/{id}
        url = f"{self.cfg.base_url.rstrip('/')}/contacts/{record_id}"
        resp = self._request("PATCH", url, json=payload)
        resp.raise_for_status()

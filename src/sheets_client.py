from __future__ import annotations

from dataclasses import dataclass
from typing import List

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

@dataclass
class SheetsConfig:
    service_account_json: str
    spreadsheet_id: str
    range_a1: str

def fetch_sheet_rows(cfg: SheetsConfig) -> List[dict]:
    """Fetch rows from Google Sheets as list of dicts using first row as headers."""
    creds = Credentials.from_service_account_file(
        cfg.service_account_json,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    service = build("sheets", "v4", credentials=creds)

    resp = service.spreadsheets().values().get(
        spreadsheetId=cfg.spreadsheet_id, range=cfg.range_a1
    ).execute()

    values = resp.get("values", [])
    if not values or len(values) < 2:
        return []

    headers = [h.strip() for h in values[0]]
    rows = []
    for line in values[1:]:
        padded = line + [""] * (len(headers) - len(line))
        rows.append({headers[i]: padded[i] for i in range(len(headers))})
    return rows

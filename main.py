from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from dotenv import dotenv_values

from src.crm_client import CRMClient, CRMConfig
from src.mapper import FieldMapping, map_row
from src.sheets_client import SheetsConfig, fetch_sheet_rows

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync Google Sheets rows to CRM via REST API")
    p.add_argument("--dry-run", action="store_true", help="Validate and show what would change")
    p.add_argument("--sync", action="store_true", help="Perform sync")
    p.add_argument("--mapping", default="mapping.csv", help="CSV mapping file: sheet_col,crm_field")
    return p.parse_args()

def load_mapping(path: str) -> FieldMapping:
    mp = pd.read_csv(path)
    sheet_to_crm = dict(zip(mp["sheet_col"].astype(str), mp["crm_field"].astype(str)))
    return FieldMapping(sheet_to_crm=sheet_to_crm)

def main() -> int:
    args = parse_args()
    if not args.dry_run and not args.sync:
        print("Choose one: --dry-run or --sync")
        return 2

    env = dotenv_values(".env")
    sheets_cfg = SheetsConfig(
        service_account_json=env.get("GOOGLE_SERVICE_ACCOUNT_JSON", ""),
        spreadsheet_id=env.get("GOOGLE_SHEETS_SPREADSHEET_ID", ""),
        range_a1=env.get("GOOGLE_SHEETS_RANGE", "Sheet1!A1:Z"),
    )
    crm_cfg = CRMConfig(base_url=env.get("CRM_BASE_URL", ""), token=env.get("CRM_TOKEN", ""))

    unique_key_col = env.get("UNIQUE_KEY_COLUMN", "email")
    log_path = Path(env.get("LOG_PATH", "out/sync_log.csv"))
    log_path.parent.mkdir(parents=True, exist_ok=True)

    if not sheets_cfg.service_account_json or not sheets_cfg.spreadsheet_id:
        print("Missing Google Sheets config. Fill .env")
        return 2
    if not crm_cfg.base_url or not crm_cfg.token:
        print("Missing CRM config. Fill .env")
        return 2

    mapping = load_mapping(args.mapping)
    rows = fetch_sheet_rows(sheets_cfg)
    if not rows:
        print("No rows found.")
        return 0

    crm = CRMClient(crm_cfg)
    logs = []

    for i, row in enumerate(rows, start=2):  # approx sheet row index
        unique_val = str(row.get(unique_key_col, "")).strip().lower()
        if not unique_val:
            logs.append({"sheet_row": i, "status": "skipped", "reason": f"missing_unique_key:{unique_key_col}"})
            continue

        existing = crm.find_by_key(unique_key_col, unique_val)
        if not existing:
            logs.append({"sheet_row": i, "status": "skipped", "reason": "not_found_in_crm"})
            continue

        record_id = str(existing.get("id") or existing.get("record_id") or "").strip()
        if not record_id:
            logs.append({"sheet_row": i, "status": "skipped", "reason": "crm_missing_id"})
            continue

        payload = map_row(row, mapping)

        if args.dry_run:
            logs.append({"sheet_row": i, "status": "dry_run", "reason": "would_update", "record_id": record_id})
            continue

        try:
            crm.update_record(record_id, payload)
            logs.append({"sheet_row": i, "status": "updated", "record_id": record_id})
        except Exception as e:
            logs.append({"sheet_row": i, "status": "error", "reason": str(e), "record_id": record_id})

    pd.DataFrame(logs).to_csv(log_path, index=False)
    print(f"Done. Log saved to: {log_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

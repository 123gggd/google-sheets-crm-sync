from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

@dataclass
class FieldMapping:
    """Maps sheet headers -> CRM payload fields."""
    sheet_to_crm: Dict[str, str]

def map_row(row: Dict[str, str], mapping: FieldMapping) -> Dict[str, str]:
    payload: Dict[str, str] = {}
    for sheet_col, crm_field in mapping.sheet_to_crm.items():
        v = row.get(sheet_col, "")
        payload[crm_field] = str(v or "").strip()
    return payload

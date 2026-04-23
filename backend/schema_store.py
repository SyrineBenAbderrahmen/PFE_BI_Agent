from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any

from config import settings


def _snap_dir() -> Path:
    p = Path(settings.SNAPSHOT_DIR)
    p.mkdir(parents=True, exist_ok=True)
    return p


def schema_latest_path(dw_id: str) -> Path:
    return _snap_dir() / f"{dw_id}_schema_latest.json"


def save_schema_snapshot(dw_id: str, snapshot: Dict[str, Any]) -> Dict[str, Any]:
    latest_path = schema_latest_path(dw_id)
    latest_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    return {"latest": str(latest_path)}


def load_schema_snapshot(dw_id: str) -> Dict[str, Any] | None:
    latest_path = schema_latest_path(dw_id)
    if not latest_path.exists():
        return None
    return json.loads(latest_path.read_text(encoding="utf-8"))
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import pyodbc

from config import settings


def _connect_registry_db():
    driver = settings.SQL_DRIVER
    server = settings.SQL_SERVER
    database = settings.REGISTRY_DB

    if settings.SQL_TRUSTED:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={settings.SQL_USER};"
            f"PWD={settings.SQL_PASSWORD};"
            "TrustServerCertificate=yes;"
        )

    return pyodbc.connect(conn_str)


def save_prompt_history(
    database: Optional[str] = None,
    dw_id: Optional[str] = None,
    user_prompt: Optional[str] = None,
    agent_response: Optional[str] = None,
    generated_mdx: Optional[str] = None,
    *,
    cube_name: Optional[str] = None,
    intent: Optional[str] = None,
    status: Optional[str] = None,
    response_message: Optional[str] = None,
    xmla_script: Optional[str] = None,
    preview: Optional[Dict[str, Any]] = None,
    prompt: Optional[str] = None,
) -> None:
    final_dw_id = dw_id
    final_prompt = prompt if prompt is not None else user_prompt
    final_response_message = response_message if response_message is not None else agent_response
    final_cube_name = cube_name if cube_name is not None else database

    preview_payload = preview.copy() if isinstance(preview, dict) else {}

    # IMPORTANT:
    # si on reçoit generated_mdx depuis agent_prompt, on le garde dans preview_json
    # et on laisse xmla_script vide
    if generated_mdx:
        preview_payload["suggested_mdx"] = generated_mdx
        preview_payload["mdx"] = generated_mdx

    preview_json = json.dumps(preview_payload, ensure_ascii=False) if preview_payload else None

    conn = _connect_registry_db()
    cur = conn.cursor()

    cur.execute(
        """
INSERT INTO dbo.prompt_history
(
    dw_id,
    cube_name,
    prompt,
    intent,
    status,
    response_message,
    xmla_script,
    preview_json
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            final_dw_id,
            final_cube_name,
            final_prompt,
            intent,
            status,
            final_response_message,
            xmla_script,   # seulement le vrai XMLA
            preview_json,
        ),
    )

    conn.commit()
    conn.close()


def get_prompt_history(dw_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    conn = _connect_registry_db()
    cur = conn.cursor()

    cur.execute(
        """
SELECT TOP (?)
    id,
    dw_id,
    cube_name,
    prompt,
    intent,
    status,
    response_message,
    xmla_script,
    preview_json,
    created_at
FROM dbo.prompt_history
WHERE dw_id = ?
ORDER BY created_at DESC
        """,
        (limit, dw_id),
    )

    rows = cur.fetchall()
    conn.close()

    items: List[Dict[str, Any]] = []
    for row in rows:
        preview = json.loads(row.preview_json) if row.preview_json else None
        items.append(
            {
                "id": row.id,
                "dw_id": row.dw_id,
                "cube_name": row.cube_name,
                "prompt": row.prompt,
                "intent": row.intent,
                "status": row.status,
                "response_message": row.response_message,
                "xmla_script": row.xmla_script,
                "preview": preview,
                "created_at": str(row.created_at),
            }
        )

    return items


def get_recent_prompt_history(dw_id: str, limit: int = 8) -> List[Dict[str, Any]]:
    rows = get_prompt_history(dw_id=dw_id, limit=limit)

    normalized: List[Dict[str, Any]] = []
    for row in rows:
        preview = row.get("preview") or {}
        normalized.append(
            {
                "user_prompt": row.get("prompt"),
                "generated_mdx": preview.get("suggested_mdx") or preview.get("mdx"),
                "agent_response": row.get("response_message"),
                "cube_name": row.get("cube_name"),
                "intent": row.get("intent"),
                "status": row.get("status"),
                "created_at": row.get("created_at"),
            }
        )

    return normalized
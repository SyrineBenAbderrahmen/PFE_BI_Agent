from __future__ import annotations

import json
import pyodbc
from typing import Optional, Dict, Any

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


def save_cube_record(
    dw_id: str,
    cube_name: str,
    ssas_database: str,
    cube_model: Dict[str, Any],
    status: str = "created",
) -> None:
    conn = _connect_registry_db()
    cur = conn.cursor()

    cube_json = json.dumps(cube_model, ensure_ascii=False)

    cur.execute(
        """
MERGE dbo.cube_registry AS target
USING (
    SELECT
        ? AS dw_id,
        ? AS cube_name
) AS source
ON target.dw_id = source.dw_id
   AND target.cube_name = source.cube_name
WHEN MATCHED THEN
    UPDATE SET
        ssas_database = ?,
        status = ?,
        cube_model_json = ?,
        updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (dw_id, cube_name, ssas_database, status, cube_model_json)
    VALUES (?, ?, ?, ?, ?);
        """,
        (
            dw_id,
            cube_name,
            ssas_database,
            status,
            cube_json,
            dw_id,
            cube_name,
            ssas_database,
            status,
            cube_json,
        ),
    )

    conn.commit()
    conn.close()


def load_cube_record(dw_id: str, cube_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
    conn = _connect_registry_db()
    cur = conn.cursor()

    if cube_name:
        cur.execute(
            """
SELECT TOP 1
    id,
    dw_id,
    cube_name,
    ssas_database,
    status,
    cube_model_json,
    created_at,
    updated_at
FROM dbo.cube_registry
WHERE dw_id = ?
  AND cube_name = ?
ORDER BY updated_at DESC;
            """,
            (dw_id, cube_name),
        )
    else:
        cur.execute(
            """
SELECT TOP 1
    id,
    dw_id,
    cube_name,
    ssas_database,
    status,
    cube_model_json,
    created_at,
    updated_at
FROM dbo.cube_registry
WHERE dw_id = ?
ORDER BY updated_at DESC;
            """,
            (dw_id,),
        )

    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "id": row.id,
        "dw_id": row.dw_id,
        "cube_name": row.cube_name,
        "ssas_database": row.ssas_database,
        "status": row.status,
        "cube_model": json.loads(row.cube_model_json),
        "created_at": str(row.created_at),
        "updated_at": str(row.updated_at),
    }


def cube_exists(dw_id: str, cube_name: str) -> bool:
    conn = _connect_registry_db()
    cur = conn.cursor()

    cur.execute(
        """
SELECT 1
FROM dbo.cube_registry
WHERE dw_id = ?
  AND cube_name = ?;
        """,
        (dw_id, cube_name),
    )

    exists = cur.fetchone() is not None
    conn.close()
    return exists
from __future__ import annotations

import pyodbc
from typing import Dict, Any, List

from config import settings


def _connect_sqlserver(database: str):
    driver = settings.SQL_DRIVER
    server = settings.SQL_SERVER

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


def _load_ssas_metadata(ssas_database: str, cube_name: str) -> Dict[str, Any]:
    """
    Extrait les dimensions / hiérarchies / niveaux depuis SSAS.
    Retourne {} si pyadomd n'est pas disponible ou si l'extraction échoue.
    """
    try:
        from pyadomd import Pyadomd
    except Exception:
        return {}

    conn_str = (
        f"Provider=MSOLAP;"
        f"Data Source={settings.SSAS_SERVER};"
        f"Catalog={ssas_database};"
        f"Integrated Security=SSPI;"
    )

    dimensions: Dict[str, Any] = {}

    try:
        with Pyadomd(conn_str) as conn:
            hier_query = f"""
SELECT
    [DIMENSION_UNIQUE_NAME],
    [DIMENSION_CAPTION],
    [HIERARCHY_UNIQUE_NAME],
    [HIERARCHY_CAPTION],
    [CUBE_NAME]
FROM $SYSTEM.MDSCHEMA_HIERARCHIES
WHERE [CUBE_NAME] = '{cube_name}'
"""
            with conn.cursor().execute(hier_query) as cur:
                for row in cur.fetchall():
                    dim_unique = str(row[0] or "").strip()
                    dim_caption = str(row[1] or "").strip()
                    hier_unique = str(row[2] or "").strip()
                    hier_caption = str(row[3] or "").strip()

                    if not dim_unique or not hier_unique:
                        continue

                    dim_name = dim_caption or dim_unique.strip("[]")
                    dim_obj = dimensions.setdefault(dim_name, {
                        "name": dim_name,
                        "mdx_name": dim_name,
                        "hierarchies": []
                    })

                    dim_obj["hierarchies"].append({
                        "name": hier_caption or hier_unique.strip("[]"),
                        "mdx_name": hier_caption or hier_unique.strip("[]"),
                        "unique_name": hier_unique,
                        "levels": []
                    })

            lvl_query = f"""
SELECT
    [DIMENSION_UNIQUE_NAME],
    [DIMENSION_CAPTION],
    [HIERARCHY_UNIQUE_NAME],
    [HIERARCHY_CAPTION],
    [LEVEL_UNIQUE_NAME],
    [LEVEL_CAPTION],
    [LEVEL_NUMBER],
    [CUBE_NAME]
FROM $SYSTEM.MDSCHEMA_LEVELS
WHERE [CUBE_NAME] = '{cube_name}'
ORDER BY [DIMENSION_UNIQUE_NAME], [HIERARCHY_UNIQUE_NAME], [LEVEL_NUMBER]
"""
            with conn.cursor().execute(lvl_query) as cur:
                for row in cur.fetchall():
                    dim_caption = str(row[1] or "").strip()
                    hier_unique = str(row[2] or "").strip()
                    lvl_unique = str(row[4] or "").strip()
                    lvl_caption = str(row[5] or "").strip()

                    if not dim_caption or not hier_unique or not lvl_unique:
                        continue

                    dim_obj = dimensions.get(dim_caption)
                    if not dim_obj:
                        continue

                    hier_obj = None
                    for h in dim_obj["hierarchies"]:
                        if h.get("unique_name") == hier_unique:
                            hier_obj = h
                            break

                    if not hier_obj:
                        continue

                    hier_obj["levels"].append({
                        "name": lvl_caption,
                        "mdx_name": lvl_caption,
                        "unique_name": lvl_unique
                    })

    except Exception:
        return {}

    return dimensions


def fetch_columns_and_pk(cursor, schema: str):
    cursor.execute(
        """
SELECT
    c.TABLE_SCHEMA,
    c.TABLE_NAME,
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.IS_NULLABLE,
    c.ORDINAL_POSITION,
    CASE
        WHEN kcu.COLUMN_NAME IS NOT NULL THEN 1
        ELSE 0
    END AS IS_PK
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN (
    SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
        ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
       AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
      AND ku.TABLE_SCHEMA = ?
) kcu
    ON c.TABLE_SCHEMA = kcu.TABLE_SCHEMA
   AND c.TABLE_NAME = kcu.TABLE_NAME
   AND c.COLUMN_NAME = kcu.COLUMN_NAME
WHERE c.TABLE_SCHEMA = ?
ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION;
        """,
        (schema, schema),
    )

    columns = []
    for row in cursor.fetchall():
        columns.append({
            "TABLE_SCHEMA": row.TABLE_SCHEMA,
            "TABLE_NAME": row.TABLE_NAME,
            "COLUMN_NAME": row.COLUMN_NAME,
            "DATA_TYPE": row.DATA_TYPE,
            "IS_NULLABLE": row.IS_NULLABLE,
            "ORDINAL_POSITION": row.ORDINAL_POSITION,
            "IS_PK": row.IS_PK,
        })
    return columns


def fetch_foreign_keys(cursor, schema: str):
    cursor.execute(
        """
SELECT
    fk.TABLE_SCHEMA,
    fk.TABLE_NAME,
    fk.COLUMN_NAME,
    pk.TABLE_NAME AS REFERENCED_TABLE,
    pk.COLUMN_NAME AS REFERENCED_COLUMN
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk
    ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME
   AND rc.CONSTRAINT_SCHEMA = fk.CONSTRAINT_SCHEMA
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
    ON rc.UNIQUE_CONSTRAINT_NAME = pk.CONSTRAINT_NAME
   AND rc.UNIQUE_CONSTRAINT_SCHEMA = pk.CONSTRAINT_SCHEMA
   AND fk.ORDINAL_POSITION = pk.ORDINAL_POSITION
WHERE fk.TABLE_SCHEMA = ?
ORDER BY fk.TABLE_SCHEMA, fk.TABLE_NAME, fk.COLUMN_NAME;
        """,
        (schema,),
    )

    foreign_keys = []
    for row in cursor.fetchall():
        foreign_keys.append({
            "TABLE_SCHEMA": row.TABLE_SCHEMA,
            "TABLE_NAME": row.TABLE_NAME,
            "COLUMN_NAME": row.COLUMN_NAME,
            "REFERENCED_TABLE": row.REFERENCED_TABLE,
            "REFERENCED_COLUMN": row.REFERENCED_COLUMN,
        })
    return foreign_keys


def build_dw_schema_snapshot(columns_rows, fk_rows, database_name: str) -> dict:
    fk_map = {}
    for row in fk_rows:
        key = (row["TABLE_SCHEMA"], row["TABLE_NAME"], row["COLUMN_NAME"])
        fk_map[key] = {
            "references_table": row["REFERENCED_TABLE"],
            "references_column": row["REFERENCED_COLUMN"],
        }

    tables_map = {}

    for row in columns_rows:
        table_key = (row["TABLE_SCHEMA"], row["TABLE_NAME"])
        if table_key not in tables_map:
            tables_map[table_key] = {
                "name": row["TABLE_NAME"],
                "schema_name": row["TABLE_SCHEMA"],
                "columns": [],
            }

        fk_info = fk_map.get((row["TABLE_SCHEMA"], row["TABLE_NAME"], row["COLUMN_NAME"]))

        tables_map[table_key]["columns"].append({
            "name": row["COLUMN_NAME"],
            "data_type": row["DATA_TYPE"],
            "nullable": str(row.get("IS_NULLABLE", "YES")).upper() == "YES",
            "is_pk": bool(row["IS_PK"]),
            "is_fk": fk_info is not None,
            "references_table": fk_info["references_table"] if fk_info else None,
            "references_column": fk_info["references_column"] if fk_info else None,
        })

    return {
        "database_name": database_name,
        "tables": list(tables_map.values())
    }


def get_db_metadata(
    database: str,
    schema: str = "dbo",
    ssas_database: str | None = None,
    cube_name: str | None = None
) -> Dict[str, Any]:
    conn = _connect_sqlserver(database)
    cur = conn.cursor()

    columns_rows = fetch_columns_and_pk(cur, schema)
    fk_rows = fetch_foreign_keys(cur, schema)

    conn.close()

    snapshot = build_dw_schema_snapshot(columns_rows, fk_rows, database)

    ssas_dimensions = {}
    if ssas_database and cube_name:
        ssas_dimensions = _load_ssas_metadata(ssas_database, cube_name)

    snapshot["ssas_dimensions"] = ssas_dimensions
    snapshot["schema"] = schema

    return snapshot
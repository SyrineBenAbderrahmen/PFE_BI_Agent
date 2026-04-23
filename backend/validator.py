from __future__ import annotations

import re
import unicodedata
from typing import Dict, Any, List, Set


def _norm_text(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    s = re.sub(r"[_\-\s]+", "", s)
    return s


def _build_dimension_catalog(schema: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Catalogue unifié des dimensions valides.
    Compatible avec :
    - ancien format: schema["dimensions"]
    - nouveau format relationnel: schema["tables"] avec tables Dim*
    """
    catalog: Dict[str, Dict[str, Any]] = {}

    # 1) Cas ancien / SSAS-friendly : dimensions déjà présentes
    for d in schema.get("dimensions", []) or []:
        dim_name = (d.get("name") or d.get("dimension_name") or d.get("table") or "").strip()
        if not dim_name:
            continue

        attrs: Set[str] = set()

        for a in d.get("attributes", []) or []:
            if isinstance(a, dict):
                for key in [a.get("name"), a.get("attribute_name"), a.get("mdx_name")]:
                    if key:
                        attrs.add(key)
            elif isinstance(a, str) and a.strip():
                attrs.add(a.strip())

        # hiérarchies naturelles éventuelles
        for h in d.get("hierarchies", []) or []:
            for lvl in h.get("levels", []) or []:
                if isinstance(lvl, dict):
                    for key in [lvl.get("name"), lvl.get("level_name"), lvl.get("mdx_name")]:
                        if key:
                            attrs.add(key)
                elif isinstance(lvl, str) and lvl.strip():
                    attrs.add(lvl.strip())

        catalog[dim_name] = {
            "name": dim_name,
            "aliases": {
                _norm_text(dim_name),
                _norm_text(dim_name.replace("Dim", "", 1)),
            },
            "attributes": attrs,
        }

    # 2) natural_hierarchies
    for dim_name, hier_list in (schema.get("natural_hierarchies", {}) or {}).items():
        if not dim_name:
            continue

        if dim_name not in catalog:
            catalog[dim_name] = {
                "name": dim_name,
                "aliases": {
                    _norm_text(dim_name),
                    _norm_text(dim_name.replace("Dim", "", 1)),
                },
                "attributes": set(),
            }

        for h in hier_list or []:
            h_name = h.get("name")
            h_mdx = h.get("mdx_name")
            if h_name:
                catalog[dim_name]["attributes"].add(h_name)
            if h_mdx:
                catalog[dim_name]["attributes"].add(h_mdx)

            for lvl in h.get("levels", []) or []:
                if isinstance(lvl, dict):
                    for key in [lvl.get("name"), lvl.get("level_name"), lvl.get("mdx_name")]:
                        if key:
                            catalog[dim_name]["attributes"].add(key)
                elif isinstance(lvl, str) and lvl.strip():
                    catalog[dim_name]["attributes"].add(lvl.strip())

    # 3) Fallback relationnel : tables Dim*
    for table in schema.get("tables", []) or []:
        table_name = (table.get("name") or "").strip()
        if not table_name.startswith("Dim"):
            continue

        if table_name not in catalog:
            attrs = set()
            for col in table.get("columns", []) or []:
                col_name = (col.get("name") or "").strip()
                if col_name:
                    attrs.add(col_name)

            catalog[table_name] = {
                "name": table_name,
                "aliases": {
                    _norm_text(table_name),
                    _norm_text(table_name.replace("Dim", "", 1)),
                },
                "attributes": attrs,
            }
        else:
            for col in table.get("columns", []) or []:
                col_name = (col.get("name") or "").strip()
                if col_name:
                    catalog[table_name]["attributes"].add(col_name)

    return catalog


def _resolve_dimension_name(raw_dim: str, catalog: Dict[str, Dict[str, Any]]) -> str | None:
    if not raw_dim:
        return None

    raw_norm = _norm_text(raw_dim)

    # Match direct nom canonique
    for canonical, info in catalog.items():
        if raw_norm == _norm_text(canonical):
            return canonical

    # Match alias
    for canonical, info in catalog.items():
        if raw_norm in info["aliases"]:
            return canonical

    return None


def _is_valid_attribute_for_dimension(attr: str, dim_name: str, catalog: Dict[str, Dict[str, Any]]) -> bool:
    if not attr:
        return False

    attr_norm = _norm_text(attr)
    attrs = catalog.get(dim_name, {}).get("attributes", set())

    for a in attrs:
        if attr_norm == _norm_text(a):
            return True

    return False


def validate_plan_against_schema(plan: Dict[str, Any], schema: Dict[str, Any]) -> List[str]:
    errors: List[str] = []

    if not isinstance(plan, dict):
        return ["Plan must be a JSON object"]

    catalog = _build_dimension_catalog(schema)

    # ===== Dimensions =====
    for dim in plan.get("dimensions", []) or []:
        if not isinstance(dim, dict):
            continue

        raw_dim_name = (
            dim.get("table")
            or dim.get("name")
            or dim.get("dimension")
            or ""
        ).strip()

        resolved_dim = _resolve_dimension_name(raw_dim_name, catalog)
        if not resolved_dim:
            errors.append(f"Unknown dimension in plan: {raw_dim_name}")
            continue

        for attr in dim.get("attributes", []) or []:
            if not isinstance(attr, str):
                continue

            # on autorise les dimensions sans attributs
            if attr.strip() and not _is_valid_attribute_for_dimension(attr, resolved_dim, catalog):
                errors.append(f"Unknown attribute '{attr}' for dimension '{resolved_dim}'")

    # ===== Fact table =====
    fact_table = (plan.get("fact_table") or "").strip()
    if fact_table:
        fact_tables = {
            (t.get("name") or "").strip()
            for t in schema.get("tables", []) or []
            if (t.get("name") or "").strip().startswith("Fact")
        }
        if fact_tables and fact_table not in fact_tables:
            errors.append(f"Unknown fact table in plan: {fact_table}")

    # ===== Measures =====
    if fact_table:
        fact_columns = set()
        for t in schema.get("tables", []) or []:
            if (t.get("name") or "").strip() == fact_table:
                for c in t.get("columns", []) or []:
                    col_name = (c.get("name") or "").strip()
                    if col_name:
                        fact_columns.add(col_name)
                break

        for m in plan.get("measures", []) or []:
            if not isinstance(m, dict):
                continue

            col = (m.get("column") or "").strip()
            if col and fact_columns and col not in fact_columns:
                errors.append(f"Unknown measure column '{col}' in fact table '{fact_table}'")

    return errors
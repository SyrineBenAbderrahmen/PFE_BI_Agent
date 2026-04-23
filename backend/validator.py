from __future__ import annotations

import re
import unicodedata
from typing import Dict, Any, List, Optional, Set
from schema_builder import _norm
from schema_store import load_schema_snapshot
from config import settings
from schema_builder import _norm


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
def _valid_dimension_attributes(schema: Optional[Dict[str, Any]], dim_name: str) -> List[str]:
    if not schema:
        return []

    for d in schema.get("dimensions", []) or []:
        current_dim = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        if _norm(current_dim) != _norm(dim_name):
            continue

        attrs = []
        raw_attrs = d.get("attributes") or d.get("levels") or []
        for a in raw_attrs:
            if isinstance(a, dict):
                attr_name = (
                    a.get("name")
                    or a.get("column")
                    or a.get("source_column")
                    or a.get("mdx_name")
                    or ""
                ).strip()
            else:
                attr_name = str(a).strip()

            if attr_name:
                attrs.append(attr_name)

        return attrs

    return []

def _is_valid_attr_for_dim(schema: Optional[Dict[str, Any]], dim_name: str, attr_name: str) -> bool:
    if not schema:
        return True

    valid_attrs = _valid_dimension_attributes(schema, dim_name)
    return any(_norm(a) == _norm(attr_name) for a in valid_attrs)

def _looks_like_measure_name(schema: Optional[Dict[str, Any]], name: str) -> bool:
    if not schema or not name:
        return False

    for fact in schema.get("facts", []) or []:
        for m in fact.get("measures", []) or []:
            m_name = (m.get("name") or "").strip()
            m_col = (m.get("column") or "").strip()
            if _norm(m_name) == _norm(name) or _norm(m_col) == _norm(name):
                return True
    return False


def normalize_plan_measure_facts(plan: dict, schema: dict) -> dict:
    for m in plan.get("measures", []) or []:
        if not isinstance(m, dict):
            continue

        measure_name = (m.get("name") or "").strip()
        measure_column = (m.get("column") or "").strip()

        resolved_fact = None
        if measure_column:
            resolved_fact = _find_fact_for_measure(schema, measure_column)
        if not resolved_fact and measure_name:
            resolved_fact = _find_fact_for_measure(schema, measure_name)

        if resolved_fact:
            m["fact_table"] = resolved_fact

    if not (plan.get("fact_table") or "").strip():
        for m in plan.get("measures", []) or []:
            if isinstance(m, dict) and m.get("fact_table"):
                plan["fact_table"] = m["fact_table"]
                break

    return plan


def normalize_plan_dimensions(plan: dict, schema: dict, user_prompt: str = "") -> dict:
    cleaned_dims = []

    for d in plan.get("dimensions", []) or []:
        if not isinstance(d, dict):
            continue

        dim_name = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        if not dim_name:
            continue

        attrs = d.get("attributes") or d.get("levels") or []
        cleaned_attrs = []

        for a in attrs:
            attr_name = str(a).strip()
            if not attr_name:
                continue

            if _looks_like_measure_name(schema, attr_name):
                continue

            if _is_valid_attr_for_dim(schema, dim_name, attr_name):
                cleaned_attrs.append(attr_name)

        t = (user_prompt or "").lower()
        if not cleaned_attrs:
            if _norm(dim_name) == _norm("DimProduct") and any(x in t for x in ["produit", "produits", "product", "products"]):
                cleaned_attrs = ["ProductName"]
            elif _norm(dim_name) == _norm("DimVendor") and any(x in t for x in ["fournisseur", "fournisseurs", "vendor", "vendors"]):
                cleaned_attrs = ["VendorName"]
            elif _norm(dim_name) == _norm("DimPurchaseOrder") and any(x in t for x in ["commande", "commandes", "purchase order"]):
                cleaned_attrs = ["PurchaseOrderID"]
            elif _norm(dim_name) == _norm("DimDate"):
                cleaned_attrs = ["YearNumber"]

        new_dim = dict(d)
        new_dim["attributes"] = cleaned_attrs
        cleaned_dims.append(new_dim)

    plan["dimensions"] = cleaned_dims
    return plan



def _is_valid_attribute_for_dimension(attr: str, dim_name: str, catalog: Dict[str, Dict[str, Any]]) -> bool:
    if not attr:
        return False

    attr_norm = _norm_text(attr)
    attrs = catalog.get(dim_name, {}).get("attributes", set())

    for a in attrs:
        if attr_norm == _norm_text(a):
            return True

    return False

def _measure_exists_in_fact(schema: dict, fact_table: str, measure_name: str) -> bool:
    for fact in schema.get("facts", []) or []:
        if (fact.get("name") or "").strip() != (fact_table or "").strip():
            continue

        for m in fact.get("measures", []) or []:
            m_name = (m.get("name") or "").strip()
            m_col = (m.get("column") or "").strip()
            if _norm(m_name) == _norm(measure_name) or _norm(m_col) == _norm(measure_name):
                return True

    return False


def _find_fact_for_measure(schema: Optional[Dict[str, Any]], measure_name: str) -> str:
    if not schema or not measure_name:
        return ""

    matches = []
    for fact in schema.get("facts", []) or []:
        fact_name = (fact.get("name") or "").strip()
        for m in fact.get("measures", []) or []:
            m_name = (m.get("name") or "").strip()
            m_col = (m.get("column") or "").strip()
            if _norm(m_name) == _norm(measure_name) or _norm(m_col) == _norm(measure_name):
                matches.append(fact_name)

    matches = list(dict.fromkeys(matches))
    return matches[0] if len(matches) == 1 else ""

def validate_plan_against_schema(plan: dict, schema: dict, user_prompt: str = "") -> list[str]:
    errors: list[str] = []

    plan = normalize_plan_measure_facts(plan, schema)
    plan = normalize_plan_dimensions(plan, schema, user_prompt)

    schema_dimensions = {
        (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        for d in schema.get("dimensions", []) or []
    }

    schema_facts = {
        (f.get("name") or "").strip()
        for f in schema.get("facts", []) or []
    }

    plan_fact_table = (plan.get("fact_table") or "").strip()

    if plan_fact_table and plan_fact_table not in schema_facts:
        errors.append(f"Unknown fact table in plan: {plan_fact_table}")

    for d in plan.get("dimensions", []) or []:
        dim_name = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        if dim_name and dim_name not in schema_dimensions:
            errors.append(f"Unknown dimension in plan: {dim_name}")
            continue

        for attr in d.get("attributes", []) or []:
            if not _is_valid_attr_for_dim(schema, dim_name, attr):
                errors.append(f"Unknown attribute '{attr}' for dimension '{dim_name}'")

    for m in plan.get("measures", []) or []:
        if not isinstance(m, dict):
            continue

        measure_name = (m.get("column") or m.get("name") or "").strip()
        if not measure_name:
            continue

        measure_fact_table = (m.get("fact_table") or "").strip()

        if not measure_fact_table:
            resolved_fact = _find_fact_for_measure(schema, measure_name)
            if resolved_fact:
                m["fact_table"] = resolved_fact
                measure_fact_table = resolved_fact

        if not measure_fact_table:
            errors.append(
                f"Unknown measure column '{measure_name}' in fact table '{plan_fact_table or 'unknown'}'"
            )
            continue

        if not _measure_exists_in_fact(schema, measure_fact_table, measure_name):
            resolved_fact = _find_fact_for_measure(schema, measure_name)
            if resolved_fact:
                m["fact_table"] = resolved_fact
                measure_fact_table = resolved_fact

        if not _measure_exists_in_fact(schema, measure_fact_table, measure_name):
            errors.append(
                f"Unknown measure column '{measure_name}' in fact table '{measure_fact_table}'"
            )

    return errors

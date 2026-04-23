from __future__ import annotations

import re
from typing import Dict, Any, List, Optional


def _norm(s: str) -> str:
    return re.sub(r"[\s_\-]+", "", (s or "").strip().lower())


def _space_label(name: str) -> str:
    if not name:
        return name
    s = str(name).strip()
    s = s.replace("_", " ")
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _column_name(col: Any) -> str:
    if isinstance(col, dict):
        return col.get("name") or col.get("column_name") or col.get("Name") or ""
    return str(col or "")


def classify_fact_dim(table_name: str) -> str:
    n = _norm(table_name)
    if n.startswith("fact"):
        return "fact"
    if n.startswith("dim"):
        return "dimension"
    if "fact" in n:
        return "fact"
    if "dimension" in n:
        return "dimension"
    return "other"


def _infer_measure_agg(col_name: str) -> Optional[str]:
    n = _norm(col_name)

    if n.endswith("key") or n.endswith("id"):
        return None

    blocked_suffixes = [
        "key", "id", "histkey", "reviewkey", "saleskey",
        "pricehistkey", "costhistkey", "productkey",
        "datekey", "vendorkey", "workorderkey",
        "purchaseorderkey", "inventorymovementkey",
        "productionimpactkey", "supplyriskkey"
    ]
    if any(k == n or n.endswith(k) for k in blocked_suffixes):
        return None

    if any(k in n for k in ["avg", "average", "mean", "rate"]):
        return "Avg"

    if any(k in n for k in ["count", "number", "nbr", "nb", "flag"]):
        return "Count"

    if any(k in n for k in [
        "amount", "total", "revenue", "price", "cost",
        "qty", "quantity", "value", "profit", "balance",
        "line", "margin", "rating", "days", "stock"
    ]):
        return "Sum"

    return None


def _collect_dimension_attributes(table: Dict[str, Any], ssas_dimensions: Dict[str, Any]) -> List[Dict[str, Any]]:
    attrs = []
    table_name = table.get("name", "").strip()

    dim_unique_name = f"[{table_name}]"
    for dim_name, dim_obj in (ssas_dimensions or {}).items():
        if _norm(dim_name) == _norm(table_name):
            dim_unique_name = dim_obj.get("unique_name") or f"[{dim_name}]"
            break

    seen = set()

    for col in table.get("columns", []) or []:
        col_name = _column_name(col).strip()
        if not col_name:
            continue

        norm_col = _norm(col_name)
        if norm_col in seen:
            continue
        seen.add(norm_col)

        mdx_name = _space_label(col_name)

        attrs.append({
            "name": col_name,
            "mdx_name": mdx_name,
            "unique_name": f"{dim_unique_name}.[{mdx_name}]",
            "sql_type": col.get("data_type", "nvarchar") if isinstance(col, dict) else "nvarchar",
        })

    return attrs


def _collect_fact_measures(table: Dict[str, Any]) -> List[Dict[str, Any]]:
    measures = []
    seen = set()

    for col in table.get("columns", []) or []:
        col_name = _column_name(col).strip()
        if not col_name:
            continue

        agg = _infer_measure_agg(col_name)
        if agg:
            key = (_norm(col_name), agg)
            if key in seen:
                continue
            seen.add(key)

            measures.append({
                "name": _space_label(col_name),
                "column": col_name,
                "agg": agg,
                "sql_type": col.get("data_type", "decimal") if isinstance(col, dict) else "decimal",
            })

    return measures


def _find_column(cols: List[str], *wanted_names: str) -> Optional[str]:
    for wanted in wanted_names:
        for c in cols:
            if _norm(c) == _norm(wanted):
                return c
    return None


def _make_level(col_name: str) -> Dict[str, Any]:
    return {
        "name": col_name,
        "mdx_name": _space_label(col_name),
        "unique_name": None,
        "source_column": col_name,
    }


def _infer_fallback_hierarchies(table: Dict[str, Any]) -> List[Dict[str, Any]]:
    cols = [_column_name(c).strip() for c in table.get("columns", []) or []]
    cols = [c for c in cols if c]

    hierarchies = []
    table_name = table.get("name", "").strip()
    table_norm = _norm(table_name)

    if table_norm == _norm("DimDate"):
        year_col = _find_column(cols, "Year", "YearNumber")
        quarter_col = _find_column(cols, "Quarter", "QuarterNumber")
        month_col = _find_column(cols, "Month", "MonthName", "MonthNumber")
        date_col = _find_column(cols, "FullDate", "Date", "DateValue")

        levels = []
        for c in [year_col, quarter_col, month_col, date_col]:
            if c:
                levels.append(_make_level(c))

        if len(levels) >= 2:
            hierarchies.append({
                "name": "Calendar",
                "mdx_name": "Calendar",
                "unique_name": None,
                "levels": levels
            })

    if table_norm == _norm("DimProduct"):
        category_col = _find_column(cols, "Category")
        subcategory_col = _find_column(cols, "SubCategory", "Sub Category")
        product_name_col = _find_column(cols, "ProductName", "Product Name")

        if category_col and subcategory_col:
            levels = [_make_level(category_col), _make_level(subcategory_col)]
            if product_name_col:
                levels.append(_make_level(product_name_col))

            hierarchies.append({
                "name": "Product",
                "mdx_name": "Product",
                "unique_name": None,
                "levels": levels
            })
        elif product_name_col:
            hierarchies.append({
                "name": "Product",
                "mdx_name": "Product",
                "unique_name": None,
                "levels": [_make_level(product_name_col)]
            })

    if table_norm == _norm("DimVendor"):
        vendor_name_col = _find_column(cols, "VendorName", "Vendor Name")
        account_col = _find_column(cols, "AccountNumber", "Account Number")

        levels = []
        if vendor_name_col:
            levels.append(_make_level(vendor_name_col))
        if account_col:
            levels.append(_make_level(account_col))

        if levels:
            hierarchies.append({
                "name": "Vendor",
                "mdx_name": "Vendor",
                "unique_name": None,
                "levels": levels
            })

    if table_norm == _norm("DimWorkOrder"):
        workorder_id_col = _find_column(cols, "WorkOrderID", "Work Order ID")
        start_date_col = _find_column(cols, "StartDate", "Start Date")
        due_date_col = _find_column(cols, "DueDate", "Due Date")

        levels = []
        if workorder_id_col:
            levels.append(_make_level(workorder_id_col))
        if start_date_col:
            levels.append(_make_level(start_date_col))
        if due_date_col:
            levels.append(_make_level(due_date_col))

        if levels:
            hierarchies.append({
                "name": "Work Order",
                "mdx_name": "Work Order",
                "unique_name": None,
                "levels": levels
            })

    if table_norm == _norm("DimPurchaseOrder"):
        po_id_col = _find_column(cols, "PurchaseOrderID", "Purchase Order ID")
        order_date_col = _find_column(cols, "OrderDate", "Order Date")
        ship_date_col = _find_column(cols, "ShipDate", "Ship Date")

        levels = []
        if po_id_col:
            levels.append(_make_level(po_id_col))
        if order_date_col:
            levels.append(_make_level(order_date_col))
        if ship_date_col:
            levels.append(_make_level(ship_date_col))

        if levels:
            hierarchies.append({
                "name": "Purchase Order",
                "mdx_name": "Purchase Order",
                "unique_name": None,
                "levels": levels
            })

    return hierarchies


def infer_natural_hierarchies(meta: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {}
    tables = meta.get("tables", []) or []
    ssas_dimensions = meta.get("ssas_dimensions", {}) or {}

    for table in tables:
        table_name = table.get("name", "").strip()
        if classify_fact_dim(table_name) != "dimension":
            continue

        matched_ssas = None
        for dim_name, dim_obj in ssas_dimensions.items():
            if _norm(dim_name) == _norm(table_name):
                matched_ssas = dim_obj
                break

        if matched_ssas and matched_ssas.get("hierarchies"):
            result[table_name] = matched_ssas["hierarchies"]
        else:
            fallback = _infer_fallback_hierarchies(table)
            if fallback:
                result[table_name] = fallback

    return result


def _infer_key_attribute(table: Dict[str, Any]) -> str:
    for col in table.get("columns", []) or []:
        if isinstance(col, dict) and col.get("is_pk") and col.get("name"):
            return col["name"]

    for col in table.get("columns", []) or []:
        col_name = _column_name(col).strip()
        if col_name.endswith("Key"):
            return col_name

    for col in table.get("columns", []) or []:
        col_name = _column_name(col).strip()
        if col_name.endswith("ID"):
            return col_name

    first_col = next((_column_name(c).strip() for c in table.get("columns", []) or [] if _column_name(c).strip()), None)
    return first_col or "Key"


def build_dw_schema_snapshot(meta: Dict[str, Any]) -> Dict[str, Any]:
    tables = meta.get("tables", []) or []
    ssas_dimensions = meta.get("ssas_dimensions", {}) or {}

    dimensions = []
    facts = []

    for table in tables:
        table_name = table.get("name", "").strip()
        kind = classify_fact_dim(table_name)

        if kind == "dimension":
            dim_unique_name = f"[{table_name}]"
            for dim_name, dim_obj in ssas_dimensions.items():
                if _norm(dim_name) == _norm(table_name):
                    dim_unique_name = dim_obj.get("unique_name") or f"[{dim_name}]"
                    break

            dimensions.append({
                "name": table_name,
                "source_table": table_name,
                "key_attribute": _infer_key_attribute(table),
                "unique_name": dim_unique_name,
                "attributes": _collect_dimension_attributes(table, ssas_dimensions),
                "hierarchies": []
            })

        elif kind == "fact":
            facts.append({
                "name": table_name,
                "source_table": table_name,
                "measures": _collect_fact_measures(table)
            })

    return {
        "database_name": meta.get("database_name", "unknown_database"), 
        "schema": meta.get("schema", "dbo"),
        "ssas_dimensions": meta.get("ssas_dimensions", {}),
        "tables": meta.get("tables", []),
        "dimensions": dimensions,
        "facts": facts,
        "natural_hierarchies": infer_natural_hierarchies(meta)
    }
from __future__ import annotations

from typing import Any, Optional
from xml.sax.saxutils import escape


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def _as_dict(obj: Any) -> dict:
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if isinstance(obj, dict):
        return obj
    return {}


def _safe_name(value: Optional[str], fallback: str = "") -> str:
    return escape(value or fallback)


def _sanitize_id(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in (value or "")).strip("_")


def _normalize_name(value: str) -> str:
    return "".join(ch.lower() for ch in (value or "") if ch.isalnum())


def _agg_to_assl(agg: Optional[str]) -> str:
    a = (agg or "sum").strip().lower()
    mapping = {
        "sum": "Sum",
        "count": "Count",
        "avg": "Sum",
        "average": "Sum",
        "min": "Min",
        "max": "Max",
    }
    return mapping.get(a, "Sum")


def _friendly_fact_prefix(fact_name: str) -> str:
    mapping = {
        "FactSupplyRisk": "Supply Risk",
        "FactProductionImpact": "Production Impact",
        "FactInventoryMovement": "Inventory Movement",
    }
    return mapping.get(fact_name, fact_name.replace("Fact", "").strip() or fact_name)


# ------------------------------------------------------------
# Connection / IDs
# ------------------------------------------------------------

def _build_connection_string(cfg: dict) -> str:
    if cfg.get("connection_string"):
        return cfg["connection_string"]

    server = cfg.get("sql_server", "localhost")
    database = cfg.get("database", "")
    provider = cfg.get("provider", "MSOLEDBSQL.1")

    sql_user = cfg.get("sql_user")
    sql_password = cfg.get("sql_password")

    if sql_user and sql_password:
        return (
            f"Provider={provider};"
            f"Data Source={server};"
            f"Initial Catalog={database};"
            f"User ID={sql_user};"
            f"Password={sql_password};"
            f"Persist Security Info=True;"
        )

    return (
        f"Provider={provider};"
        f"Data Source={server};"
        f"Initial Catalog={database};"
        f"Integrated Security=SSPI;"
        f"Persist Security Info=False;"
    )


def _database_id(cfg: dict, cube_model: dict) -> str:
    name = cube_model.get("cube_name") or "AutoCube"
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)


def _cube_id(cfg: dict, cube_model: dict) -> str:
    name = cube_model.get("cube_name") or "AutoCube"
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)


def _dsv_id(cfg: dict, cube_model: dict) -> str:
    return cfg.get("dsv_id") or cfg.get("database") or f"{_cube_id(cfg, cube_model)}DSV"


# ------------------------------------------------------------
# Type mapping
# ------------------------------------------------------------

SEMANTIC_TYPE_MAP = {
    # DimDate
    "DateKey": "int",
    "FullDate": "datetime",
    "DayNumber": "int",
    "MonthNumber": "int",
    "MonthName": "nvarchar",
    "QuarterNumber": "int",
    "YearNumber": "int",
    "WeekDayNumber": "int",
    "WeekDayName": "nvarchar",

    # DimProduct
    "ProductKey": "int",
    "ProductID": "int",
    "ProductName": "nvarchar",
    "ProductNumber": "nvarchar",
    "MakeFlag": "bit",
    "FinishedGoodsFlag": "bit",
    "SafetyStockLevel": "int",
    "ReorderPoint": "int",
    "StandardCost": "decimal",
    "ListPrice": "decimal",
    "DaysToManufacture": "int",

    # DimVendor
    "VendorKey": "int",
    "VendorID": "int",
    "AccountNumber": "nvarchar",
    "VendorName": "nvarchar",
    "CreditRating": "int",
    "PreferredVendorStatus": "bit",
    "ActiveFlag": "bit",

    # DimPurchaseOrder
    "PurchaseOrderKey": "int",
    "PurchaseOrderID": "int",
    "RevisionNumber": "int",
    "Status": "int",
    "EmployeeID": "int",
    "VendorID": "int",
    "ShipMethodID": "int",
    "OrderDate": "datetime",
    "ShipDate": "datetime",
    "SubTotal": "decimal",
    "TaxAmt": "decimal",
    "Freight": "decimal",
    "TotalDue": "decimal",

    # DimWorkOrder
    "WorkOrderKey": "int",
    "WorkOrderID": "int",
    "ProductID": "int",
    "StartDate": "datetime",
    "EndDate": "datetime",
    "DueDate": "datetime",
    "OrderQty": "int",
    "ScrappedQty": "int",
    "ScrapReasonID": "int",

    # Facts
    "Quantity": "decimal",
    "ActualCost": "decimal",
    "MovementCount": "int",
    "TotalMovementCost": "decimal",
    "ReceivedQty": "decimal",
    "RejectedQty": "decimal",
    "StockedQty": "decimal",
    "UnitPrice": "decimal",
    "LineAmount": "decimal",

    # Dates likely in facts
    "TransactionDate": "datetime",
}


def _semantic_sql_type(column_name: str, default: str = "nvarchar") -> str:
    return SEMANTIC_TYPE_MAP.get(column_name, default)


def _xs_type_from_sql_type(sql_type: str) -> str:
    t = (sql_type or "").lower().strip()

    if t in {"int", "bigint", "smallint", "tinyint"}:
        return "xs:int"

    if t in {"decimal", "numeric", "money", "smallmoney", "float", "real"}:
        return "xs:decimal"

    if t in {"varchar", "nvarchar", "char", "nchar", "text", "ntext"}:
        return "xs:string"

    if t in {"date", "datetime", "datetime2", "smalldatetime"}:
        return "xs:dateTime"

    if t == "bit":
        return "xs:boolean"

    return "xs:string"


def _assl_data_type_from_sql_type(sql_type: str) -> str:
    t = (sql_type or "").lower().strip()

    if t in {"int", "bigint", "smallint", "tinyint"}:
        return "Integer"

    if t in {"decimal", "numeric", "money", "smallmoney", "float", "real"}:
        return "Double"

    if t in {"varchar", "nvarchar", "char", "nchar", "text", "ntext"}:
        return "WChar"

    if t in {"date", "datetime", "datetime2", "smalldatetime"}:
        return "Date"

    if t == "bit":
        return "Boolean"

    return "WChar"


def _measure_assl_data_type_from_sql_type(sql_type: str) -> str:
    return _assl_data_type_from_sql_type(sql_type)


# ------------------------------------------------------------
# Snapshot helpers
# ------------------------------------------------------------

def _get_table(schema_snapshot: dict, table_name: str) -> dict | None:
    # Cas 1 : snapshot relationnel brut
    for t in schema_snapshot.get("tables", []) or []:
        if t.get("name") == table_name:
            return t

    # Cas 2 : snapshot sémantique -> chercher dans dimensions
    for d in schema_snapshot.get("dimensions", []) or []:
        if d.get("name") == table_name:
            attrs = []
            for a in d.get("attributes", []) or []:
                if isinstance(a, dict) and a.get("name"):
                    attrs.append({
                        "name": a.get("name"),
                        "data_type": a.get("sql_type") or _semantic_sql_type(a.get("name"), "nvarchar"),
                        "is_pk": a.get("name") == d.get("key_attribute"),
                        "is_fk": False,
                    })
            return {
                "name": table_name,
                "columns": attrs,
            }

    # Cas 3 : snapshot sémantique -> chercher dans facts
    for f in schema_snapshot.get("facts", []) or []:
        if f.get("name") == table_name:
            cols = []
            for m in f.get("measures", []) or []:
                col_name = m.get("source_column") or m.get("column") or m.get("name")
                if col_name:
                    cols.append({
                        "name": col_name,
                        "data_type": m.get("sql_type") or _semantic_sql_type(col_name, "decimal"),
                        "is_pk": False,
                        "is_fk": False,
                    })
            return {
                "name": table_name,
                "columns": cols,
            }

    return None

def _guess_dimension_key_attribute(dim: dict) -> str:
    key_attr = dim.get("key_attribute")
    if key_attr:
        return key_attr

    for a in dim.get("attributes", []) or []:
        name = a.get("name") if isinstance(a, dict) else str(a)
        if name and name.endswith("Key"):
            return name

    for a in dim.get("attributes", []) or []:
        name = a.get("name") if isinstance(a, dict) else str(a)
        if name and name.endswith("ID"):
            return name

    first = (dim.get("attributes") or [{}])[0]
    if isinstance(first, dict):
        return first.get("name", "Key")
    return str(first or "Key")


def _guess_fact_fk_candidates(fact_name: str, dim_name: str):
    base = dim_name.replace("Dim", "")
    candidates = []

    if base:
        candidates.extend([
            f"{base}Key",
            f"{base}ID",
        ])

    special = {
        ("FactInventoryMovement", "DimDate"): ["TransactionDateKey"],
        ("FactProductionImpact", "DimDate"): ["StartDateKey", "EndDateKey", "DueDateKey"],
        ("FactSupplyRisk", "DimDate"): ["OrderDateKey", "DueDateKey", "ShipDateKey"],
        ("FactInventoryMovement", "DimProduct"): ["ProductKey"],
        ("FactProductionImpact", "DimProduct"): ["ProductKey"],
        ("FactProductionImpact", "DimWorkOrder"): ["WorkOrderKey"],
        ("FactSupplyRisk", "DimPurchaseOrder"): ["PurchaseOrderKey"],
        ("FactSupplyRisk", "DimVendor"): ["VendorKey"],
        ("FactSupplyRisk", "DimProduct"): ["ProductKey"],
    }

    candidates = special.get((fact_name, dim_name), []) + candidates

    seen = []
    for c in candidates:
        if c not in seen:
            seen.append(c)
    return seen

def _table_columns_from_snapshot(schema_snapshot: dict, table_name: str) -> list[dict]:
    table = _get_table(schema_snapshot, table_name)
    if not table:
        return []
    return table.get("columns", []) or []


def _find_best_date_column(schema_snapshot: dict, fact_name: str) -> str | None:
    fact_columns = _table_columns_from_snapshot(schema_snapshot, fact_name)

    date_candidates = []
    for c in fact_columns:
        sql_type = (c.get("data_type") or "").lower()
        col_name = c.get("name", "")
        if sql_type in {"date", "datetime", "datetime2", "smalldatetime"}:
            date_candidates.append(col_name)

    if not date_candidates:
        return None

    for preferred in ["DateKey", "OrderDate", "TransactionDate", "StartDate", "ShipDate", "DueDate", "EndDate"]:
        if preferred in date_candidates:
            return preferred

    return date_candidates[0]


def _get_dimension_key_column(dim: dict, schema_snapshot: dict) -> str | None:
    dim_table = dim.get("source_table") or dim.get("name")
    table = _get_table(schema_snapshot, dim_table)
    if not table:
        return dim.get("key_attribute")

    for col in table.get("columns", []):
        if col.get("is_pk"):
            return col.get("name")

    return dim.get("key_attribute")


def _resolve_granularity_from_fk(schema_snapshot: dict, fact_name: str, dim: dict) -> str | None:
    dim_table = dim.get("source_table") or dim.get("name")
    dim_key = _get_dimension_key_column(dim, schema_snapshot)

    fact_table = _get_table(schema_snapshot, fact_name)
    if not fact_table:
        return None

    for col in fact_table.get("columns", []):
        if not col.get("is_fk"):
            continue

        ref_table = col.get("references_table")
        ref_col = col.get("references_column")

        if ref_table == dim_table and ref_col == dim_key:
            return col.get("name")

    return None


def _resolve_granularity_fallback(schema_snapshot: dict, fact_name: str, dim: dict) -> str:
    fact_table = _get_table(schema_snapshot, fact_name)
    if not fact_table:
        return _guess_dimension_key_attribute(dim)

    dim_name = dim.get("name", "")
    key_attr = _guess_dimension_key_attribute(dim)

    fact_cols = {c["name"] for c in fact_table.get("columns", []) if c.get("name")}

    # 1) candidats métier explicites
    for cand in _guess_fact_fk_candidates(fact_name, dim_name):
        if cand in fact_cols:
            return cand

    # 2) fallback par nom de clé
    candidates = [key_attr]
    if key_attr.endswith("Key"):
        candidates.append(key_attr[:-3] + "ID")
    if key_attr.endswith("ID"):
        candidates.append(key_attr[:-2] + "Key")

    for cand in candidates:
        if cand in fact_cols:
            return cand

    # 3) si rien trouvé, renvoyer la clé de dimension
    return key_attr


def _resolve_granularity_column(schema_snapshot: dict, fact_name: str, dim: dict) -> str:
    by_fk = _resolve_granularity_from_fk(schema_snapshot, fact_name, dim)
    if by_fk:
        return by_fk
    return _resolve_granularity_fallback(schema_snapshot, fact_name, dim)


# ------------------------------------------------------------
# Database / DataSource / DSV
# ------------------------------------------------------------

def generate_database_header(cube_model: dict, cfg: dict) -> str:
    db_id = _database_id(cfg, cube_model)
    return f"""
    <ID>{_safe_name(db_id)}</ID>
    <Name>{_safe_name(db_id)}</Name>
    <ddl200:CompatibilityLevel>1100</ddl200:CompatibilityLevel>
    <Language>1036</Language>
    <Collation>French_CI_AS</Collation>
    <DataSourceImpersonationInfo>
      <ImpersonationMode>Default</ImpersonationMode>
    </DataSourceImpersonationInfo>
    """.strip()


def generate_datasource(cfg: dict, cube_model: dict) -> str:
    ds_id = _dsv_id(cfg, cube_model)
    conn = _build_connection_string(cfg)

    return f"""
    <DataSources>
      <DataSource xsi:type="RelationalDataSource">
        <ID>{_safe_name(ds_id)}</ID>
        <Name>{_safe_name(ds_id)}</Name>
        <ConnectionString>{_safe_name(conn)}</ConnectionString>
        <ImpersonationInfo>
          <ImpersonationMode>ImpersonateServiceAccount</ImpersonationMode>
        </ImpersonationInfo>
        <Timeout>PT0S</Timeout>
      </DataSource>
    </DataSources>
    """.strip()


def _build_dsv_table_element(table_name: str, columns: list[dict], schema_name: str = "dbo") -> str:
    cols_xml = []

    for col in columns:
        col_name = col.get("name")
        sql_type = col.get("sql_type", "nvarchar")
        xs_type = _xs_type_from_sql_type(sql_type)

        cols_xml.append(
            f'<xs:element name="{_safe_name(col_name)}" '
            f'msprop:FriendlyName="{_safe_name(col_name)}" '
            f'msprop:DbColumnName="{_safe_name(col_name)}" '
            f'type="{xs_type}" minOccurs="0" />'
        )

    cols_joined = "\n                                                    ".join(cols_xml)

    return f"""
                                        <xs:element name="{_safe_name(table_name)}"
                                            msprop:DbTableName="{_safe_name(table_name)}"
                                            msprop:FriendlyName="{_safe_name(table_name)}"
                                            msprop:TableType="Table"
                                            msprop:DbSchemaName="{_safe_name(schema_name)}">
                                            <xs:complexType>
                                                <xs:sequence>
                                                    {cols_joined}
                                                </xs:sequence>
                                            </xs:complexType>
                                        </xs:element>
    """.strip()


def _collect_dsv_tables(schema_snapshot: dict, cube_model: dict, schema_name: str = "dbo") -> list[dict]:
    tables = []

    relational_tables = schema_snapshot.get("tables", []) or []
    if relational_tables:
        for t in relational_tables:
            t_name = t.get("name")
            cols = []
            for c in (t.get("columns", []) or []):
                if c.get("name"):
                    cols.append({
                        "name": c.get("name"),
                        "sql_type": c.get("data_type", "nvarchar"),
                    })

            if t_name and cols:
                tables.append({
                    "name": t_name,
                    "columns": cols,
                    "schema": t.get("schema_name", schema_name),
                })
        return tables

    for dim in cube_model.get("dimensions", []) or []:
        t_name = dim.get("source_table") or dim.get("name")
        cols = []
        for a in dim.get("attributes", []) or []:
            col = a.get("source_column") or a.get("name")
            if col and not any(x["name"] == col for x in cols):
                cols.append({
                    "name": col,
                    "sql_type": a.get("sql_type") or _semantic_sql_type(col, "nvarchar"),
                })
        if t_name and cols:
            tables.append({
                "name": t_name,
                "columns": cols,
                "schema": schema_name,
            })

    for fact in cube_model.get("facts", []) or []:
        t_name = fact.get("source_table") or fact.get("name")
        cols = []
        for m in fact.get("measures", []) or []:
            # pour une mesure calculée, on ne crée pas de colonne physique dans la DSV
            if m.get("is_calculated") and m.get("expression"):
                continue

            col = m.get("source_column") or m.get("column") or m.get("name")
            if col and not any(x["name"] == col for x in cols):
                cols.append({
                    "name": col,
                    "sql_type": m.get("sql_type") or _semantic_sql_type(col, "decimal"),
                })
        if t_name and cols:
            tables.append({
                "name": t_name,
                "columns": cols,
                "schema": schema_name,
            })

    merged = {}
    for t in tables:
        key = t["name"]
        if key not in merged:
            merged[key] = {
                "name": t["name"],
                "schema": t["schema"],
                "columns": [],
            }
        for c in t["columns"]:
            if not any(x["name"] == c["name"] for x in merged[key]["columns"]):
                merged[key]["columns"].append(c)

    return list(merged.values())


def generate_dsv(schema_snapshot: dict, cfg: dict, cube_model: dict) -> str:
    dsv_id = _dsv_id(cfg, cube_model)
    ds_id = _dsv_id(cfg, cube_model)
    schema_name = cfg.get("schema", "dbo")

    tables = _collect_dsv_tables(schema_snapshot, cube_model, schema_name=schema_name)
    tables_xml = "\n".join(
        _build_dsv_table_element(
            table_name=t["name"],
            columns=t["columns"],
            schema_name=t["schema"],
        )
        for t in tables
    )

    return f"""
    <DataSourceViews>
      <DataSourceView>
        <ID>{_safe_name(dsv_id)}</ID>
        <Name>{_safe_name(dsv_id)}</Name>
        <DataSourceID>{_safe_name(ds_id)}</DataSourceID>
        <Schema>
          <xs:schema id="{_safe_name(dsv_id)}"
                     xmlns=""
                     xmlns:xs="http://www.w3.org/2001/XMLSchema"
                     xmlns:msdata="urn:schemas-microsoft-com:xml-msdata"
                     xmlns:msprop="urn:schemas-microsoft-com:xml-msprop">
            <xs:element name="{_safe_name(dsv_id)}" msdata:IsDataSet="true" msdata:UseCurrentLocale="true">
              <xs:complexType>
                <xs:choice minOccurs="0" maxOccurs="unbounded">
{tables_xml}
                </xs:choice>
              </xs:complexType>
            </xs:element>
          </xs:schema>
          <{_safe_name(dsv_id)} xmlns="" />
        </Schema>
      </DataSourceView>
    </DataSourceViews>
    """.strip()


# ------------------------------------------------------------
# Dimensions
# ------------------------------------------------------------

def _generate_database_dimension_attribute(attr: dict, source_table: str, is_key: bool = False) -> str:
    attr_name = attr.get("name", "UnknownAttribute")
    source_column = attr.get("source_column", attr_name)
    sql_type = attr.get("sql_type") or _semantic_sql_type(source_column, "nvarchar")
    data_type = _assl_data_type_from_sql_type(sql_type)

    usage_xml = "<Usage>Key</Usage>" if is_key else ""

    name_column_xml = ""
    if is_key:
        name_column_xml = f"""
        <NameColumn>
          <DataType>WChar</DataType>
          <Source xsi:type="ColumnBinding">
            <TableID>{_safe_name(source_table)}</TableID>
            <ColumnID>{_safe_name(source_column)}</ColumnID>
          </Source>
        </NameColumn>
        """.strip()

    return f"""
    <Attribute>
      <ID>{_safe_name(attr_name)}</ID>
      <Name>{_safe_name(attr_name)}</Name>
      {usage_xml}
      <KeyColumns>
        <KeyColumn>
          <DataType>{_safe_name(data_type)}</DataType>
          <Source xsi:type="ColumnBinding">
            <TableID>{_safe_name(source_table)}</TableID>
            <ColumnID>{_safe_name(source_column)}</ColumnID>
          </Source>
        </KeyColumn>
      </KeyColumns>
      {name_column_xml}
      <OrderBy>Key</OrderBy>
    </Attribute>
    """.strip()


def _generate_database_dimension_hierarchy(h: dict, dim: dict) -> str:
    levels = []

    for lvl in h.get("levels", []) or []:
        level_name = lvl.get("name", "Level")
        source_attr_id = _resolve_hierarchy_source_attribute_id(lvl, dim)

        # ignorer les niveaux non résolus pour éviter l'erreur SSAS
        if not source_attr_id:
            continue

        levels.append(
            f"""
            <Level>
              <ID>{_safe_name(level_name)}</ID>
              <Name>{_safe_name(level_name)}</Name>
              <SourceAttributeID>{_safe_name(source_attr_id)}</SourceAttributeID>
            </Level>
            """.strip()
        )

    # si aucun niveau valide, ne pas générer la hiérarchie
    if not levels:
        return ""

    return f"""
      <Hierarchy>
        <ID>{_safe_name(h.get("name", "Hierarchy"))}</ID>
        <Name>{_safe_name(h.get("name", "Hierarchy"))}</Name>
        <Levels>
          {' '.join(levels)}
        </Levels>
      </Hierarchy>
    """.strip()


def _generate_database_dimension(dim: dict, cfg: dict, cube_model: dict, schema_snapshot: dict) -> str:
    dsv_id = _dsv_id(cfg, cube_model)
    dim_name = dim.get("name", "UnknownDimension")
    source_table = dim.get("source_table", dim_name)
    key_attr = dim.get("key_attribute") or _guess_dimension_key_attribute(dim)

    attrs = dim.get("attributes", []) or []
    if not attrs and key_attr:
        attrs = [{
            "name": key_attr,
            "source_column": key_attr,
            "sql_type": _semantic_sql_type(key_attr),
        }]

    attr_blocks = []
    key_found = False

    for attr in attrs:
        attr_name = attr.get("name")
        attr = {
            **attr,
            "sql_type": attr.get("sql_type") or _semantic_sql_type(attr.get("source_column") or attr_name),
        }
        is_key = bool(key_attr) and attr_name == key_attr
        if is_key:
            key_found = True
        attr_blocks.append(
            _generate_database_dimension_attribute(attr, source_table, is_key=is_key)
        )

    if key_attr and not key_found:
        attr_blocks.insert(
            0,
            _generate_database_dimension_attribute(
                {
                    "name": key_attr,
                    "source_column": key_attr,
                    "sql_type": _semantic_sql_type(key_attr),
                },
                source_table,
                is_key=True,
            ),
        )

    hierarchies = _merge_dimension_hierarchies(dim, schema_snapshot)

    attrs_xml = "\n".join(attr_blocks)
    hier_blocks = []
    for h in hierarchies:
        hx = _generate_database_dimension_hierarchy(h, dim)
        if hx:
            hier_blocks.append(hx)

    hier_xml = "\n".join(hier_blocks)

    return f"""
    <Dimension>
      <ID>{_safe_name(dim_name)}</ID>
      <Name>{_safe_name(dim_name)}</Name>
      <Source xsi:type="DataSourceViewBinding">
        <DataSourceViewID>{_safe_name(dsv_id)}</DataSourceViewID>
      </Source>
      <ErrorConfiguration>
        <KeyNotFound>ReportAndStop</KeyNotFound>
        <KeyDuplicate>ReportAndStop</KeyDuplicate>
        <NullKeyNotAllowed>ReportAndStop</NullKeyNotAllowed>
      </ErrorConfiguration>
      <Language>1036</Language>
      <Collation>French_CI_AS</Collation>
      <UnknownMemberName>Unknown</UnknownMemberName>
      <Attributes>
        {attrs_xml}
      </Attributes>
      <Hierarchies>
        {hier_xml}
      </Hierarchies>
      <ProactiveCaching>
        <SilenceInterval>-PT1S</SilenceInterval>
        <Latency>-PT1S</Latency>
        <SilenceOverrideInterval>-PT1S</SilenceOverrideInterval>
        <ForceRebuildInterval>-PT1S</ForceRebuildInterval>
        <Source xsi:type="ProactiveCachingInheritedBinding" />
      </ProactiveCaching>
    </Dimension>
    """.strip()


def generate_database_dimensions(cube_model: dict, cfg: dict, schema_snapshot: dict) -> str:
    dims = cube_model.get("dimensions", []) or []
    dims_xml = "\n".join(
        _generate_database_dimension(d, cfg, cube_model, schema_snapshot)
        for d in dims
    )
    return f"""
    <Dimensions>
      {dims_xml}
    </Dimensions>
    """.strip()

def _generate_cube_dimension(dim: dict, schema_snapshot: dict) -> str:
    dim_name = dim.get("name", "UnknownDimension")
    attrs = dim.get("attributes", []) or []
    hierarchies = _merge_dimension_hierarchies(dim, schema_snapshot)

    attrs_xml = "\n".join(
        f"""
        <Attribute>
          <AttributeID>{_safe_name(a.get("name", "UnknownAttribute"))}</AttributeID>
        </Attribute>
        """.strip()
        for a in attrs
    )

    hier_xml = "\n".join(
        f"""
        <Hierarchy>
          <HierarchyID>{_safe_name(h.get("name", "Hierarchy"))}</HierarchyID>
        </Hierarchy>
        """.strip()
        for h in hierarchies
    )

    return f"""
    <Dimension>
      <ID>{_safe_name(dim_name)}</ID>
      <Name>{_safe_name(dim_name)}</Name>
      <DimensionID>{_safe_name(dim_name)}</DimensionID>
      <Attributes>
        {attrs_xml}
      </Attributes>
      <Hierarchies>
        {hier_xml}
      </Hierarchies>
    </Dimension>
    """.strip()


def _generate_cube_dimension(dim: dict, schema_snapshot: dict) -> str:
    dim_name = dim.get("name", "UnknownDimension")
    attrs = dim.get("attributes", []) or []
    hierarchies = _merge_dimension_hierarchies(dim, schema_snapshot)

    attrs_xml = "\n".join(
        f"""
        <Attribute>
          <AttributeID>{_safe_name(a.get("name", "UnknownAttribute"))}</AttributeID>
        </Attribute>
        """.strip()
        for a in attrs
    )

    hier_xml = "\n".join(
        f"""
        <Hierarchy>
          <HierarchyID>{_safe_name(h.get("name", "Hierarchy"))}</HierarchyID>
        </Hierarchy>
        """.strip()
        for h in hierarchies
    )

    return f"""
    <Dimension>
      <ID>{_safe_name(dim_name)}</ID>
      <Name>{_safe_name(dim_name)}</Name>
      <DimensionID>{_safe_name(dim_name)}</DimensionID>
      <Attributes>
        {attrs_xml}
      </Attributes>
      <Hierarchies>
        {hier_xml}
      </Hierarchies>
    </Dimension>
    """.strip()


def generate_cube_dimensions(cube_model: dict, schema_snapshot: dict) -> str:
    dims = cube_model.get("dimensions", []) or []
    dims_xml = "\n".join(_generate_cube_dimension(d, schema_snapshot) for d in dims)
    return f"""
    <Dimensions>
      {dims_xml}
    </Dimensions>
    """.strip()

# ------------------------------------------------------------
# Measures / Measure Groups
# ------------------------------------------------------------

def _measure_sql_type(measure: dict) -> str:
    col = measure.get("source_column") or measure.get("column") or measure.get("name")
    return measure.get("sql_type") or _semantic_sql_type(col, "decimal")


def _generate_measure(measure: dict, fact_name: str) -> str:
    # IMPORTANT: une mesure calculée ne doit pas être générée comme colonne physique
    if measure.get("is_calculated") and measure.get("expression"):
        return ""

    measure_name = measure.get("name", "UnnamedMeasure")
    source_col = measure.get("source_column") or measure.get("column") or measure_name
    sql_type = _measure_sql_type(measure)
    data_type = _measure_assl_data_type_from_sql_type(sql_type)
    agg = _agg_to_assl(measure.get("aggregation") or measure.get("agg"))

    measure_id = f"{_sanitize_id(fact_name)}_{_sanitize_id(measure_name)}"
    visible_name = f"{_friendly_fact_prefix(fact_name)} - {measure_name}"

    agg_xml = f"<AggregateFunction>{agg}</AggregateFunction>" if agg != "Sum" else ""

    return f"""
    <Measure>
      <ID>{_safe_name(measure_id)}</ID>
      <Name>{_safe_name(visible_name)}</Name>
      {agg_xml}
      <DataType>{_safe_name(data_type)}</DataType>
      <Source>
        <DataType>{_safe_name(data_type)}</DataType>
        <Source xsi:type="ColumnBinding">
          <TableID>{_safe_name(fact_name)}</TableID>
          <ColumnID>{_safe_name(source_col)}</ColumnID>
        </Source>
      </Source>
    </Measure>
    """.strip()


def _generate_measure_group_dimension(dim: dict, fact_name: str, schema_snapshot: dict) -> str:
    dim_name = dim.get("name", "UnknownDimension")
    key_attr = dim.get("key_attribute", "Key")

    granularity_col = _resolve_granularity_column(schema_snapshot, fact_name, dim)

    fact_table = _get_table(schema_snapshot, fact_name)
    granularity_sql_type = "int"
    if fact_table:
        for c in fact_table.get("columns", []):
            if c.get("name") == granularity_col:
                granularity_sql_type = c.get("data_type", "int")
                break
    else:
        granularity_sql_type = _semantic_sql_type(granularity_col, "int")

    granularity_assl_type = _assl_data_type_from_sql_type(granularity_sql_type)

    attr_blocks = [
        f"""
        <Attribute>
          <AttributeID>{_safe_name(key_attr)}</AttributeID>
          <KeyColumns>
            <KeyColumn>
              <DataType>{_safe_name(granularity_assl_type)}</DataType>
              <Source xsi:type="ColumnBinding">
                <TableID>{_safe_name(fact_name)}</TableID>
                <ColumnID>{_safe_name(granularity_col)}</ColumnID>
              </Source>
            </KeyColumn>
          </KeyColumns>
          <Type>Granularity</Type>
        </Attribute>
        """.strip()
    ]

    return f"""
    <Dimension xsi:type="RegularMeasureGroupDimension">
      <CubeDimensionID>{_safe_name(dim_name)}</CubeDimensionID>
      <Cardinality>One</Cardinality>
      <Attributes>
        {' '.join(attr_blocks)}
      </Attributes>
    </Dimension>
    """.strip()
def _available_dimension_attribute_ids(dim: dict) -> dict:
    """
    Retourne un mapping normalisé -> vrai ID d'attribut de la dimension.
    On mappe à la fois le name et le source_column.
    """
    aliases = {}

    for attr in dim.get("attributes", []) or []:
        attr_name = attr.get("name")
        source_col = attr.get("source_column", attr_name)

        if attr_name:
            aliases[_normalize_name(attr_name)] = attr_name
        if source_col:
            aliases[_normalize_name(source_col)] = attr_name or source_col

    return aliases


def _resolve_hierarchy_source_attribute_id(level: dict, dim: dict) -> str | None:
    """
    Résout un niveau vers un AttributeID réellement existant dans la dimension.
    """
    aliases = _available_dimension_attribute_ids(dim)

    candidates = [
        level.get("source_attribute_id"),
        level.get("source_column"),
        level.get("name"),
    ]

    for cand in candidates:
        if not cand:
            continue
        resolved = aliases.get(_normalize_name(cand))
        if resolved:
            return resolved

    return None

def _generate_partition(fact: dict, cfg: dict, cube_model: dict) -> str:
    fact_name = fact.get("name", "UnnamedFact")
    dsv_id = _dsv_id(cfg, cube_model)

    return f"""
    <Partitions>
      <Partition>
        <ID>{_safe_name(fact_name)}</ID>
        <Name>{_safe_name(fact_name)}</Name>
        <Source xsi:type="DsvTableBinding">
          <DataSourceViewID>{_safe_name(dsv_id)}</DataSourceViewID>
          <TableID>{_safe_name(fact_name)}</TableID>
        </Source>
        <StorageMode>Molap</StorageMode>
        <ProcessingMode>Regular</ProcessingMode>
        <ProactiveCaching>
          <SilenceInterval>-PT1S</SilenceInterval>
          <Latency>-PT1S</Latency>
          <SilenceOverrideInterval>-PT1S</SilenceOverrideInterval>
          <ForceRebuildInterval>-PT1S</ForceRebuildInterval>
          <Source xsi:type="ProactiveCachingInheritedBinding" />
        </ProactiveCaching>
      </Partition>
    </Partitions>
    """.strip()


def _merge_dimension_hierarchies(dim: dict, schema_snapshot: dict) -> list[dict]:
    current = dim.get("hierarchies", []) or []
    if current:
        return current

    dim_name = dim.get("name")
    natural = (schema_snapshot.get("natural_hierarchies", {}) or {}).get(dim_name, []) or []

    merged = []
    for h in natural:
        levels = []
        for lvl in h.get("levels", []) or []:
            lvl_name = lvl.get("name") if isinstance(lvl, dict) else str(lvl)
            if lvl_name:
                levels.append({
                    "name": lvl_name,
                    "source_column": lvl_name,
                })

        if levels:
            merged.append({
                "name": h.get("name") or h.get("mdx_name") or "Hierarchy",
                "levels": levels,
            })

    return merged

def _dimension_applies_to_fact(schema_snapshot: dict, fact_name: str, dim: dict) -> bool:
    fact_table = _get_table(schema_snapshot, fact_name)
    if not fact_table:
        return False

    granularity_col = _resolve_granularity_column(schema_snapshot, fact_name, dim)
    if not granularity_col:
        return False

    fact_cols = {c.get("name") for c in fact_table.get("columns", []) if c.get("name")}
    return granularity_col in fact_cols

def _generate_measure_group(fact: dict, cube_model: dict, cfg: dict, schema_snapshot: dict) -> str:
    fact_name = fact.get("name", "UnnamedFact")
    measures = fact.get("measures", []) or []
    all_dimensions = cube_model.get("dimensions", []) or []
    dimensions = [
        d for d in all_dimensions
        if _dimension_applies_to_fact(schema_snapshot, fact_name, d)
    ]

    measure_blocks = []
    for m in measures:
        xml = _generate_measure(m, fact_name)
        if xml.strip():
            measure_blocks.append(xml)

    measures_xml = "\n".join(measure_blocks)
    mg_dims_xml = "\n".join(
        _generate_measure_group_dimension(d, fact_name, schema_snapshot)
        for d in dimensions
    )
    partition_xml = _generate_partition(fact, cfg, cube_model)

    return f"""
    <MeasureGroup>
      <ID>{_safe_name(fact_name)}</ID>
      <Name>{_safe_name(fact_name)}</Name>
      <Measures>
        {measures_xml}
      </Measures>
      <StorageMode>Molap</StorageMode>
      <ProcessingMode>Regular</ProcessingMode>
      <Dimensions>
        {mg_dims_xml}
      </Dimensions>
      {partition_xml}
      <ProactiveCaching>
        <SilenceInterval>-PT1S</SilenceInterval>
        <Latency>-PT1S</Latency>
        <SilenceOverrideInterval>-PT1S</SilenceOverrideInterval>
        <ForceRebuildInterval>-PT1S</ForceRebuildInterval>
        <Source xsi:type="ProactiveCachingInheritedBinding" />
      </ProactiveCaching>
    </MeasureGroup>
    """.strip()


def generate_measure_groups(cube_model: dict, cfg: dict, schema_snapshot: dict) -> str:
    facts = cube_model.get("facts", []) or []
    groups_xml = "\n".join(
        _generate_measure_group(f, cube_model, cfg, schema_snapshot)
        for f in facts
    )
    return f"""
    <MeasureGroups>
      {groups_xml}
    </MeasureGroups>
    """.strip()


# ------------------------------------------------------------
# Cube
# ------------------------------------------------------------

def generate_cube_source(cfg: dict, cube_model: dict) -> str:
    dsv_id = _dsv_id(cfg, cube_model)
    return f"""
    <Source>
      <DataSourceViewID>{_safe_name(dsv_id)}</DataSourceViewID>
    </Source>
    """.strip()


def _generate_calculated_members(cube_model: dict) -> str:
    lines = ["CALCULATE;"]

    for fact in cube_model.get("facts", []) or []:
        for measure in fact.get("measures", []) or []:
            if measure.get("is_calculated") and measure.get("expression"):
                measure_name = measure.get("name", "CalculatedMeasure")
                expression = measure.get("expression", "")
                lines.append(
                    f'CREATE MEMBER CURRENTCUBE.[Measures].[{measure_name}] AS {expression};'
                )

    return "\n".join(lines)


def generate_mdx_script(cube_model: dict) -> str:
    mdx_text = _generate_calculated_members(cube_model)
    return f"""
    <MdxScripts>
      <MdxScript>
        <ID>MdxScript</ID>
        <Name>MdxScript</Name>
        <Commands>
          <Command>
            <Text>{_safe_name(mdx_text)}</Text>
          </Command>
        </Commands>
      </MdxScript>
    </MdxScripts>
    """.strip()


def generate_cube(cube_model: dict, cfg: dict, schema_snapshot: dict) -> str:
    cube_id = _cube_id(cfg, cube_model)
    cube_name = _cube_id(cfg, cube_model)
    description = cube_model.get("description", "")

    cube_dims = generate_cube_dimensions(cube_model, schema_snapshot)
    measure_groups = generate_measure_groups(cube_model, cfg, schema_snapshot)
    source_xml = generate_cube_source(cfg, cube_model)
    mdx_script = generate_mdx_script(cube_model)

    return f"""
    <Cubes>
      <Cube>
        <ID>{_safe_name(cube_id)}</ID>
        <Name>{_safe_name(cube_name)}</Name>
        <Description>{_safe_name(description)}</Description>
        <Language>1036</Language>
        <Collation>French_CI_AS</Collation>
        {cube_dims}
        {measure_groups}
        {source_xml}
        {mdx_script}
        <ProactiveCaching>
          <SilenceInterval>-PT1S</SilenceInterval>
          <Latency>-PT1S</Latency>
          <SilenceOverrideInterval>-PT1S</SilenceOverrideInterval>
          <ForceRebuildInterval>-PT1S</ForceRebuildInterval>
          <Source xsi:type="ProactiveCachingInheritedBinding" />
        </ProactiveCaching>
      </Cube>
    </Cubes>
    """.strip()


# ------------------------------------------------------------
# Entry point
# ------------------------------------------------------------

def generate_xmla(cube_model: Any, schema_snapshot: Any = None, cfg: Optional[dict] = None) -> str:
    cube_dict = _as_dict(cube_model)
    schema_dict = _as_dict(schema_snapshot)
    cfg = cfg or {}

    db_header = generate_database_header(cube_dict, cfg)
    datasource = generate_datasource(cfg, cube_dict)
    dsv = generate_dsv(schema_dict, cfg, cube_dict)
    database_dims = generate_database_dimensions(cube_dict, cfg, schema_dict)
    cubes_xml = generate_cube(cube_dict, cfg, schema_dict)

    return f"""<Create xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">
  <ObjectDefinition>
    <Database
      xmlns:xsd="http://www.w3.org/2001/XMLSchema"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xmlns:ddl200="http://schemas.microsoft.com/analysisservices/2010/engine/200">
      {db_header}
      {datasource}
      {dsv}
      {database_dims}
      {cubes_xml}
    </Database>
  </ObjectDefinition>
</Create>"""
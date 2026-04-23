from __future__ import annotations

from typing import List, Optional

from models import (
    ColumnSchema,
    CubeDimension,
    CubeFact,
    CubeMeasure,
    DetectedAttribute,
    DetectedHierarchy,
    HierarchyLevel,
    SchemaSnapshot,
    TableSchema,
)


NUMERIC_TYPES = {
    "int", "bigint", "smallint", "tinyint", "decimal", "numeric",
    "float", "real", "money", "smallmoney"
}
DATE_TYPES = {"date", "datetime", "datetime2", "smalldatetime"}


def _normalize_type(data_type: str) -> str:
    return (data_type or "").lower().strip()


def is_numeric_column(column: ColumnSchema) -> bool:
    return _normalize_type(column.data_type) in NUMERIC_TYPES


def is_date_column(column: ColumnSchema) -> bool:
    return _normalize_type(column.data_type) in DATE_TYPES


def guess_fact_tables(snapshot: SchemaSnapshot) -> List[TableSchema]:
    results: List[TableSchema] = []

    for table in snapshot.tables:
        numeric_count = sum(1 for col in table.columns if is_numeric_column(col))
        fk_count = sum(1 for col in table.columns if col.is_fk)
        if numeric_count >= 2 and fk_count >= 1:
            results.append(table)

    return results


def guess_dimension_tables(snapshot: SchemaSnapshot) -> List[TableSchema]:
    results: List[TableSchema] = []

    fact_names = {t.name for t in guess_fact_tables(snapshot)}
    for table in snapshot.tables:
        if table.name in fact_names:
            continue
        results.append(table)

    return results


def detect_measures(table: TableSchema) -> List[CubeMeasure]:
    measures: List[CubeMeasure] = []

    for col in table.columns:
        if col.is_pk or col.is_fk:
            continue
        if not is_numeric_column(col):
            continue

        measures.append(
            CubeMeasure(
                name=col.name,
                source_table=table.name,
                source_column=col.name,
                aggregation="sum",
                is_calculated=False,
                description=f"Mesure détectée automatiquement depuis {table.name}.{col.name}",
            )
        )

    return measures


def _guess_key_attribute(table: TableSchema) -> Optional[str]:
    for col in table.columns:
        if col.is_pk:
            return col.name
    if table.columns:
        return table.columns[0].name
    return None


def _build_date_hierarchy(table: TableSchema) -> List[DetectedHierarchy]:
    col_names = {c.name for c in table.columns}

    level_map = [
        ("Year", "YearNumber"),
        ("Quarter", "QuarterNumber"),
        ("Month", "MonthNumber"),
        ("Day", "DayNumber"),
    ]

    levels: List[HierarchyLevel] = []
    for level_name, source_col in level_map:
        if source_col in col_names:
            levels.append(
                HierarchyLevel(
                    name=level_name,
                    source_column=source_col,
                )
            )

    if len(levels) < 2:
        return []

    return [
        DetectedHierarchy(
            name="DateHierarchy",
            levels=levels,
        )
    ]


def build_dimensions_from_table(table: TableSchema) -> Optional[CubeDimension]:
    key_attr = _guess_key_attribute(table)
    if not key_attr:
        return None

    is_date_dimension = table.name.lower() == "dimdate"

    date_allowed = {
        "DateKey",
        "FullDate",
        "DayNumber",
        "MonthNumber",
        "MonthName",
        "QuarterNumber",
        "YearNumber",
        "WeekDayNumber",
        "WeekDayName",
    }

    attributes = []
    for col in table.columns:
        if is_date_dimension:
            if col.name in date_allowed:
                attributes.append(
                    DetectedAttribute(
                        name=col.name,
                        source_table=table.name,
                        source_column=col.name,
                        description=f"Attribut issu de {table.name}.{col.name}",
                    )
                )
        else:
            if not is_numeric_column(col) or col.name == key_attr:
                attributes.append(
                    DetectedAttribute(
                        name=col.name,
                        source_table=table.name,
                        source_column=col.name,
                        description=f"Attribut issu de {table.name}.{col.name}",
                    )
                )

    return CubeDimension(
        name=table.name,
        source_table=table.name,
        key_attribute=key_attr,
        attributes=attributes,
        hierarchies=_build_date_hierarchy(table),
        description=f"Dimension générée automatiquement depuis {table.name}",
    )

def build_facts(snapshot: SchemaSnapshot) -> List[CubeFact]:
    facts: List[CubeFact] = []
    for table in guess_fact_tables(snapshot):
        measures = detect_measures(table)
        if not measures:
            continue
        facts.append(
            CubeFact(
                name=table.name,
                source_table=table.name,
                measures=measures,
                description=f"Fait détecté automatiquement depuis {table.name}",
            )
        )
    return facts


def build_dimensions(snapshot: SchemaSnapshot) -> List[CubeDimension]:
    dimensions: List[CubeDimension] = []
    for table in guess_dimension_tables(snapshot):
        dim = build_dimensions_from_table(table)
        if dim:
            dimensions.append(dim)
    return dimensions
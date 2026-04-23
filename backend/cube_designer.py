from __future__ import annotations

from typing import Any, List

from models import (
    CubeDimension,
    CubeFact,
    CubeMeasure,
    CubeModel,
    DetectedAttribute,
    DetectedHierarchy,
    HierarchyLevel,
    UserIntent,
    SchemaSnapshot,
)
from schema_analyzer import build_dimensions, build_facts


def _normalize(value: str) -> str:
    return "".join(ch.lower() for ch in (value or "") if ch.isalnum())


def _as_mapping(snapshot: Any) -> dict:
    if hasattr(snapshot, "model_dump"):
        return snapshot.model_dump()
    if isinstance(snapshot, dict):
        return snapshot
    return {}


def _as_schema_snapshot(snapshot: Any):
    if isinstance(snapshot, SchemaSnapshot):
        return snapshot

    if hasattr(snapshot, "model_dump"):
        snapshot = snapshot.model_dump()

    if isinstance(snapshot, dict) and "tables" in snapshot:
        try:
            payload = {
                "database_name": snapshot.get("database_name", "unknown_database"),
                "tables": snapshot.get("tables", []),
                "ssas_dimensions": snapshot.get("ssas_dimensions", {}),
                "schema": snapshot.get("schema", "dbo"),
            }
            return SchemaSnapshot(**payload)
        except Exception:
            return None

    return None


def cube_model_from_registry(cube_model_data: dict) -> CubeModel:
    return CubeModel(**cube_model_data)


def _build_cube_name(intent: UserIntent, snapshot_data: dict) -> str:
    if intent.cube_name:
        return intent.cube_name
    return "AutoCube"


def _build_description(intent: UserIntent, facts: List[CubeFact], dimensions: List[CubeDimension]) -> str:
    fact_names = ", ".join(f.name for f in facts[:3]) or "faits détectés"
    dim_names = ", ".join(d.name for d in dimensions[:4]) or "dimensions détectées"

    if intent.description_hint:
        return (
            f"Cube OLAP généré automatiquement à partir d'un DW relationnel. "
            f"Intention utilisateur: {intent.description_hint}. "
            f"Faits sélectionnés: {fact_names}. Dimensions sélectionnées: {dim_names}."
        )

    return (
        f"Cube OLAP généré automatiquement à partir d'un DW relationnel. "
        f"Faits sélectionnés: {fact_names}. Dimensions sélectionnées: {dim_names}."
    )


def _filter_requested_measures(facts: List[CubeFact], requested: List[str]) -> List[CubeFact]:
    if not requested:
        return facts

    requested_set = {_normalize(x) for x in requested if x}
    filtered_facts: List[CubeFact] = []

    for fact in facts:
        measures = [
            m for m in fact.measures
            if any(token in _normalize(m.name) for token in requested_set)
        ]
        if measures:
            filtered_facts.append(
                CubeFact(
                    name=fact.name,
                    source_table=fact.source_table,
                    measures=measures,
                    description=fact.description,
                )
            )

    return filtered_facts or facts


def _filter_requested_dimensions(dimensions: List[CubeDimension], requested: List[str]) -> List[CubeDimension]:
    if not requested:
        return dimensions

    requested_set = {_normalize(x) for x in requested if x}
    filtered = [
        d for d in dimensions
        if any(token in _normalize(d.name) for token in requested_set)
    ]
    return filtered or dimensions


def _build_semantic_fact_fk_attributes(snapshot_data: dict, fact_name: str) -> List[DetectedAttribute]:
    attrs: List[DetectedAttribute] = []

    for table in snapshot_data.get("tables", []) or []:
        if table.get("name") != fact_name:
            continue

        for col in table.get("columns", []) or []:
            col_name = col.get("name")
            if not col_name:
                continue

            if col.get("is_fk"):
                attrs.append(
                    DetectedAttribute(
                        name=col_name,
                        source_table=fact_name,
                        source_column=col_name,
                        description=f"Clé étrangère issue de {fact_name}.{col_name}",
                    )
                )
        break

    return attrs


def _build_from_semantic_snapshot(snapshot_data: dict) -> tuple[list[CubeFact], list[CubeDimension]]:
    facts: List[CubeFact] = []
    dimensions: List[CubeDimension] = []

    natural_map = snapshot_data.get("natural_hierarchies", {}) or {}

    for fact in snapshot_data.get("facts", []) or []:
        measures = []
        for m in fact.get("measures", []) or []:
            measures.append(
                CubeMeasure(
                    name=m.get("name", m.get("column", "UnnamedMeasure")),
                    source_table=fact.get("source_table", fact.get("name", "")),
                    source_column=m.get("column", m.get("source_column", "")),
                    aggregation=(m.get("agg") or m.get("aggregation") or "sum").lower(),
                    is_calculated=False,
                    description=(
                        f"Mesure issue de "
                        f"{fact.get('source_table', fact.get('name', ''))}."
                        f"{m.get('column', m.get('source_column', ''))}"
                    ),
                )
            )

        facts.append(
            CubeFact(
                name=fact.get("name", "UnnamedFact"),
                source_table=fact.get("source_table", fact.get("name", "UnnamedFact")),
                measures=measures,
                description=f"Fait généré depuis le snapshot sémantique: {fact.get('name', 'UnnamedFact')}",
            )
        )

    for dim in snapshot_data.get("dimensions", []) or []:
        attributes = []
        for attr in dim.get("attributes", []) or []:
            attributes.append(
                DetectedAttribute(
                    name=attr.get("name", "UnnamedAttribute"),
                    source_table=dim.get("source_table", dim.get("name", "")),
                    source_column=attr.get("source_column", attr.get("name", "")),
                    description=(
                        f"Attribut issu de "
                        f"{dim.get('source_table', dim.get('name', ''))}."
                        f"{attr.get('name', attr.get('source_column', ''))}"
                    ),
                )
            )

        hierarchies = []
        for h in natural_map.get(dim.get("name", ""), []) or []:
            levels = []
            for level in h.get("levels", []) or []:
                if isinstance(level, dict):
                    level_name = level.get("name", "UnnamedLevel")
                    source_col = level.get("source_column", level_name)
                else:
                    level_name = str(level)
                    source_col = level_name

                levels.append(
                    HierarchyLevel(
                        name=level_name,
                        source_column=source_col,
                    )
                )

            hierarchies.append(
                DetectedHierarchy(
                    name=h.get("name", "UnnamedHierarchy"),
                    levels=levels,
                )
            )

        key_attr = dim.get("key_attribute")
        if not key_attr and attributes:
            key_attr = attributes[0].name
        if not key_attr:
            key_attr = "Key"

        dimensions.append(
            CubeDimension(
                name=dim.get("name", "UnnamedDimension"),
                source_table=dim.get("source_table", dim.get("name", "UnnamedDimension")),
                key_attribute=key_attr,
                attributes=attributes,
                hierarchies=hierarchies,
                description=f"Dimension générée depuis le snapshot sémantique: {dim.get('name', 'UnnamedDimension')}",
            )
        )

    return facts, dimensions


def _dimension_match_score(dim: CubeDimension, fact: CubeFact, snapshot_data: dict) -> int:
    score = 0

    fact_table_name = fact.source_table or fact.name
    dim_table_name = dim.source_table or dim.name

    for table in snapshot_data.get("tables", []) or []:
        if table.get("name") != fact_table_name:
            continue

        for col in table.get("columns", []) or []:
            if not col.get("is_fk"):
                continue

            if col.get("references_table") == dim_table_name:
                score += 20

            if col.get("name") == dim.key_attribute:
                score += 10

        break

    if _normalize(dim.name) == "dimdate":
        score += 3
    if _normalize(dim.name) == "dimproduct":
        score += 2

    return score


def _select_best_dimensions_for_fact(dimensions: List[CubeDimension], fact: CubeFact, snapshot_data: dict) -> List[CubeDimension]:
    ranked = sorted(
        dimensions,
        key=lambda d: (_dimension_match_score(d, fact, snapshot_data), d.name),
        reverse=True,
    )

    useful = [d for d in ranked if _dimension_match_score(d, fact, snapshot_data) > 0]
    return useful or ranked


def _apply_constraints(
    facts: List[CubeFact],
    dimensions: List[CubeDimension],
    intent: UserIntent,
    snapshot_data: dict,
) -> tuple[List[CubeFact], List[CubeDimension]]:
    selected_facts = facts
    selected_dimensions = dimensions

    if intent.max_facts is not None and intent.max_facts > 0:
        selected_facts = facts[:intent.max_facts]

    if intent.max_dimensions is not None and intent.max_dimensions > 0:
        if len(selected_facts) == 1:
            ranked_dims = _select_best_dimensions_for_fact(dimensions, selected_facts[0], snapshot_data)
            selected_dimensions = ranked_dims[:intent.max_dimensions]
        else:
            selected_dimensions = dimensions[:intent.max_dimensions]

    return selected_facts, selected_dimensions


def create_cube_model(snapshot: Any, intent: UserIntent) -> CubeModel:
    snapshot_data = _as_mapping(snapshot)
    schema_snapshot = _as_schema_snapshot(snapshot)

    if snapshot_data.get("facts") or snapshot_data.get("dimensions"):
        facts, dimensions = _build_from_semantic_snapshot(snapshot_data)
    elif schema_snapshot is not None and getattr(schema_snapshot, "tables", None):
        facts = build_facts(schema_snapshot)
        dimensions = build_dimensions(schema_snapshot)
    else:
        facts, dimensions = [], []

    # =========================================================
    # LOGIQUE DE FILTRAGE
    # =========================================================
    # Si l'utilisateur demande juste "créer un cube nommé X",
    # on garde tous les faits et toutes les dimensions.
    #
    # On ne filtre que si :
    # - il a explicitement demandé certaines mesures/dimensions
    # - ou il a imposé max_facts / max_dimensions
    # =========================================================

    selected_facts = facts
    selected_dimensions = dimensions

    has_explicit_measure_filter = bool(intent.requested_measures)
    has_explicit_dimension_filter = bool(intent.requested_dimensions)
    has_explicit_count_constraint = (
        intent.max_facts is not None or intent.max_dimensions is not None
    )

    if intent.intent.value == "create_cube":
        if has_explicit_measure_filter:
            selected_facts = _filter_requested_measures(selected_facts, intent.requested_measures)

        if has_explicit_dimension_filter:
            selected_dimensions = _filter_requested_dimensions(selected_dimensions, intent.requested_dimensions)

        if has_explicit_count_constraint:
            selected_facts, selected_dimensions = _apply_constraints(
                selected_facts,
                selected_dimensions,
                intent,
                snapshot_data,
            )
    else:
        # Pour les autres intentions, on garde l'ancien comportement
        selected_facts = _filter_requested_measures(selected_facts, intent.requested_measures)
        selected_dimensions = _filter_requested_dimensions(selected_dimensions, intent.requested_dimensions)
        selected_facts, selected_dimensions = _apply_constraints(
            selected_facts,
            selected_dimensions,
            intent,
            snapshot_data,
        )

    cube_name = _build_cube_name(intent, snapshot_data)
    description = _build_description(intent, selected_facts, selected_dimensions)

    return CubeModel(
        cube_name=cube_name,
        description=description,
        facts=selected_facts,
        dimensions=selected_dimensions,
        metadata={
            "intent": intent.intent.value,
            "database_name": snapshot_data.get("database_name", "unknown"),
            "fact_count": len(selected_facts),
            "dimension_count": len(selected_dimensions),
        },
    )
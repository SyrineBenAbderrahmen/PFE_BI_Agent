from __future__ import annotations

import re
from typing import Optional

from models import (
    CubeModel,
    CubeFact,
    CubeMeasure,
    CubeDimension,
    DetectedAttribute,
    DetectedHierarchy,
    HierarchyLevel,
    UserIntent,
)


def _norm(text: str) -> str:
    return "".join(ch.lower() for ch in (text or "") if ch.isalnum())


def _find_dimension(cube: CubeModel, hint: str | None = None) -> Optional[CubeDimension]:
    if not cube.dimensions:
        return None

    if hint:
        hint_n = _norm(hint)
        for dim in cube.dimensions:
            if hint_n in _norm(dim.name):
                return dim

    return cube.dimensions[0]


def _extract_formula(prompt: str) -> tuple[Optional[str], Optional[str]]:
    m = re.search(
        r"(?:mesure|measure)\s+([A-Za-z_À-ÿ][A-Za-z0-9_À-ÿ]*)\s*=\s*(.+)$",
        prompt,
        flags=re.IGNORECASE,
    )
    if not m:
        return None, None
    return m.group(1).strip(), m.group(2).strip()


def _extract_formula_tokens(expression: str) -> list[str]:
    if not expression:
        return []
    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expression)
    blacklist = {"sum", "avg", "count", "min", "max", "and", "or", "not"}
    return [t for t in tokens if t.lower() not in blacklist]


def _measure_exists_by_name_or_source(
    fact: CubeFact,
    measure_name: str,
    source_column: str,
) -> Optional[CubeMeasure]:
    target_name = _norm(measure_name)
    target_source = _norm(source_column)

    for m in fact.measures:
        if _norm(m.name) == target_name:
            return m
        if _norm(m.source_column) == target_source:
            return m

    return None


def _find_best_fact_for_formula(cube: CubeModel, expression: str) -> Optional[CubeFact]:
    tokens = _extract_formula_tokens(expression)
    if not tokens:
        return cube.facts[0] if cube.facts else None

    best_fact = None
    best_score = -1

    for fact in cube.facts:
        score = 0
        fact_measure_names = {_norm(m.name) for m in fact.measures}
        fact_measure_sources = {_norm(m.source_column) for m in fact.measures}

        for token in tokens:
            token_n = _norm(token)
            if token_n in fact_measure_names or token_n in fact_measure_sources:
                score += 1

        if score > best_score:
            best_score = score
            best_fact = fact

    return best_fact


def add_measure_to_cube(
    cube: CubeModel,
    intent: UserIntent,
) -> tuple[CubeModel, CubeFact | None, CubeMeasure | None, bool, str | None]:
    requested = intent.requested_measures or []
    prompt = intent.extra_instructions or intent.description_hint or ""

    calc_name, calc_expr = _extract_formula(prompt)

    # Cas mesure calculée
    if calc_name and calc_expr:
        target_fact = _find_best_fact_for_formula(cube, calc_expr)
        if not target_fact:
            return cube, None, None, False, "Impossible d'identifier le fait cible pour la formule."

        existing = _measure_exists_by_name_or_source(target_fact, calc_name, calc_name)
        if existing:
            return (
                cube,
                target_fact,
                existing,
                True,
                f"La mesure '{existing.name}' existe déjà dans le cube '{cube.cube_name}'.",
            )

        new_measure = CubeMeasure(
            name=calc_name,
            source_table=target_fact.source_table,
            source_column=calc_name,
            aggregation="sum",
            expression=calc_expr,
            is_calculated=True,
            description=f"Mesure calculée ajoutée automatiquement: {calc_expr}",
        )
        target_fact.measures.append(new_measure)
        return cube, target_fact, new_measure, False, None

    # Cas mesure simple
    if not requested:
        return cube, None, None, False, "Aucune mesure demandée n'a été détectée."

    measure_token = requested[0]

    target_fact = cube.facts[0] if cube.facts else None
    if not target_fact:
        return cube, None, None, False, "Impossible d'identifier le fait cible."

    existing = _measure_exists_by_name_or_source(target_fact, measure_token, measure_token)
    if existing:
        return (
            cube,
            target_fact,
            existing,
            True,
            f"La mesure '{existing.name}' existe déjà dans le cube '{cube.cube_name}'.",
        )

    new_measure = CubeMeasure(
        name=measure_token,
        source_table=target_fact.source_table,
        source_column=measure_token,
        aggregation="sum",
        is_calculated=False,
        description=f"Mesure ajoutée automatiquement depuis {target_fact.source_table}.{measure_token}",
    )
    target_fact.measures.append(new_measure)
    return cube, target_fact, new_measure, False, None


def modify_dimension_in_cube(cube: CubeModel, intent: UserIntent) -> tuple[CubeModel, CubeDimension | None]:
    requested_dims = intent.requested_dimensions or []
    target_dim = _find_dimension(cube, requested_dims[0] if requested_dims else None)
    if not target_dim:
        return cube, None

    prompt = (intent.extra_instructions or intent.description_hint or "").lower()

    if "hiérarchie" in prompt or "hierarchie" in prompt or "hierarchy" in prompt:
        if len(target_dim.attributes) >= 2:
            existing = {_norm(h.name) for h in target_dim.hierarchies}
            hierarchy_name = f"{target_dim.name}Hierarchy"

            if _norm(hierarchy_name) not in existing:
                levels = []
                for attr in target_dim.attributes[:3]:
                    levels.append(
                        HierarchyLevel(
                            name=attr.name,
                            source_column=attr.source_column,
                        )
                    )

                target_dim.hierarchies.append(
                    DetectedHierarchy(
                        name=hierarchy_name,
                        levels=levels,
                    )
                )

    if "attribut" in prompt or "attribute" in prompt:
        existing_attrs = {_norm(a.name) for a in target_dim.attributes}
        candidate_name = "NewAttribute"

        if candidate_name.lower() not in existing_attrs:
            target_dim.attributes.append(
                DetectedAttribute(
                    name=candidate_name,
                    source_table=target_dim.source_table,
                    source_column=candidate_name,
                    description=f"Attribut ajouté automatiquement dans {target_dim.name}",
                )
            )

    return cube, target_dim
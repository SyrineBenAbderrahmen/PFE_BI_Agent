from __future__ import annotations

from models import CubeModel, ValidationIssue, ValidationResult


def validate_cube_model(cube: CubeModel) -> ValidationResult:
    issues = []

    if not cube.cube_name.strip():
        issues.append(
            ValidationIssue(
                level="error",
                code="EMPTY_CUBE_NAME",
                message="Le nom du cube est vide.",
            )
        )

    if not cube.facts:
        issues.append(
            ValidationIssue(
                level="error",
                code="NO_FACTS",
                message="Aucune table de faits n'a été détectée ou sélectionnée.",
            )
        )

    if not cube.dimensions:
        issues.append(
            ValidationIssue(
                level="warning",
                code="NO_DIMENSIONS",
                message="Aucune dimension n'a été détectée ou sélectionnée.",
            )
        )

    for fact in cube.facts:
        if not fact.measures:
            issues.append(
                ValidationIssue(
                    level="warning",
                    code="FACT_WITHOUT_MEASURE",
                    message=f"La table de faits {fact.name} ne contient aucune mesure.",
                )
            )

    for dim in cube.dimensions:
        if not dim.key_attribute:
            issues.append(
                ValidationIssue(
                    level="error",
                    code="DIMENSION_WITHOUT_KEY",
                    message=f"La dimension {dim.name} ne possède pas d'attribut clé.",
                )
            )

    has_error = any(issue.level == "error" for issue in issues)

    return ValidationResult(
        is_valid=not has_error,
        issues=issues,
    )
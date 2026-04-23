from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Optional


# ============================================================
# Helpers
# ============================================================

def _normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.strip().lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"\s+", " ", text)
    return text


def _safe_get(obj, key, default=None):
    if obj is None:
        return default

    if isinstance(obj, dict):
        return obj.get(key, default)

    if hasattr(obj, "model_dump"):
        try:
            data = obj.model_dump()
            if isinstance(data, dict):
                return data.get(key, default)
        except Exception:
            pass

    if hasattr(obj, "dict"):
        try:
            data = obj.dict()
            if isinstance(data, dict):
                return data.get(key, default)
        except Exception:
            pass

    return getattr(obj, key, default)


def _extract_name(item) -> Optional[str]:
    if item is None:
        return None
    if isinstance(item, dict):
        return item.get("name")
    return getattr(item, "name", None)


def _contains_any(text: str, keywords: List[str]) -> bool:
    return any(k in text for k in keywords)


# ============================================================
# Schema extraction
# ============================================================

def _extract_schema_summary(schema) -> Dict[str, Any]:
    summary = {
        "cube_name": None,
        "measures": [],
        "dimensions": [],
        "measure_groups": [],
    }

    if schema is None:
        return summary

    summary["cube_name"] = _safe_get(schema, "cube_name", None)

    measures = _safe_get(schema, "measures", []) or []
    dimensions = _safe_get(schema, "dimensions", []) or []
    measure_groups = _safe_get(schema, "measure_groups", []) or []
    facts = _safe_get(schema, "facts", []) or []

    # cas snapshot sémantique
    for d in dimensions:
        name = _extract_name(d)
        if name:
            summary["dimensions"].append(name)

    for m in measures:
        name = _extract_name(m)
        if name:
            summary["measures"].append(name)

    for mg in measure_groups:
        name = _extract_name(mg)
        if name:
            summary["measure_groups"].append(name)

    # si le schéma utilise "facts" au lieu de measure_groups/measures
    for f in facts:
        fact_name = _extract_name(f)
        if fact_name and fact_name not in summary["measure_groups"]:
            summary["measure_groups"].append(fact_name)

        fact_measures = f.get("measures", []) if isinstance(f, dict) else getattr(f, "measures", [])
        for m in fact_measures or []:
            name = _extract_name(m)
            if name and name not in summary["measures"]:
                summary["measures"].append(name)

    return summary


# ============================================================
# Sujet 2 business groups
# ============================================================

DOMAIN_GROUP_KEYWORDS = {
    "FactInventoryMovement": [
        "inventory",
        "inventaire",
        "stock",
        "movement",
        "movements",
        "mouvement",
        "mouvements",
        "warehouse",
        "entree",
        "sortie",
        "transfer",
        "transfert",
        "reception",
        "received",
        "rejected",
        "quantity received",
        "quantity rejected",
        "mouvements d inventaire",
    ],
    "FactProductionImpact": [
        "production",
        "impact",
        "impacts",
        "production impact",
        "perturbation",
        "perturbations",
        "work order",
        "atelier",
        "retard production",
        "arret",
        "interruption",
        "impacts de production",
    ],
    "FactSupplyRisk": [
        "risk",
        "risque",
        "supply chain",
        "fournisseur",
        "vendor",
        "supplier",
        "critical supplier",
        "resilience",
        "perturbation supply",
        "operational risk",
        "risque operationnel",
        "chaine logistique",
    ],
}


def _detect_business_group(prompt: str) -> Optional[str]:
    normalized = _normalize_text(prompt)

    scores = {}
    for group_name, keywords in DOMAIN_GROUP_KEYWORDS.items():
        score = 0
        for kw in keywords:
            if kw in normalized:
                score += 1
        if score > 0:
            scores[group_name] = score

    if not scores:
        return None

    return max(scores, key=scores.get)


# ============================================================
# Generic business signal (Sujet 1 + Sujet 2)
# ============================================================

GENERAL_MEASURE_KEYWORDS = {
    "Line Total": [
        "chiffre d affaires",
        "chiffre d’affaire",
        "chiffre d affaires",
        "ca",
        "sales",
        "sale",
        "revenue",
        "line total",
        "ventes",
        "vente",
        "revenu",
    ],
    "Margin": [
        "marge",
        "margin",
        "profit",
        "rentable",
        "profitabilite",
        "profitabilité",
    ],
    "Order Qty": [
        "quantite",
        "quantité",
        "qty",
        "volume",
        "nombre vendu",
        "vendu",
        "vendus",
    ],
    "Unit Price": [
        "prix unitaire",
        "unit price",
        "price",
        "prix",
    ],
    "Standard Cost": [
        "cout standard",
        "coût standard",
        "standard cost",
        "cost",
        "cout",
        "coût",
    ],
    "Rating": [
        "rating",
        "note",
        "avis",
        "satisfaction",
    ],
    "Review Count": [
        "review count",
        "nombre d avis",
        "nombre d'avis",
        "count reviews",
    ],
    "Quantity": [
        "quantity",
        "quantite",
        "quantité",
    ],
    "Actual Cost": [
        "actual cost",
        "cout reel",
        "coût réel",
    ],
    "Total Movement Cost": [
        "total movement cost",
        "cout total du mouvement",
        "coût total du mouvement",
    ],
    "Received Qty": [
        "received qty",
        "quantite recue",
        "quantité reçue",
        "recu",
        "reçue",
    ],
    "Rejected Qty": [
        "rejected qty",
        "quantite rejetee",
        "quantité rejetée",
        "rejete",
        "rejetee",
        "rejetée",
    ],
    "Delay Days": [
        "delay days",
        "jours de retard",
        "retard",
    ],
}

GENERAL_DIMENSION_KEYWORDS = {
    "DimProduct": [
        "produit",
        "produits",
        "product",
        "products",
        "categorie",
        "catégorie",
        "category",
        "subcategory",
        "sub category",
        "sous categorie",
        "sous-categorie",
        "sous catégorie",
    ],
    "DimDate": [
        "date",
        "annee",
        "année",
        "year",
        "mois",
        "month",
        "months",
        "trimestre",
        "quarter",
        "jour",
        "day",
        "2011",
        "2012",
        "2013",
        "2014",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
        "2025",
        "2026",
    ],
    "DimVendor": [
        "vendor",
        "vendors",
        "supplier",
        "suppliers",
        "fournisseur",
        "fournisseurs",
    ],
    "DimPurchaseOrder": [
        "purchase order",
        "purchase orders",
        "commande achat",
        "commandes d'achat",
        "commandes d achat",
        "bon de commande",
        "bons de commande",
    ],
    "DimWorkOrder": [
        "work order",
        "ordre de travail",
    ],
}


def _detect_general_business_signal(prompt: str) -> Dict[str, Any]:
    p = _normalize_text(prompt)

    found_measures = []
    found_dimensions = []

    for measure_name, keywords in GENERAL_MEASURE_KEYWORDS.items():
        if any(k in p for k in keywords):
            found_measures.append(measure_name)

    for dim_name, keywords in GENERAL_DIMENSION_KEYWORDS.items():
        if any(k in p for k in keywords):
            found_dimensions.append(dim_name)

    year_found = bool(re.search(r"\b20\d{2}\b", p))

    return {
        "measures": found_measures,
        "dimensions": found_dimensions,
        "has_year": year_found,
    }


# ============================================================
# Suggestions
# ============================================================

def _score_names_against_prompt(prompt: str, names: List[str]) -> List[str]:
    normalized_prompt = _normalize_text(prompt)
    scored = []

    for name in names:
        norm_name = _normalize_text(name)
        score = 0

        if norm_name in normalized_prompt or normalized_prompt in norm_name:
            score += 3

        for token in norm_name.split():
            if token and token in normalized_prompt:
                score += 1

        if score > 0:
            scored.append((name, score))

    scored.sort(key=lambda x: (-x[1], x[0]))
    return [name for name, _ in scored]


def _suggest_default_dimensions(dimensions: List[str]) -> List[str]:
    priority = []
    dim_norm_map = {d: _normalize_text(d) for d in dimensions}

    preferred_tokens = [
        "date", "time", "product", "category", "subcategory",
        "vendor", "supplier", "warehouse", "location"
    ]

    for token in preferred_tokens:
        for d, dn in dim_norm_map.items():
            if token in dn and d not in priority:
                priority.append(d)

    for d in dimensions:
        if d not in priority:
            priority.append(d)

    return priority[:5]


def _suggest_default_measures(measures: List[str], group_name: Optional[str], prompt: str) -> List[str]:
    if not measures:
        return []

    general_signal = _detect_general_business_signal(prompt)
    if general_signal["measures"]:
        preferred = []
        norm_measures = {m: _normalize_text(m) for m in measures}
        for gm in general_signal["measures"]:
            gm_norm = _normalize_text(gm)
            for m, mn in norm_measures.items():
                if gm_norm in mn or mn in gm_norm:
                    preferred.append(m)
        if preferred:
            out = []
            for m in preferred + measures:
                if m not in out:
                    out.append(m)
            return out[:5]

    group_priorities = {
        "FactInventoryMovement": ["quantity", "received", "rejected", "movement", "stock", "cost"],
        "FactProductionImpact": ["impact", "cost", "delay", "production", "loss", "scrap", "order qty"],
        "FactSupplyRisk": ["risk", "score", "probability", "severity", "critical", "delay", "received", "rejected"],
    }

    tokens = group_priorities.get(group_name, [])
    ranked = []

    for m in measures:
        norm = _normalize_text(m)
        score = 0
        for t in tokens:
            if t in norm:
                score += 2
        ranked.append((m, score))

    ranked.sort(key=lambda x: (-x[1], x[0]))
    return [m for m, _ in ranked[:5]]


# ============================================================
# Vague detection
# ============================================================

GENERIC_VAGUE_PATTERNS = [
    "affiche les plus importants",
    "montre les plus importants",
    "affiche les donnees",
    "montre les donnees",
    "compare les resultats",
    "compare",
    "affiche la tendance",
    "montre la tendance",
    "analyse la performance",
    "donne moi les details",
    "affiche les details",
    "montre les details",
]


def _is_strongly_vague(prompt: str, detected_group: Optional[str], general_signal: Dict[str, Any]) -> bool:
    normalized = _normalize_text(prompt)

    if detected_group:
        return False

    if general_signal["measures"] or general_signal["dimensions"] or general_signal["has_year"]:
        return False

    if normalized in GENERIC_VAGUE_PATTERNS:
        return True

    short_vague_terms = [
        "compare",
        "analyse",
        "affiche",
        "montre",
        "donne",
        "resultats",
        "donnees",
        "details",
        "tendance",
        "performance",
        "indicateurs",
    ]

    words = normalized.split()

    if len(words) <= 2 and any(w in short_vague_terms for w in words):
        return True

    for pattern in GENERIC_VAGUE_PATTERNS:
        if pattern in normalized:
            return True

    return False


# ============================================================
# Main guidance
# ============================================================

def generate_dynamic_guidance(
    prompt: str,
    schema: Any = None,
    cube_name: Optional[str] = None,
    dw_id: Optional[str] = None,
) -> Dict[str, Any]:
    schema_summary = _extract_schema_summary(schema)

    measures = schema_summary.get("measures", [])
    dimensions = schema_summary.get("dimensions", [])
    measure_groups = schema_summary.get("measure_groups", [])
    final_cube_name = cube_name or schema_summary.get("cube_name")

    detected_group = _detect_business_group(prompt)
    general_signal = _detect_general_business_signal(prompt)

    matched_measures = _score_names_against_prompt(prompt, measures)
    matched_dimensions = _score_names_against_prompt(prompt, dimensions)

    suggested_measures = matched_measures[:5] if matched_measures else _suggest_default_measures(measures, detected_group, prompt)
    suggested_dimensions = matched_dimensions[:5] if matched_dimensions else _suggest_default_dimensions(dimensions)

    # enrichir avec signaux métier généraux
    for gm in general_signal["measures"]:
        if gm not in suggested_measures:
            suggested_measures.insert(0, gm)

    for gd in general_signal["dimensions"]:
        if gd not in suggested_dimensions:
            suggested_dimensions.insert(0, gd)

    suggested_measures = suggested_measures[:5]
    suggested_dimensions = suggested_dimensions[:5]

    if _is_strongly_vague(prompt, detected_group, general_signal):
        return {
            "is_vague": True,
            "help_message": "Le prompt semble vague. Peux-tu préciser la mesure, la dimension ou la période souhaitée ?",
            "guided_questions": [
                "Quelle mesure veux-tu analyser exactement ?",
                "Selon quelle dimension veux-tu afficher le résultat ?",
                "Veux-tu ajouter une période ?",
            ],
            "suggested_measures": suggested_measures,
            "suggested_dimensions": suggested_dimensions,
            "assistant_stage": "pre_generation_guidance",
            "reasoning_label": "vague_prompt_detected",
            "detected_group": None,
            "cube_name": final_cube_name,
            "dw_id": dw_id,
            "available_measure_groups": measure_groups,
        }

    # détection métier générale sujet1/sujet2
    if general_signal["measures"] or general_signal["dimensions"] or general_signal["has_year"]:
        return {
            "is_vague": False,
            "help_message": "Le prompt contient assez d’indices pour générer une analyse.",
            "guided_questions": [
                "Souhaites-tu un affichage plus détaillé ?",
                "Veux-tu ajouter ou modifier un filtre ?",
            ],
            "suggested_measures": suggested_measures,
            "suggested_dimensions": suggested_dimensions,
            "assistant_stage": "ready_for_generation",
            "reasoning_label": "general_business_signal_detected",
            "detected_group": detected_group,
            "cube_name": final_cube_name,
            "dw_id": dw_id,
            "available_measure_groups": measure_groups,
        }

    if detected_group == "FactInventoryMovement":
        return {
            "is_vague": False,
            "help_message": "Le prompt est compréhensible. Le domaine détecté est les mouvements d’inventaire.",
            "guided_questions": [
                "Veux-tu un total global ou une analyse par produit, entrepôt ou date ?",
                "Souhaites-tu filtrer par période ?",
            ],
            "suggested_measures": suggested_measures,
            "suggested_dimensions": suggested_dimensions,
            "assistant_stage": "ready_for_generation",
            "reasoning_label": "business_group_detected_inventory",
            "detected_group": detected_group,
            "cube_name": final_cube_name,
            "dw_id": dw_id,
            "available_measure_groups": measure_groups,
        }

    if detected_group == "FactProductionImpact":
        return {
            "is_vague": False,
            "help_message": "Le prompt est compréhensible. Le domaine détecté est l’impact de production.",
            "guided_questions": [
                "Veux-tu un total global ou une analyse par produit, atelier ou date ?",
                "Souhaites-tu ajouter une période ou un filtre ?",
            ],
            "suggested_measures": suggested_measures,
            "suggested_dimensions": suggested_dimensions,
            "assistant_stage": "ready_for_generation",
            "reasoning_label": "business_group_detected_production",
            "detected_group": detected_group,
            "cube_name": final_cube_name,
            "dw_id": dw_id,
            "available_measure_groups": measure_groups,
        }

    if detected_group == "FactSupplyRisk":
        return {
            "is_vague": False,
            "help_message": "Le prompt est compréhensible. Le domaine détecté est le risque supply chain.",
            "guided_questions": [
                "Souhaites-tu analyser le risque par fournisseur, produit ou période ?",
                "Veux-tu un classement Top N ?",
            ],
            "suggested_measures": suggested_measures,
            "suggested_dimensions": suggested_dimensions,
            "assistant_stage": "ready_for_generation",
            "reasoning_label": "business_group_detected_risk",
            "detected_group": detected_group,
            "cube_name": final_cube_name,
            "dw_id": dw_id,
            "available_measure_groups": measure_groups,
        }

    if matched_measures or matched_dimensions:
        return {
            "is_vague": False,
            "help_message": "Le prompt contient assez d’indices pour générer une analyse.",
            "guided_questions": [
                "Souhaites-tu ajouter un filtre temporel ?",
                "Veux-tu un affichage global ou par dimension ?",
            ],
            "suggested_measures": suggested_measures,
            "suggested_dimensions": suggested_dimensions,
            "assistant_stage": "ready_for_generation",
            "reasoning_label": "schema_terms_detected",
            "detected_group": detected_group,
            "cube_name": final_cube_name,
            "dw_id": dw_id,
            "available_measure_groups": measure_groups,
        }

    return {
        "is_vague": True,
        "help_message": "Le prompt semble encore vague ou incomplet.",
        "guided_questions": [
            "Quelle mesure veux-tu analyser exactement ?",
            "Selon quelle dimension veux-tu afficher le résultat ?",
            "Veux-tu ajouter une période ?",
        ],
        "suggested_measures": suggested_measures,
        "suggested_dimensions": suggested_dimensions,
        "assistant_stage": "pre_generation_guidance",
        "reasoning_label": "fallback_no_clear_signal",
        "detected_group": detected_group,
        "cube_name": final_cube_name,
        "dw_id": dw_id,
        "available_measure_groups": measure_groups,
    }


# ============================================================
# Public API
# ============================================================

def analyze_prompt_guidance(
    prompt: str,
    schema: Any = None,
    cube_name: Optional[str] = None,
    dw_id: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    return generate_dynamic_guidance(
        prompt=prompt,
        schema=schema,
        cube_name=cube_name,
        dw_id=dw_id,
    )
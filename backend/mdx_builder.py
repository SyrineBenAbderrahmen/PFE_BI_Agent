from __future__ import annotations

import re
from typing import Dict, Any, List, Optional, Tuple

from validator import (
    _find_fact_for_measure,
    _is_valid_attr_for_dim,
    _looks_like_measure_name,
    normalize_plan_measure_facts,
    normalize_plan_dimensions,
)
# =========================================================
# BASIC HELPERS
# =========================================================

def _br(x: str) -> str:
    x = (x or "").strip()
    if x.startswith("[") and x.endswith("]"):
        return x
    return f"[{x}]"


def _norm(x: str) -> str:
    return re.sub(r"[\s_\-]+", "", (x or "").strip().lower())


def _norm_text(text: str) -> str:
    text = (text or "").lower().strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _contains_any(text: str, variants: List[str]) -> bool:
    t = (text or "").lower()
    return any(v in t for v in variants)


def _has_any(text: str, patterns: List[str]) -> bool:
    t = _norm_text(text)
    return any(p in t for p in patterns)


def _hier(dim: str, attr: str) -> str:
    return f"{_br(dim)}.{_br(attr)}"


# =========================================================
# SCHEMA EXTRACTION HELPERS FOR GUIDANCE
# =========================================================

def _extract_all_measure_names(schema: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    for fact in schema.get("facts", []) or []:
        for m in fact.get("measures", []) or []:
            name = (m.get("name") or m.get("column") or "").strip()
            if name and name not in names:
                names.append(name)
    return names


def _extract_all_dimension_names(schema: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    for d in schema.get("dimensions", []) or []:
        name = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        if name and name not in names:
            names.append(name)
    return names


def _extract_dimension_attributes(schema: Dict[str, Any]) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}

    for d in schema.get("dimensions", []) or []:
        dim_name = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        attrs: List[str] = []

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

            if attr_name and attr_name not in attrs:
                attrs.append(attr_name)

        if dim_name:
            result[dim_name] = attrs

    return result


# =========================================================
# GUIDANCE / SMART SUGGESTIONS
# =========================================================

def _is_vague_prompt(text: str) -> bool:
    t = _norm_text(text)

    vague_patterns = [
        "je veux voir",
        "affiche les données",
        "donne moi un rapport",
        "montre moi les statistiques",
        "je veux une analyse",
        "afficher les résultats",
        "donne moi les informations importantes",
        "je veux un tableau",
        "montre moi la performance",
        "afficher les indicateurs",
        "je veux comprendre les ventes",
        "donne moi un résumé",
        "afficher les mesures",
        "quelles sont les données disponibles",
        "montre moi quelque chose",
        "je veux les données",
        "je veux un dashboard",
        "donne moi les stats",
        "montre moi les résultats",
        "je veux un aperçu",
    ]

    return any(p in t for p in vague_patterns)


def _is_metric_oriented_but_incomplete(text: str, schema: Dict[str, Any]) -> bool:
    t = _norm_text(text)

    generic_metric_patterns = [
        "total des ventes",
        "somme des ventes",
        "profit",
        "quantité vendue",
        "quantite vendue",
        "chiffre d’affaires",
        "chiffre d'affaires",
        "chiffre d affaires",
        "nombre de ventes",
        "moyenne des ventes",
        "montant total",
        "total global",
        "ventes globales",
        "marge",
        "coût",
        "cout",
        "prix",
        "note",
        "rating",
        "review count",
        "line total",
        "order qty",
        "unit price",
        "standard cost",
    ]

    has_metric_signal = any(p in t for p in generic_metric_patterns)

    for measure_name in _extract_all_measure_names(schema):
        if _norm_text(measure_name) in t:
            has_metric_signal = True
            break

    if not has_metric_signal:
        return False

    dimension_signals = [
        "par produit", "par catégorie", "par categorie", "par sous catégorie", "par sous categorie",
        "par mois", "par année", "par annee", "par date", "par fournisseur",
        "par vendor", "par work order", "par purchase order",
        "en 20", "sur 20", "entre 20", "dans 20",
        "top ", "entre ", "pour ", "selon "
    ]

    has_axis_or_filter = any(s in t for s in dimension_signals)

    dim_attrs = _extract_dimension_attributes(schema)
    for _dim, attrs in dim_attrs.items():
        for attr in attrs:
            attr_norm = _norm_text(attr)
            if attr_norm and attr_norm in t:
                has_axis_or_filter = True
                break
        if has_axis_or_filter:
            break

    return has_metric_signal and not has_axis_or_filter


def _guess_relevant_measures(text: str, schema: Dict[str, Any], max_items: int = 5) -> List[str]:
    t = _norm_text(text)
    all_measures = _extract_all_measure_names(schema)
    suggestions: List[str] = []

    semantic_map = {
        "ventes": ["Line Total", "Order Qty"],
        "vente": ["Line Total", "Order Qty"],
        "chiffre d’affaires": ["Line Total"],
        "chiffre d'affaires": ["Line Total"],
        "chiffre d affaires": ["Line Total"],
        "revenu": ["Line Total"],
        "revenue": ["Line Total"],
        "profit": ["Margin"],
        "marge": ["Margin"],
        "rentabilité": ["Margin"],
        "rentabilite": ["Margin"],
        "quantité vendue": ["Order Qty"],
        "quantite vendue": ["Order Qty"],
        "volume de vente": ["Order Qty"],
        "volume des ventes": ["Order Qty"],
        "prix": ["Unit Price"],
        "prix unitaire": ["Unit Price"],
        "prix de vente": ["Unit Price"],
        "coût": ["Standard Cost"],
        "cout": ["Standard Cost"],
        "coût standard": ["Standard Cost"],
        "cout standard": ["Standard Cost"],
        "note": ["Rating"],
        "satisfaction": ["Rating"],
        "rating": ["Rating"],
        "avis": ["Review Count"],
        "review": ["Review Count"],
        "reviews": ["Review Count"],
        "retard": ["Late Flag", "Delay Days"],
        "retards": ["Late Flag", "Delay Days"],
        "late": ["Late Flag", "Delay Days"],
        "delay": ["Delay Days", "Late Flag"],
        "achat": ["Line Amount"],
        "achats": ["Line Amount"],
        "line amount": ["Line Amount"],
    }

    def add_if_exists(name: str):
        for m in all_measures:
            if _norm_text(m) == _norm_text(name) and m not in suggestions:
                suggestions.append(m)
                return

    for key, mapped in semantic_map.items():
        if key in t:
            for measure_name in mapped:
                add_if_exists(measure_name)

    for m in all_measures:
        if _norm_text(m) in t and m not in suggestions:
            suggestions.append(m)

    if not suggestions:
        fallback_priority = [
            "Line Total",
            "Margin",
            "Order Qty",
            "Unit Price",
            "Standard Cost",
            "Rating",
            "Review Count",
            "Late Flag",
            "Delay Days",
            "Line Amount",
        ]
        for fallback in fallback_priority:
            add_if_exists(fallback)

    return suggestions[:max_items]


def _guess_relevant_dimensions(text: str, schema: Dict[str, Any], max_items: int = 5) -> List[str]:
    t = _norm_text(text)
    all_dims = _extract_all_dimension_names(schema)
    dim_attrs = _extract_dimension_attributes(schema)

    suggestions: List[str] = []

    semantic_map = {
        "produit": ["DimProduct"],
        "produits": ["DimProduct"],
        "product": ["DimProduct"],
        "products": ["DimProduct"],
        "catégorie": ["DimProduct"],
        "categorie": ["DimProduct"],
        "category": ["DimProduct"],
        "sous catégorie": ["DimProduct"],
        "sous categorie": ["DimProduct"],
        "subcategory": ["DimProduct"],
        "sub category": ["DimProduct"],
        "mois": ["DimDate"],
        "month": ["DimDate"],
        "année": ["DimDate"],
        "annee": ["DimDate"],
        "year": ["DimDate"],
        "date": ["DimDate"],
        "dates": ["DimDate"],
        "jour": ["DimDate"],
        "fournisseur": ["DimVendor"],
        "fournisseurs": ["DimVendor"],
        "vendor": ["DimVendor"],
        "vendors": ["DimVendor"],
        "commande": ["DimPurchaseOrder"],
        "commandes": ["DimPurchaseOrder"],
        "purchase order": ["DimPurchaseOrder"],
        "work order": ["DimWorkOrder"],
    }

    def add_dim_if_exists(name: str):
        for d in all_dims:
            if _norm_text(d) == _norm_text(name) and d not in suggestions:
                suggestions.append(d)
                return

    for key, mapped_dims in semantic_map.items():
        if key in t:
            for dim_name in mapped_dims:
                add_dim_if_exists(dim_name)

    for dim_name, attrs in dim_attrs.items():
        for attr in attrs:
            attr_norm = _norm_text(attr)
            if attr_norm and attr_norm in t and dim_name not in suggestions:
                suggestions.append(dim_name)
                break

    if not suggestions:
        fallback_priority = [
            "DimDate",
            "DimProduct",
            "DimVendor",
            "DimPurchaseOrder",
            "DimWorkOrder",
        ]
        for fallback in fallback_priority:
            add_dim_if_exists(fallback)

    return suggestions[:max_items]


def _build_guided_questions(text: str, schema: Dict[str, Any]) -> List[str]:
    t = _norm_text(text)

    measures = _guess_relevant_measures(text, schema, max_items=3)
    dimensions = _guess_relevant_dimensions(text, schema, max_items=3)

    questions: List[str] = []

    if _is_vague_prompt(t):
        questions.append("Quelle mesure veux-tu analyser exactement ?")
        questions.append("Selon quelle dimension veux-tu afficher le résultat ?")
        questions.append("Veux-tu ajouter une période ou un filtre ?")
        return questions

    if _is_metric_oriented_but_incomplete(t, schema):
        if measures:
            questions.append(f"Veux-tu analyser {', '.join(measures[:2])} ?")
        if dimensions:
            questions.append(
                f"Souhaites-tu afficher le résultat par {', '.join(dimensions[:3])} ?"
            )
        else:
            questions.append("Souhaites-tu afficher le résultat par produit, catégorie, mois ou année ?")
        questions.append("Veux-tu limiter l’analyse à une période précise ?")
        return questions

    return questions


def analyze_prompt_guidance(dw_id: str, prompt: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    t = _norm_text(prompt)

    all_dims = _extract_all_dimension_names(schema)
    all_measures = _extract_all_measure_names(schema)

    is_vague = _is_vague_prompt(t)
    is_metric_incomplete = _is_metric_oriented_but_incomplete(t, schema)

    suggested_measures = _guess_relevant_measures(t, schema)
    suggested_dimensions = _guess_relevant_dimensions(t, schema)
    guided_questions = _build_guided_questions(t, schema)

    available_measure_groups = []
    for fact in schema.get("facts", []) or []:
        fact_name = (fact.get("name") or "").strip()
        if fact_name:
            available_measure_groups.append(fact_name)

    if is_vague:
        help_message = (
            "Le prompt est trop général. "
            "Précise la mesure à analyser, la dimension d’affichage et éventuellement une période."
        )
        assistant_stage = "pre_generation_guidance"
        reasoning_label = "vague_prompt"
    elif is_metric_incomplete:
        help_message = (
            "J’ai détecté une mesure, mais il manque encore un axe d’analyse. "
            "Choisis une dimension d’affichage et éventuellement un filtre temporel."
        )
        assistant_stage = "pre_generation_guidance"
        reasoning_label = "metric_but_incomplete"
    else:
        help_message = "Prompt suffisamment précis pour générer une requête."
        assistant_stage = "ready_for_generation"
        reasoning_label = "ready"

    return {
        "is_vague": is_vague or is_metric_incomplete,
        "help_message": help_message,
        "guided_questions": guided_questions,
        "suggested_measures": suggested_measures,
        "suggested_dimensions": suggested_dimensions,
        "assistant_stage": assistant_stage,
        "reasoning_label": reasoning_label,
        "detected_group": None,
        "cube_name": None,
        "dw_id": dw_id,
        "available_measure_groups": available_measure_groups,
        "all_dimensions": all_dims,
        "all_measures": all_measures,
    }


# =========================================================
# MEASURE / FACT RESOLUTION
# =========================================================

def _friendly_fact_prefix_for_mdx(fact_name: str) -> str:
    mapping = {
        "FactCostHistory": "CostHistory",
        "FactListPriceHistory": "ListPriceHistory",
        "FactProductSales": "ProductSales",
        "FactReviews": "Reviews",
        "FactInventoryMovement": "Inventory Movement",
        "FactProductionImpact": "Production Impact",
        "FactSupplyRisk": "Supply Risk",
    }
    return mapping.get(fact_name, "")


def _measure_exists_in_snapshot(schema: Optional[Dict[str, Any]], fact_table: str, measure_name: str) -> Optional[str]:
    if not schema:
        return None

    for fact in schema.get("facts", []) or []:
        if fact_table and (fact.get("name") or "").strip() != fact_table.strip():
            continue

        for m in fact.get("measures", []) or []:
            m_name = (m.get("name") or "").strip()
            m_col = (m.get("column") or "").strip()
            if _norm(m_name) == _norm(measure_name) or _norm(m_col) == _norm(measure_name):
                return m_name or m_col

    return None


def _infer_fact_table_from_measure(schema: Optional[Dict[str, Any]], measure_name: str) -> str:
    return _find_fact_for_measure(schema, measure_name)


def _measure_unique_name(measure_name: str, fact_table: str = "", schema: Optional[Dict[str, Any]] = None) -> str:
    effective_fact_table = fact_table or _infer_fact_table_from_measure(schema, measure_name)
    exact_name = _measure_exists_in_snapshot(schema, effective_fact_table, measure_name)
    if exact_name:
        measure_name = exact_name

    prefix = _friendly_fact_prefix_for_mdx(effective_fact_table)
    if prefix:
        return f"[Measures].{_br(prefix + ' - ' + measure_name)}"
    return f"[Measures].{_br(measure_name)}"


# =========================================================
# PROMPT PARSING HELPERS
# =========================================================

def _find_year_in_text(text: str) -> Optional[int]:
    m = re.search(r"\b(20\d{2})\b", text or "")
    return int(m.group(1)) if m else None


def _find_all_years_in_text(text: str) -> List[int]:
    return [int(y) for y in re.findall(r"\b(20\d{2})\b", text or "")]


def _find_top_n(text: str) -> Optional[int]:
    m = re.search(r"\btop\s*(\d+)\b", (text or "").lower())
    return int(m.group(1)) if m else None


def _detect_agg_intent(text: str) -> Optional[str]:
    t = (text or "").lower()
    if any(k in t for k in ["moyenne", "average", "avg", "moyen"]):
        return "Avg"
    if any(k in t for k in ["nombre", "count", "combien", "nb", "nbr"]):
        return "Count"
    if any(k in t for k in ["somme", "sum", "total de", "somme du", "somme de"]):
        return "Sum"
    return None


def _has_metric_intent(text: str) -> bool:
    t = (text or "").lower().strip()
    metric_keywords = [
        "line total", "sales", "sale", "ventes", "vente",
        "chiffre d'affaires", "chiffre d affaires", "revenu", "revenue",
        "profit", "profitables", "profitable", "margin", "marge",
        "rentabilité", "rentabilite", "coût", "cout", "cost",
        "prix", "price", "total", "moyenne", "average", "avg",
        "count", "nombre", "sum", "somme", "measure", "mesure",
        "order qty", "unit price", "standard cost", "rating", "note",
        "review count", "quantity", "delay", "risk", "avis",
        "volume de vente", "volume des ventes", "late flag", "delay days", "line amount",
    ]
    return any(k in t for k in metric_keywords)


def _is_listing_prompt(text: str) -> bool:
    t = (text or "").lower().strip()
    listing_keywords = [
        "donne moi", "lister", "liste", "nom", "noms",
        "name", "names", "toutes les", "tous les", "all",
    ]
    return any(k in t for k in listing_keywords) and not _has_metric_intent(t)


def _is_pure_dimension_listing_prompt(text: str) -> bool:
    t = (text or "").lower().strip()
    listing_keywords = ["liste", "lister", "affiche", "donne", "montre", "show", "list", "display"]
    dimension_words = [
        "annee", "année", "annees", "années", "year", "years",
        "mois", "month", "months",
        "categorie", "catégorie", "categories", "catégories", "category",
        "sous categorie", "sous-catégorie", "subcategory", "sub category",
        "produit", "produits", "product", "products",
        "date", "dates",
        "fournisseur", "fournisseurs", "vendor", "vendors",
        "work order", "work orders",
        "purchase order", "purchase orders", "commandes d'achat", "commandes d achat",
    ]
    return any(k in t for k in listing_keywords) and any(k in t for k in dimension_words) and not _has_metric_intent(t)


def _detect_requested_listing_target(user_prompt: str) -> Optional[str]:
    t = (user_prompt or "").lower()

    if any(x in t for x in ["dates disponibles", "date disponible", "liste des dates", "affiche les dates", "donne les dates"]):
        return "DimDate:FullDate"
    if any(x in t for x in ["annees disponibles", "années disponibles", "liste les annees", "liste les années", "affiche les annees", "affiche les années"]):
        return "DimDate:Year"
    if any(x in t for x in ["mois disponibles", "liste les mois", "affiche les mois"]):
        return "DimDate:MonthName"
    if any(x in t for x in ["fournisseurs disponibles", "liste les fournisseurs", "affiche les fournisseurs", "donne les fournisseurs", "vendors", "suppliers"]):
        return "DimVendor:VendorName"
    if any(x in t for x in ["commandes d'achat", "commandes d achat", "liste les commandes d'achat", "affiche les commandes d'achat", "purchase orders", "purchase order"]):
        return "DimPurchaseOrder:PurchaseOrderID"
    if any(x in t for x in ["work orders", "ordres de travail", "liste les work orders", "affiche les work orders"]):
        return "DimWorkOrder:WorkOrderID"
    if any(x in t for x in ["categories", "catégories", "liste les categories", "liste les catégories"]):
        return "DimProduct:Category"
    if any(x in t for x in ["sous categories", "sous-categories", "sous catégories", "subcategories", "sub category", "sub categories"]):
        return "DimProduct:SubCategory"
    if any(x in t for x in ["produits", "products", "liste les produits", "affiche les produits"]):
        return "DimProduct:ProductName"

    return None


def _wants_product_listing(text: str) -> bool:
    t = (text or "").lower()
    product_words = ["produit", "produits", "product", "products"]
    analytic_words = [
        "chiffre d'affaires", "chiffre d affaires", "sales", "sale",
        "line total", "margin", "marge", "rentabilité", "rentabilite",
        "profit", "cost", "price", "revenu", "revenue", "order qty",
        "unit price", "standard cost", "rating", "review count", "count",
        "nombre", "sum", "somme", "average", "avg", "moyenne", "moyen",
        "late flag", "delay days", "line amount", "retard", "retards",
    ]
    return any(w in t for w in product_words) and not any(w in t for w in analytic_words)


# =========================================================
# SCHEMA / HIERARCHY HELPERS
# =========================================================

def _pick_date_dim_year(plan_dims: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    for d in plan_dims:
        dim = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        attrs = [a.strip() for a in (d.get("attributes") or d.get("levels") or []) if isinstance(a, str) and a.strip()]
        if ("date" in dim.lower() or "time" in dim.lower()) and attrs:
            for a in attrs:
                if "year" in a.lower():
                    return dim, a

    for d in plan_dims:
        dim = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        attrs = [a.strip() for a in (d.get("attributes") or d.get("levels") or []) if isinstance(a, str) and a.strip()]
        for a in attrs:
            if "year" in a.lower():
                return dim, a

    return None, None


def _pick_ranking_dim_attr(plan_dims: List[Dict[str, Any]], user_prompt: str) -> Tuple[Optional[str], Optional[str]]:
    t = (user_prompt or "").lower()

    if any(x in t for x in ["categorie", "catégorie", "category", "categories", "catégories"]):
        for d in plan_dims:
            dim = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
            attrs = [a.strip() for a in (d.get("attributes") or d.get("levels") or []) if isinstance(a, str) and a.strip()]
            if "product" in dim.lower():
                for a in attrs:
                    if _norm(a) == _norm("Category"):
                        return dim, a

    if any(x in t for x in ["sub category", "subcategory", "sous categorie", "sous-catégorie", "sous catégories"]):
        for d in plan_dims:
            dim = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
            attrs = [a.strip() for a in (d.get("attributes") or d.get("levels") or []) if isinstance(a, str) and a.strip()]
            if "product" in dim.lower():
                for a in attrs:
                    if _norm(a) == _norm("SubCategory"):
                        return dim, a

    for d in plan_dims:
        dim = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        attrs = [a.strip() for a in (d.get("attributes") or d.get("levels") or []) if isinstance(a, str) and a.strip()]
        if "product" in dim.lower():
            for a in attrs:
                if _norm(a) in {_norm("ProductName"), _norm("Product Name")}:
                    return dim, a

    return None, None


def _find_level_hierarchy(schema: Dict[str, Any], dim: str, level: str) -> Optional[Tuple[str, str, str, int]]:
    dim_n = _norm(dim)
    level_n = _norm(level)

    natural = schema.get("natural_hierarchies", {}) or {}
    for dim_name, hierarchies in natural.items():
        if _norm(dim_name) != dim_n:
            continue
        for h in hierarchies or []:
            h_name = (h.get("mdx_name") or h.get("name") or "").strip()
            levels = h.get("levels", []) or []
            for idx, lvl in enumerate(levels):
                if isinstance(lvl, dict):
                    logical_name = (lvl.get("name") or "").strip()
                    mdx_name = (lvl.get("mdx_name") or logical_name).strip()
                    source_column = (lvl.get("source_column") or logical_name).strip()
                else:
                    logical_name = str(lvl).strip()
                    mdx_name = logical_name
                    source_column = logical_name

                if _norm(logical_name) == level_n or _norm(mdx_name) == level_n or _norm(source_column) == level_n:
                    return dim_name, h_name, (source_column or logical_name or mdx_name), idx

    for d in schema.get("dimensions", []) or []:
        dim_name = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        if _norm(dim_name) != dim_n:
            continue
        for h in d.get("hierarchies", []) or []:
            h_name = (h.get("mdx_name") or h.get("name") or "").strip()
            levels = h.get("levels", []) or []
            for idx, lvl in enumerate(levels):
                if isinstance(lvl, dict):
                    logical_name = (lvl.get("name") or lvl.get("level_name") or "").strip()
                    mdx_name = (lvl.get("mdx_name") or logical_name).strip()
                    source_column = (lvl.get("source_column") or logical_name).strip()
                else:
                    logical_name = str(lvl).strip()
                    mdx_name = logical_name
                    source_column = logical_name

                if _norm(logical_name) == level_n or _norm(mdx_name) == level_n or _norm(source_column) == level_n:
                    return dim_name, h_name, (source_column or logical_name or mdx_name), idx

    return None


def _resolve_date_dim_year(plan_dims: List[Dict[str, Any]], schema: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    date_dim, year_attr = _pick_date_dim_year(plan_dims)
    if date_dim and year_attr:
        return date_dim, year_attr

    if schema:
        for d in schema.get("dimensions", []) or []:
            dim_name = (d.get("name") or "").strip()
            if _norm(dim_name) != _norm("DimDate"):
                continue

            attr_names = []
            for a in d.get("attributes", []) or []:
                if isinstance(a, dict):
                    attr_names.append((a.get("name") or "").strip())
                else:
                    attr_names.append(str(a).strip())

            for candidate in ["Year", "YearNumber"]:
                for a in attr_names:
                    if _norm(a) == _norm(candidate):
                        return dim_name, a

    return None, None


def _hierarchy_level_count(schema: Dict[str, Any], dim_name: str, hierarchy_name: str) -> int:
    natural = schema.get("natural_hierarchies", {}) or {}
    for snap_dim_name, hierarchies in natural.items():
        if _norm(snap_dim_name) != _norm(dim_name):
            continue
        for h in hierarchies or []:
            h_name = (h.get("mdx_name") or h.get("name") or "").strip()
            if _norm(h_name) == _norm(hierarchy_name):
                return len(h.get("levels", []) or [])

    for d in schema.get("dimensions", []) or []:
        current_dim = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        if _norm(current_dim) != _norm(dim_name):
            continue
        for h in d.get("hierarchies", []) or []:
            h_name = (h.get("mdx_name") or h.get("name") or "").strip()
            if _norm(h_name) == _norm(hierarchy_name):
                return len(h.get("levels", []) or [])

    return 0


def _members(dim: str, attr: str, schema: Optional[Dict[str, Any]] = None) -> str:
    if schema:
        found = _find_level_hierarchy(schema, dim, attr)
        if found:
            dim_mdx_name, hierarchy_mdx_name, level_mdx_name, _ = found
            levels_count = _hierarchy_level_count(schema, dim_mdx_name, hierarchy_mdx_name)

            if levels_count == 1:
                return f"{_br(dim_mdx_name)}.{_br(hierarchy_mdx_name)}.Members"

            return f"{_br(dim_mdx_name)}.{_br(hierarchy_mdx_name)}.{_br(level_mdx_name)}.Members"

    return f"{_hier(dim, attr)}.Members"


def _member_key(dim: str, attr: str, key_value: Any, schema: Optional[Dict[str, Any]] = None) -> str:
    if schema:
        found = _find_level_hierarchy(schema, dim, attr)
        if found:
            dim_mdx_name, hierarchy_mdx_name, level_mdx_name, _ = found
            return f"{_br(dim_mdx_name)}.{_br(hierarchy_mdx_name)}.{_br(level_mdx_name)}.&[{key_value}]"

    return f"{_hier(dim, attr)}.&[{key_value}]"


def _same_hierarchy_levels(schema: Dict[str, Any], dim: str, attrs: List[str]) -> Optional[Tuple[str, str, List[Tuple[str, int]]]]:
    found_levels = []
    dim_mdx_name = None
    hierarchy_mdx_name = None

    for attr in attrs:
        found = _find_level_hierarchy(schema, dim, attr)
        if not found:
            return None

        current_dim_mdx, current_h_mdx, current_level_mdx, current_idx = found
        if dim_mdx_name is None:
            dim_mdx_name = current_dim_mdx
            hierarchy_mdx_name = current_h_mdx
        else:
            if current_dim_mdx != dim_mdx_name or current_h_mdx != hierarchy_mdx_name:
                return None

        found_levels.append((current_level_mdx, current_idx))

    return dim_mdx_name, hierarchy_mdx_name, found_levels


def _build_same_hierarchy_table_mdx(cube_name: str, dim_mdx_name: str, hierarchy_mdx_name: str, levels: List[Tuple[str, int]], where_clause: str = "") -> str:
    ordered = sorted(levels, key=lambda x: x[1])
    deepest_level_name, deepest_idx = ordered[-1]

    with_members = []
    column_members = []

    for level_name, level_idx in ordered:
        safe_caption = level_name.replace(" ", "_")
        if level_idx == deepest_idx:
            expr = f"[{dim_mdx_name}].[{hierarchy_mdx_name}].CurrentMember.Name"
        else:
            expr = f"Ancestor([{dim_mdx_name}].[{hierarchy_mdx_name}].CurrentMember, [{dim_mdx_name}].[{hierarchy_mdx_name}].[{level_name}]).Name"

        with_members.append(f"MEMBER [Measures].[{safe_caption}] AS {expr}")
        column_members.append(f"[Measures].[{safe_caption}]")

    with_clause = "WITH " + " ".join(with_members)
    columns = "{ " + ", ".join(column_members) + " }"
    rows = f"[{dim_mdx_name}].[{hierarchy_mdx_name}].[{deepest_level_name}].Members"

    mdx = f"""
{with_clause}
SELECT
{columns} ON COLUMNS,
{rows} ON ROWS
FROM [{cube_name}]{where_clause};
"""
    return " ".join(mdx.split())


# =========================================================
# DEFAULT MEASURES / SUPPORT
# =========================================================

def _resolve_default_measure_name(user_prompt: str, schema: Optional[Dict[str, Any]], fact_table: str) -> Tuple[str, str]:
    t = (user_prompt or "").lower()

    # ===== Sujet 2 : Supply Risk =====
    if any(x in t for x in ["retard", "retards", "en retard", "late", "delay", "delai", "délai"]):
        if any(x in t for x in ["montant", "montants", "achat", "achats", "line amount"]):
            return "Line Amount", "FactSupplyRisk"
        if any(x in t for x in ["jours", "day", "days", "delay days", "durée", "duree"]):
            return "Delay Days", "FactSupplyRisk"
        return "Late Flag", "FactSupplyRisk"

    if any(x in t for x in ["fournisseur", "fournisseurs", "vendor", "vendors"]):
        return "Late Flag", "FactSupplyRisk"

    if any(x in t for x in ["commande d'achat", "commandes d'achat", "commandes d achat", "purchase order"]):
        return "Late Flag", "FactSupplyRisk"

    if any(x in t for x in ["montant d'achat", "montants d'achats", "montants achats", "line amount"]):
        return "Line Amount", "FactSupplyRisk"
        # ===== Sujet 2 : Production Impact =====
    if any(x in t for x in ["ordre de fabrication", "ordres de fabrication", "work order", "work orders"]):
        return "Risk Flag", "FactProductionImpact"

    if any(x in t for x in ["fragilisé", "fragilises", "fragilisés", "fragilise", "fragilisee"]):
        return "Risk Flag", "FactProductionImpact"

    if any(x in t for x in ["scrap", "scrapped", "scrapped qty", "scrap rate"]):
        if any(x in t for x in ["rate", "taux"]):
            return "Scrap Rate", "FactProductionImpact"
        return "Scrapped Qty", "FactProductionImpact"

    if any(x in t for x in ["quantités planifiées", "quantite planifiee", "quantites planifiees", "planned quantities", "planned qty"]):
        return "Order Qty", "FactProductionImpact"

    if any(x in t for x in ["risque", "risk flag", "menacées", "menacees", "bloqué", "blocked"]):
        return "Risk Flag", "FactProductionImpact"
    
    if any(x in t for x in ["mouvement", "mouvements", "movement", "movements", "fréquents", "frequents"]):
        return "Movement Count", "FactInventoryMovement"

    if any(x in t for x in ["stock", "inventaire", "inventory", "sortent vite du stock", "rotation rapide"]):
        return "Quantity", "FactInventoryMovement"

    if any(x in t for x in ["tension d’inventaire", "tension d'inventaire", "inventory tension"]):
        return "Movement Count", "FactInventoryMovement"

    if any(x in t for x in ["actual cost", "coût réel", "cout reel"]):
        return "Actual Cost", "FactInventoryMovement"

    if any(x in t for x in ["total movement cost", "coût total de mouvement", "cout total de mouvement"]):
        return "Total Movement Cost", "FactInventoryMovement"
    # ===== Sujet 1 / logique existante =====
    if any(x in t for x in ["avis", "review", "reviews", "nombre d’avis", "nombre d'avis"]):
        return "Review Count", "FactReviews"
    if any(x in t for x in ["rating", "note", "notes", "satisfaction"]):
        return "Rating", "FactReviews"
    if any(x in t for x in ["marge", "margin", "profit", "rentabilité", "rentabilite"]):
        return "Margin", "FactProductSales"
    if any(x in t for x in ["volume de vente", "volume des ventes", "quantité vendue", "quantite vendue", "très vendus", "tres vendus"]):
        return "Order Qty", "FactProductSales"
    if any(x in t for x in ["prix unitaire", "prix de vente", "unit price", "prix élevé", "prix eleve"]):
        return "Unit Price", "FactProductSales"
    if any(x in t for x in ["coût standard", "cout standard", "standard cost"]):
        return "Standard Cost", "FactProductSales"
    if any(x in t for x in ["chiffre d’affaires", "chiffre d'affaires", "chiffre d affaires", "sales", "vente", "ventes", "revenue"]):
        return "Line Total", "FactProductSales"

    if fact_table:
        return "Line Total", fact_table
    return "Line Total", "FactProductSales"


def _listing_needs_supporting_measure(user_prompt: str, dims: List[Dict[str, Any]]) -> bool:
    t = (user_prompt or "").lower()

    business_filter_words = [
        "vendu", "vendus", "vente", "ventes", "sales", "sold",
        "avec des ventes", "avec ventes", "avis", "reviews", "review",
        "nombre d'avis", "nombre d’avis", "marge", "margin", "profit",
        "chiffre d'affaires", "chiffre d affaires", "revenue", "avec", "ayant",
        "retard", "retards", "late", "delay"
    ]

    has_business_filter = any(x in t for x in business_filter_words)
    has_year = _find_year_in_text(user_prompt) is not None

    dimension_like_listing = any(
        any(k in ((d.get("name") or d.get("table") or "").lower()) for k in ["product", "date", "review", "vendor", "purchase", "work"])
        for d in dims
    )

    return dimension_like_listing and (has_business_filter or has_year)


def _resolve_supporting_measure(user_prompt: str, schema: Optional[Dict[str, Any]], fact_table: str) -> str:
    default_measure, default_fact = _resolve_default_measure_name(user_prompt, schema, fact_table)
    return _measure_unique_name(default_measure, default_fact, schema)

# =========================================================
# ROW SELECTION
# =========================================================

def _pick_best_single_row_expr(plan_dims: List[Dict[str, Any]], schema: Optional[Dict[str, Any]], user_prompt: str = "") -> Optional[str]:
    if not plan_dims:
        return None

    t = (user_prompt or "").lower()

    if (
        any(x in t for x in ["produit", "produits", "product", "products"])
        and any(x in t for x in ["retard", "retards", "late", "delay"])
    ):
        return _members("DimProduct", "ProductName", schema)

    if (
        any(x in t for x in ["fournisseur", "fournisseurs", "vendor", "vendors"])
        and any(x in t for x in ["retard", "retards", "late", "delay"])
    ):
        return _members("DimVendor", "VendorName", schema)

    if any(x in t for x in ["commande d'achat", "commandes d'achat", "commandes d achat", "purchase order"]):
        return _members("DimPurchaseOrder", "PurchaseOrderID", schema)

    requested_target = _detect_requested_listing_target(user_prompt)
    if requested_target:
        wanted_dim, wanted_attr = requested_target.split(":", 1)

        for d in plan_dims:
            dim_name = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
            attrs = [a.strip() for a in (d.get("attributes") or d.get("levels") or []) if isinstance(a, str) and a.strip()]

            if _norm(dim_name) == _norm(wanted_dim):
                for a in attrs:
                    if _norm(a) == _norm(wanted_attr) and _is_valid_attr_for_dim(schema, dim_name, a):
                        return _members(dim_name, a, schema)

                if _is_valid_attr_for_dim(schema, dim_name, wanted_attr):
                    return _members(dim_name, wanted_attr, schema)

    for d in plan_dims:
        dim_name = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        attrs = [a.strip() for a in (d.get("attributes") or d.get("levels") or []) if isinstance(a, str) and a.strip()]

        if "product" in dim_name.lower():
            for wanted in ["ProductName", "Product Name", "Category", "SubCategory", "Sub Category"]:
                for a in attrs:
                    if (
                        _norm(a) == _norm(wanted)
                        and _is_valid_attr_for_dim(schema, dim_name, a)
                        and not _looks_like_measure_name(schema, a)
                    ):
                        return _members(dim_name, a, schema)

            for a in attrs:
                if _is_valid_attr_for_dim(schema, dim_name, a) and not _looks_like_measure_name(schema, a):
                    return _members(dim_name, a, schema)

    for d in plan_dims:
        dim_name = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        attrs = [a.strip() for a in (d.get("attributes") or d.get("levels") or []) if isinstance(a, str) and a.strip()]

        for wanted in ["Category", "SubCategory", "Sub Category"]:
            for a in attrs:
                if (
                    _norm(a) == _norm(wanted)
                    and _is_valid_attr_for_dim(schema, dim_name, a)
                    and not _looks_like_measure_name(schema, a)
                ):
                    return _members(dim_name, a, schema)

    for d in plan_dims:
        dim_name = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        attrs = [a.strip() for a in (d.get("attributes") or d.get("levels") or []) if isinstance(a, str) and a.strip()]

        if "date" in dim_name.lower() or "time" in dim_name.lower():
            for wanted in ["Year", "YearNumber", "Month", "MonthName", "FullDate", "Full Date"]:
                for a in attrs:
                    if (
                        _norm(a) == _norm(wanted)
                        and _is_valid_attr_for_dim(schema, dim_name, a)
                        and not _looks_like_measure_name(schema, a)
                    ):
                        return _members(dim_name, a, schema)

            for a in attrs:
                if _is_valid_attr_for_dim(schema, dim_name, a) and not _looks_like_measure_name(schema, a):
                    return _members(dim_name, a, schema)

    for d in plan_dims:
        dim_name = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
        attrs = [a.strip() for a in (d.get("attributes") or d.get("levels") or []) if isinstance(a, str) and a.strip()]
        for a in attrs:
            if _is_valid_attr_for_dim(schema, dim_name, a) and not _looks_like_measure_name(schema, a):
                return _members(dim_name, a, schema)

    return None


def _pick_primary_rows_expr_for_filter(
    dims: List[Dict[str, Any]],
    schema: Optional[Dict[str, Any]],
    user_prompt: str
) -> str:
    rows_expr = _pick_best_single_row_expr(dims, schema, user_prompt)
    if rows_expr:
        return rows_expr
    return _members("DimProduct", "ProductName", schema)


# =========================================================
# FILTER / RELATIVE HELPERS
# =========================================================

def _build_relative_filter_mdx(
    cube_name: str,
    rows_expr: str,
    conditions: List[Tuple[str, str]],
    selected_measures: List[str]
) -> str:
    with_members = []
    filter_parts = []

    for i, (measure_unique_name, direction) in enumerate(conditions, start=1):
        avg_name = f"[Measures].[Avg Metric {i}]"
        with_members.append(f"MEMBER {avg_name} AS AVG({rows_expr}, {measure_unique_name})")

        if direction == "high":
            filter_parts.append(f"{measure_unique_name} > {avg_name}")
        else:
            filter_parts.append(f"{measure_unique_name} < {avg_name}")

    columns_expr = "{ " + ", ".join(selected_measures) + " }"
    filter_expr = " AND ".join(filter_parts)
    with_clause = "WITH " + " ".join(with_members) if with_members else ""

    mdx = f"""
{with_clause}
SELECT
{columns_expr} ON COLUMNS,
FILTER(
  {rows_expr},
  {filter_expr}
) ON ROWS
FROM [{cube_name}];
"""
    return " ".join(mdx.split())


def _build_listing_support_filter(rows_expr: str, support_measure: str, year_member: Optional[str] = None) -> str:
    if year_member:
        return f"NONEMPTY({rows_expr}, {{ ({support_measure}, {year_member}) }})"
    return f"NONEMPTY({rows_expr}, {{ {support_measure} }})"


def _extract_single_hierarchy_row_info(rows_expr: str) -> Optional[Tuple[str, str, str]]:
    if not isinstance(rows_expr, str):
        return None
    m = re.fullmatch(r"\[([^\]]+)\]\.\[([^\]]+)\]\.\[([^\]]+)\]\.Members", rows_expr.strip())
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3)


def _get_deeper_level_name(schema: Dict[str, Any], dim_name: str, hierarchy_name: str, current_level_name: str) -> Optional[str]:
    dim_n = _norm(dim_name)
    hier_n = _norm(hierarchy_name)
    level_n = _norm(current_level_name)

    natural = schema.get("natural_hierarchies", {}) or {}
    for snap_dim_name, hierarchies in natural.items():
        if _norm(snap_dim_name) != dim_n:
            continue
        for h in hierarchies or []:
            h_name = (h.get("mdx_name") or h.get("name") or "").strip()
            if _norm(h_name) != hier_n:
                continue

            levels = h.get("levels", []) or []
            normalized = []
            for lvl in levels:
                if isinstance(lvl, dict):
                    mdx_name = (lvl.get("source_column") or lvl.get("name") or lvl.get("mdx_name") or "").strip()
                else:
                    mdx_name = str(lvl).strip()
                normalized.append(mdx_name)

            current_idx = None
            for i, lvl_name in enumerate(normalized):
                if _norm(lvl_name) == level_n:
                    current_idx = i
                    break

            if current_idx is None:
                continue

            if current_idx + 1 < len(normalized):
                return normalized[-1]
            return normalized[current_idx]

    return None


def _pick_primary_measure_name(mset: List[str]) -> Optional[str]:
    if not mset:
        return None
    m = mset[0]
    match = re.search(r"\[Measures\]\.\[([^\]]+)\]", m)
    return match.group(1) if match else None


def _build_explicit_agg_mdx(cube_name: str, rows_expr: str, base_measure_name: str, agg_intent: str, where_clause: str, schema: Optional[Dict[str, Any]]) -> Optional[str]:
    if not schema:
        return None

    row_info = _extract_single_hierarchy_row_info(rows_expr)
    if not row_info:
        return None

    dim_name, hierarchy_name, current_level_name = row_info
    leaf_level_name = _get_deeper_level_name(schema, dim_name, hierarchy_name, current_level_name)
    if not leaf_level_name:
        return None

    calc_measure_name = f"{agg_intent} {base_measure_name}"
    descendants_expr = f"DESCENDANTS([{dim_name}].[{hierarchy_name}].CurrentMember, [{dim_name}].[{hierarchy_name}].[{leaf_level_name}])"

    if agg_intent == "Sum":
        expr = f"SUM({descendants_expr}, [Measures].[{base_measure_name}])"
    elif agg_intent == "Avg":
        expr = f"AVG({descendants_expr}, [Measures].[{base_measure_name}])"
    elif agg_intent == "Count":
        expr = f"COUNT(NONEMPTY({descendants_expr}, [Measures].[{base_measure_name}]))"
    else:
        return None

    mdx = f"""
WITH
MEMBER [Measures].[{calc_measure_name}] AS {expr}
SELECT
{{ [Measures].[{calc_measure_name}] }} ON COLUMNS,
NON EMPTY {rows_expr} ON ROWS
FROM [{cube_name}]{where_clause};
"""
    return " ".join(mdx.split())


# =========================================================
# SPECIAL PROMPT HANDLERS
# =========================================================

def _build_last_n_days_zero_sales_mdx(
    cube_name: str,
    rows_expr: str,
    sales_measure: str,
    date_members_expr: str,
    days_count: int,
    calc_name: str
) -> str:
    mdx = f"""
WITH
MEMBER [Measures].[{calc_name}] AS
SUM(
    TAIL({date_members_expr}, {days_count}),
    COALESCEEMPTY({sales_measure}, 0)
)
SELECT
{{ [Measures].[{calc_name}] }} ON COLUMNS,
FILTER(
    {rows_expr},
    [Measures].[{calc_name}] = 0
) ON ROWS
FROM [{cube_name}];
"""
    return " ".join(mdx.split())


def _build_last_n_days_low_sales_mdx(
    cube_name: str,
    rows_expr: str,
    sales_measure: str,
    date_members_expr: str,
    days_count: int,
    calc_name: str
) -> str:
    mdx = f"""
WITH
MEMBER [Measures].[{calc_name}] AS
SUM(
    TAIL({date_members_expr}, {days_count}),
    COALESCEEMPTY({sales_measure}, 0)
)
MEMBER [Measures].[Avg {calc_name}] AS
AVG(
    {rows_expr},
    [Measures].[{calc_name}]
)
SELECT
{{ [Measures].[{calc_name}] }} ON COLUMNS,
FILTER(
    {rows_expr},
    [Measures].[{calc_name}] < [Measures].[Avg {calc_name}]
) ON ROWS
FROM [{cube_name}];
"""
    return " ".join(mdx.split())


def _build_year_zero_sales_mdx(
    cube_name: str,
    rows_expr: str,
    sales_measure: str,
    year_member: str,
    calc_name: str
) -> str:
    mdx = f"""
WITH
MEMBER [Measures].[{calc_name}] AS
COALESCEEMPTY(({sales_measure}, {year_member}), 0)
SELECT
{{ [Measures].[{calc_name}] }} ON COLUMNS,
FILTER(
  {rows_expr},
  [Measures].[{calc_name}] = 0
) ON ROWS
FROM [{cube_name}];
"""
    return " ".join(mdx.split())


def _build_two_year_drop_mdx(
    cube_name: str,
    rows_expr: str,
    sales_measure: str,
    year_member_1: str,
    year_member_2: str
) -> str:
    mdx = f"""
WITH
MEMBER [Measures].[Sales Y1] AS COALESCEEMPTY(({sales_measure}, {year_member_1}), 0)
MEMBER [Measures].[Sales Y2] AS COALESCEEMPTY(({sales_measure}, {year_member_2}), 0)
MEMBER [Measures].[Drop Amount] AS [Measures].[Sales Y1] - [Measures].[Sales Y2]
SELECT
{{ [Measures].[Sales Y1], [Measures].[Sales Y2], [Measures].[Drop Amount] }} ON COLUMNS,
FILTER(
  {rows_expr},
  [Measures].[Sales Y2] < [Measures].[Sales Y1]
) ON ROWS
FROM [{cube_name}];
"""
    return " ".join(mdx.split())


def _resolve_average_comparison_prompt(
    user_prompt: str,
    schema: Optional[Dict[str, Any]],
    fact_table: str
) -> Optional[Tuple[List[Tuple[str, str]], List[str]]]:
    t = (user_prompt or "").lower()

    rules = [
        {
            "match_all": [
                ["marge"],
                ["supérieure à la moyenne", "superieure a la moyenne", "supérieur à la moyenne", "superieur a la moyenne"],
                ["ventes", "chiffre d’affaires", "chiffre d'affaires", "chiffre d affaires"],
                ["inférieures à la moyenne", "inferieures a la moyenne", "inférieure à la moyenne", "inferieure a la moyenne"],
            ],
            "conditions": [("Margin", "high"), ("Line Total", "low")],
        },
        {
            "match_all": [
                ["prix", "prix unitaire", "prix de vente"],
                ["supérieur à la moyenne", "superieur a la moyenne", "supérieure à la moyenne", "superieure a la moyenne"],
                ["satisfaction", "note", "rating"],
                ["inférieure à la moyenne", "inferieure a la moyenne", "inférieures à la moyenne", "inferieures a la moyenne"],
            ],
            "conditions": [("Unit Price", "high"), ("Rating", "low")],
        },
        {
            "match_all": [
                ["chiffre d’affaires", "chiffre d'affaires", "chiffre d affaires", "ventes"],
                ["inférieur à la moyenne", "inferieur a la moyenne", "inférieure à la moyenne", "inferieure a la moyenne"],
                ["quantité vendue", "quantite vendue", "volume de vente", "order qty"],
                ["élevée", "elevee", "supérieure à la moyenne", "superieure a la moyenne"],
            ],
            "conditions": [("Line Total", "low"), ("Order Qty", "high")],
        },
        {
            "match_all": [
                ["note", "rating", "satisfaction"],
                ["supérieure à la moyenne", "superieure a la moyenne", "supérieur à la moyenne", "superieur a la moyenne"],
                ["marge"],
                ["inférieure à la moyenne", "inferieure a la moyenne", "inférieur à la moyenne", "inferieur a la moyenne"],
            ],
            "conditions": [("Rating", "high"), ("Margin", "low")],
        },
        {
            "match_all": [
                ["coût standard", "cout standard", "standard cost"],
                ["supérieur à la moyenne", "superieur a la moyenne", "supérieure à la moyenne", "superieure a la moyenne"],
                ["marge"],
                ["inférieure à la moyenne", "inferieure a la moyenne", "inférieur à la moyenne", "inferieur a la moyenne"],
            ],
            "conditions": [("Standard Cost", "high"), ("Margin", "low")],
        },
    ]

    def has_one(text: str, variants: List[str]) -> bool:
        return any(v in text for v in variants)

    for rule in rules:
        if all(has_one(t, group) for group in rule["match_all"]):
            conditions = []
            selected_measures = []
            for measure_name, direction in rule["conditions"]:
                unique_name = _measure_unique_name(measure_name, fact_table, schema)
                conditions.append((unique_name, direction))
                if unique_name not in selected_measures:
                    selected_measures.append(unique_name)
            return conditions, selected_measures

    return None


def _build_prompt_special_mdx(
    user_prompt: str,
    cube_name: str,
    dims: List[Dict[str, Any]],
    schema: Optional[Dict[str, Any]],
    fact_table: str
) -> Optional[str]:
    t = (user_prompt or "").lower()
    rows_expr = _pick_primary_rows_expr_for_filter(dims, schema, user_prompt)

    sales_measure = _measure_unique_name("Line Total", "FactProductSales", schema)
    qty_measure = _measure_unique_name("Order Qty", "FactProductSales", schema)
    margin_measure = _measure_unique_name("Margin", "FactProductSales", schema)
    std_cost_measure = _measure_unique_name("Standard Cost", "FactProductSales", schema)
    unit_price_measure = _measure_unique_name("Unit Price", "FactProductSales", schema)
    review_count_measure = _measure_unique_name("Review Count", "FactReviews", schema)

    full_date_members = _members("DimDate", "FullDate", schema)

    if (
        _contains_any(t, ["coût standard", "cout standard", "standard cost"])
        and _contains_any(t, ["faible chiffre d’affaires", "faible chiffre d'affaires", "faible chiffre d affaires"])
    ):
        return _build_relative_filter_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            conditions=[(std_cost_measure, "high"), (sales_measure, "low")],
            selected_measures=[std_cost_measure, sales_measure],
        )

    if (
        _contains_any(t, ["très vendus", "tres vendus", "volume de vente important", "volume des ventes important"])
        and _contains_any(t, ["peu profitables", "peu rentable", "peu rentables", "rentabilité faible", "rentabilite faible"])
    ):
        return _build_relative_filter_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            conditions=[(qty_measure, "high"), (margin_measure, "low")],
            selected_measures=[qty_measure, margin_measure],
        )

    if (
        _contains_any(t, ["aucune vente", "aucune ventes", "n’ont généré aucune vente", "n'ont généré aucune vente"])
        and _contains_any(t, ["3 derniers mois", "trois derniers mois"])
    ):
        return _build_last_n_days_zero_sales_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            sales_measure=sales_measure,
            date_members_expr=full_date_members,
            days_count=90,
            calc_name="SalesLast3Months"
        )

    if (
        _contains_any(t, ["aucune vente", "aucune ventes", "n’ont généré aucune vente", "n'ont généré aucune vente"])
        and _contains_any(t, ["12 derniers mois", "douze derniers mois"])
    ):
        return _build_last_n_days_zero_sales_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            sales_measure=sales_measure,
            date_members_expr=full_date_members,
            days_count=365,
            calc_name="SalesLast12Months"
        )

    if _contains_any(t, ["ventes presque nulles", "presque nulles", "quasi nulles"]):
        return _build_relative_filter_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            conditions=[(sales_measure, "low")],
            selected_measures=[sales_measure],
        )

    if _contains_any(t, ["marge nulle", "marge très faible", "marge tres faible", "marge faible"]):
        return _build_relative_filter_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            conditions=[(margin_measure, "low")],
            selected_measures=[margin_measure],
        )

    if (
        _contains_any(t, ["aucun avis", "sans avis", "n’ont reçu aucun avis", "n'ont reçu aucun avis"])
        and _contains_any(t, ["prix élevé", "prix eleve", "prix unitaire élevé", "prix unitaire eleve", "prix de vente élevé", "prix de vente eleve"])
    ):
        return _build_relative_filter_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            conditions=[(review_count_measure, "low"), (unit_price_measure, "high")],
            selected_measures=[review_count_measure, unit_price_measure],
        )

    if (
        _contains_any(t, ["peu vendus", "faiblement vendus", "peu de ventes"])
        and _contains_any(t, ["peu rentables", "peu profitable", "peu profitables", "rentabilité faible", "rentabilite faible"])
    ):
        return _build_relative_filter_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            conditions=[(qty_measure, "low"), (margin_measure, "low")],
            selected_measures=[qty_measure, margin_measure],
        )

    return None


def _build_prompt_special_mdx_v2(
    user_prompt: str,
    cube_name: str,
    dims: List[Dict[str, Any]],
    schema: Optional[Dict[str, Any]],
    fact_table: str
) -> Optional[str]:
    t = (user_prompt or "").lower()
    rows_expr = _pick_primary_rows_expr_for_filter(dims, schema, user_prompt)

    sales_measure = _measure_unique_name("Line Total", "FactProductSales", schema)
    qty_measure = _measure_unique_name("Order Qty", "FactProductSales", schema)
    margin_measure = _measure_unique_name("Margin", "FactProductSales", schema)
    unit_price_measure = _measure_unique_name("Unit Price", "FactProductSales", schema)
    rating_measure = _measure_unique_name("Rating", "FactReviews", schema)

    date_full_expr = _members("DimDate", "FullDate", schema)
    date_dim, year_attr = _resolve_date_dim_year(dims, schema)
    years = _find_all_years_in_text(user_prompt)

    if (
        _contains_any(t, ["marge élevée", "marge elevee", "marge haute"])
        and _contains_any(t, ["faible volume de vente", "volume de vente faible", "ventes faibles", "faible quantité vendue"])
        and len(years) >= 1
        and date_dim and year_attr
    ):
        year_member = _member_key(date_dim, year_attr, years[0], schema)
        mdx = f"""
WITH
MEMBER [Measures].[Year Margin] AS COALESCEEMPTY(({margin_measure}, {year_member}), 0)
MEMBER [Measures].[Year Qty] AS COALESCEEMPTY(({qty_measure}, {year_member}), 0)
MEMBER [Measures].[Avg Year Margin] AS AVG({rows_expr}, [Measures].[Year Margin])
MEMBER [Measures].[Avg Year Qty] AS AVG({rows_expr}, [Measures].[Year Qty])
SELECT
{{ [Measures].[Year Margin], [Measures].[Year Qty] }} ON COLUMNS,
FILTER(
    {rows_expr},
    [Measures].[Year Margin] > [Measures].[Avg Year Margin]
    AND
    [Measures].[Year Qty] < [Measures].[Avg Year Qty]
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    if (
        _contains_any(t, ["mauvaise satisfaction", "satisfaction faible", "mauvaise satisfaction client"])
        and _contains_any(t, ["prix élevé", "prix eleve", "prix unitaire élevé", "prix unitaire eleve"])
        and len(years) >= 1
        and date_dim and year_attr
    ):
        year_member = _member_key(date_dim, year_attr, years[0], schema)
        mdx = f"""
WITH
MEMBER [Measures].[Year Price] AS COALESCEEMPTY(({unit_price_measure}, {year_member}), 0)
MEMBER [Measures].[Year Rating] AS COALESCEEMPTY(({rating_measure}, {year_member}), 0)
MEMBER [Measures].[Avg Year Price] AS AVG({rows_expr}, [Measures].[Year Price])
MEMBER [Measures].[Avg Year Rating] AS AVG({rows_expr}, [Measures].[Year Rating])
SELECT
{{ [Measures].[Year Price], [Measures].[Year Rating] }} ON COLUMNS,
FILTER(
    {rows_expr},
    [Measures].[Year Price] > [Measures].[Avg Year Price]
    AND
    [Measures].[Year Rating] < [Measures].[Avg Year Rating]
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    if (
        _contains_any(t, ["aucune vente", "aucune ventes", "sans ventes", "n’ont réalisé aucune vente", "n'ont réalisé aucune vente"])
        and len(years) >= 1
        and date_dim and year_attr
    ):
        year_member = _member_key(date_dim, year_attr, years[0], schema)
        return _build_year_zero_sales_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            sales_measure=sales_measure,
            year_member=year_member,
            calc_name=f"Sales_{years[0]}"
        )

    if (
        _contains_any(t, ["forte baisse", "baisse importante", "baisse de chiffre d’affaires", "baisse de chiffre d'affaires"])
        and len(years) >= 2
        and date_dim and year_attr
    ):
        y1, y2 = years[0], years[1]
        return _build_two_year_drop_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            sales_measure=sales_measure,
            year_member_1=_member_key(date_dim, year_attr, y1, schema),
            year_member_2=_member_key(date_dim, year_attr, y2, schema),
        )

    if _contains_any(t, ["moins performants", "les moins performants", "6 derniers mois", "six derniers mois"]):
        return _build_last_n_days_low_sales_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            sales_measure=sales_measure,
            date_members_expr=date_full_expr,
            days_count=180,
            calc_name="SalesLast6Months"
        )

    if (
        _contains_any(t, ["marge élevée", "marge elevee"])
        and _contains_any(t, ["ventes faibles", "faibles ventes"])
    ):
        return _build_relative_filter_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            conditions=[(margin_measure, "high"), (sales_measure, "low")],
            selected_measures=[margin_measure, sales_measure],
        )

    if (
        _contains_any(t, ["prix élevé", "prix eleve", "prix unitaire élevé", "prix unitaire eleve"])
        and _contains_any(t, ["satisfaction faible", "mauvaise satisfaction", "note faible"])
    ):
        return _build_relative_filter_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            conditions=[(unit_price_measure, "high"), (rating_measure, "low")],
            selected_measures=[unit_price_measure, rating_measure],
        )

    if _contains_any(t, ["sans ventes sur les 6 derniers mois", "sans vente sur les 6 derniers mois", "produits sans ventes sur les 6 derniers mois"]):
        return _build_last_n_days_zero_sales_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            sales_measure=sales_measure,
            date_members_expr=date_full_expr,
            days_count=180,
            calc_name="SalesLast6Months"
        )

    if (
        _contains_any(t, ["très vendus", "tres vendus"])
        and _contains_any(t, ["marge faible", "faible marge"])
    ):
        return _build_relative_filter_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            conditions=[(qty_measure, "high"), (margin_measure, "low")],
            selected_measures=[qty_measure, margin_measure],
        )

    return None


def _build_semantic_candidate_measures(user_prompt: str) -> List[Dict[str, str]]:
    t = (user_prompt or "").lower()
    candidates: List[Dict[str, str]] = []

    def add(name: str) -> None:
        if not any(_norm(c["name"]) == _norm(name) for c in candidates):
            candidates.append({"name": name})

    # ===== Sujet 2 : Supply Risk =====
    if any(x in t for x in ["retard", "retards", "en retard", "late", "delay", "delai", "délai"]):
        add("Late Flag")
        add("Delay Days")

    if any(x in t for x in ["montant", "montants", "achat", "achats", "line amount"]):
        add("Line Amount")

    if any(x in t for x in ["fournisseur", "fournisseurs", "vendor", "vendors"]):
        add("Late Flag")

    if any(x in t for x in ["commande d'achat", "commandes d'achat", "commandes d achat", "purchase order"]):
        add("Late Flag")
    
        # ===== Sujet 2 : Production Impact =====
    if any(x in t for x in ["ordre de fabrication", "ordres de fabrication", "work order", "work orders"]):
        add("Risk Flag")
        add("Scrap Rate")

    if any(x in t for x in ["fragilisé", "fragilises", "fragilisés", "fragilise", "fragilisee"]):
        add("Risk Flag")
        add("Scrapped Qty")
        add("Scrap Rate")
    
    if any(x in t for x in ["quantités planifiées", "quantite planifiee", "quantites planifiees", "planned quantities", "planned qty"]):
        add("Order Qty")
        add("Risk Flag")

    if any(x in t for x in ["scrap", "scrapped", "scrapped qty"]):
        add("Scrapped Qty")

    if any(x in t for x in ["scrap rate", "taux de rebut", "taux rebut"]):
        add("Scrap Rate")

    if any(x in t for x in ["risque", "risk flag", "bloqué", "blocked", "menacées", "menacees"]):
        add("Risk Flag")
    
        # ===== Sujet 2 : Inventory Movement =====
    if any(x in t for x in ["mouvement", "mouvements", "movement", "movements"]):
        add("Movement Count")
        add("Quantity")

    if any(x in t for x in ["stock", "inventaire", "inventory", "sortent vite du stock", "rotation rapide"]):
        add("Quantity")
        add("Movement Count")

    if any(x in t for x in ["instable", "instables", "stock instable"]):
        add("Movement Count")
        add("Quantity")
        add("Total Movement Cost")

    if any(x in t for x in ["tension d’inventaire", "tension d'inventaire", "inventory tension", "signes précoces", "signes precoces"]):
        add("Movement Count")
        add("Quantity")
        add("Actual Cost")
        add("Total Movement Cost")

    if any(x in t for x in ["actual cost", "coût réel", "cout reel"]):
        add("Actual Cost")

    if any(x in t for x in ["total movement cost", "coût total de mouvement", "cout total de mouvement"]):
        add("Total Movement Cost")
    # ===== Sujet 1 / existant =====
    if any(x in t for x in [
        "très vendus", "tres vendus", "volume de vente", "volume des ventes",
        "quantité vendue", "quantite vendue", "très peu de commandes",
        "tres peu de commandes", "peu de commandes", "commandes", "peu vendus"
    ]):
        add("Order Qty")

    if any(x in t for x in [
        "marge", "margin", "profit", "profitables", "profitable",
        "peu profitables", "rentabilité", "rentabilite", "peu rentables",
        "marge nulle", "marge très faible", "marge tres faible"
    ]):
        add("Margin")

    if any(x in t for x in [
        "baisse des ventes", "ventes en baisse", "faible chiffre d’affaires",
        "faible chiffre d'affaires", "faible chiffre d affaires",
        "chiffre d’affaires", "chiffre d'affaires", "chiffre d affaires",
        "sales", "revenue", "aucune vente", "ventes presque nulles", "quasi nulles"
    ]):
        add("Line Total")

    if any(x in t for x in ["coût standard", "cout standard", "standard cost"]):
        add("Standard Cost")

    if any(x in t for x in ["prix unitaire", "prix de vente", "unit price", "prix élevé", "prix eleve"]):
        add("Unit Price")

    if any(x in t for x in ["rating", "note", "note client", "satisfaction"]):
        add("Rating")

    if any(x in t for x in ["avis", "review count", "reviews", "nombre d'avis", "nombre d avis", "aucun avis", "sans avis"]):
        add("Review Count")

    return candidates


def _extract_relative_measure_conditions(
    user_prompt: str,
    measures: List[Dict[str, Any]],
    fact_table: str,
    schema: Optional[Dict[str, Any]]
) -> List[Tuple[str, str]]:
    t = (user_prompt or "").lower()
    normalized_prompt = _norm(t)
    conditions: List[Tuple[str, str]] = []

    for m in measures:
        if not isinstance(m, dict):
            continue

        measure_name = (m.get("name") or m.get("caption") or m.get("column") or "").strip()
        if not measure_name:
            continue

        measure_fact = _find_fact_for_measure(schema, measure_name) or fact_table
        unique_measure = _measure_unique_name(measure_name, measure_fact, schema)
        aliases = {_norm(measure_name)}

        if _norm(measure_name) == _norm("Order Qty"):
            aliases.update({
                _norm("quantité vendue"), _norm("quantite vendue"), _norm("qty"),
                _norm("quantity"), _norm("volume de vente"), _norm("volume des ventes"),
                _norm("très vendus"), _norm("tres vendus"), _norm("peu vendus"),
                _norm("très peu de commandes"), _norm("tres peu de commandes"),
                _norm("peu de commandes"),
            })

        if _norm(measure_name) == _norm("Margin"):
            aliases.update({
                _norm("marge"), _norm("profit"), _norm("rentabilité"),
                _norm("rentabilite"), _norm("peu profitables"), _norm("peu rentables"),
                _norm("marge élevée"), _norm("marge elevee"),
                _norm("marge faible"), _norm("marge très faible"), _norm("marge tres faible"),
            })

        if _norm(measure_name) == _norm("Line Total"):
            aliases.update({
                _norm("chiffre d'affaires"), _norm("chiffre d affaires"),
                _norm("sales"), _norm("revenue"), _norm("faible chiffre d'affaires"),
                _norm("faible chiffre d affaires"), _norm("baisse des ventes"),
                _norm("ventes en baisse"), _norm("aucune vente"), _norm("ventes presque nulles"),
            })

        if _norm(measure_name) == _norm("Standard Cost"):
            aliases.update({
                _norm("coût standard"), _norm("cout standard"), _norm("standard cost"),
                _norm("coût standard élevé"), _norm("cout standard eleve"),
            })

        if _norm(measure_name) == _norm("Unit Price"):
            aliases.update({
                _norm("prix de vente"), _norm("prix"), _norm("unit price"),
                _norm("prix unitaire"), _norm("prix unitaire élevé"), _norm("prix unitaire eleve"),
                _norm("prix élevé"), _norm("prix eleve"),
            })

        if _norm(measure_name) == _norm("Rating"):
            aliases.update({_norm("rating"), _norm("note"), _norm("note client"), _norm("satisfaction")})

        if _norm(measure_name) == _norm("Review Count"):
            aliases.update({_norm("avis"), _norm("reviews"), _norm("nombre d'avis"), _norm("nombre d avis"), _norm("aucun avis")})

        if _norm(measure_name) == _norm("Late Flag"):
            aliases.update({_norm("retard"), _norm("retards"), _norm("late flag"), _norm("late")})

        if _norm(measure_name) == _norm("Delay Days"):
            aliases.update({_norm("delay"), _norm("delay days"), _norm("jours de retard"), _norm("retard")})

        if _norm(measure_name) == _norm("Line Amount"):
            aliases.update({_norm("montant"), _norm("montants"), _norm("achat"), _norm("achats"), _norm("line amount")})

        found_alias = None
        for a in aliases:
            if a and a in normalized_prompt:
                found_alias = a
                break

        if not found_alias:
            continue

        idx = normalized_prompt.find(found_alias)
        window = t[max(0, idx - 80): idx + 120] if idx >= 0 else t

        high_words = ["élevé", "élevée", "élevés", "élevées", "haut", "haute", "high", "important", "fort", "très", "tres", "supérieur", "supérieure"]
        low_words = ["faible", "faibles", "bas", "basse", "low", "petit", "petite", "peu", "baisse", "inférieur", "inférieure", "aucun", "nulle"]

        if any(w in window for w in high_words):
            conditions.append((unique_measure, "high"))
        elif any(w in window for w in low_words):
            conditions.append((unique_measure, "low"))

    seen = set()
    out = []
    for measure_unique_name, direction in conditions:
        key = (measure_unique_name, direction)
        if key not in seen:
            seen.add(key)
            out.append((measure_unique_name, direction))

    return out


# =========================================================
# VALIDATION
# =========================================================



# =========================================================
# FINAL BUILDER HELPERS
# =========================================================

def _requested_year_available(schema: Optional[Dict[str, Any]], year_value: int) -> bool:
    if not schema:
        return True

    available_members = schema.get("available_members") or {}
    dim_date = available_members.get("DimDate") or {}
    years = dim_date.get("Year") or dim_date.get("YearNumber") or None

    if years is None:
        return True

    normalized_years = {str(y).strip() for y in years}
    return str(year_value) in normalized_years


def _same_date_hierarchy_filter_expr(schema: Optional[Dict[str, Any]], rows_expr: str, year_value: int) -> Optional[str]:
    if not schema or not isinstance(rows_expr, str):
        return None

    m = re.fullmatch(r"\[([^\]]+)\]\.\[([^\]]+)\]\.\[([^\]]+)\]\.Members", rows_expr.strip())
    if not m:
        return None

    dim_name, hierarchy_name, current_level = m.group(1), m.group(2), m.group(3)
    if _norm(dim_name) != _norm("DimDate") or _norm(hierarchy_name) != _norm("Calendar"):
        return None

    natural = schema.get("natural_hierarchies", {}) or {}
    levels = []
    for dname, hierarchies in natural.items():
        if _norm(dname) != _norm(dim_name):
            continue
        for h in hierarchies or []:
            hname = (h.get("mdx_name") or h.get("name") or "").strip()
            if _norm(hname) != _norm(hierarchy_name):
                continue
            for lvl in h.get("levels", []) or []:
                if isinstance(lvl, dict):
                    levels.append((lvl.get("name") or "", lvl.get("mdx_name") or "", lvl.get("source_column") or ""))
                else:
                    levels.append((str(lvl), str(lvl), str(lvl)))
            break

    year_level_name = None
    current_level_name = current_level

    for logical_name, mdx_name, source_col in levels:
        if _norm(logical_name) in {_norm("Year"), _norm("YearNumber")} or _norm(mdx_name) in {_norm("Year"), _norm("Year Number")}:
            year_level_name = source_col or logical_name or mdx_name
        if _norm(current_level) in {_norm(logical_name), _norm(mdx_name), _norm(source_col)}:
            current_level_name = source_col or logical_name or mdx_name

    if not year_level_name:
        year_level_name = "Year"

    return f"DESCENDANTS([{dim_name}].[{hierarchy_name}].[{year_level_name}].&[{year_value}], [{dim_name}].[{hierarchy_name}].[{current_level_name}])"


# =========================================================
# SPECIAL PROMPT HANDLERS - SUJET 2 (SUPPLY RISK)
# =========================================================

def _is_supply_risk_prompt(user_prompt: str) -> bool:
    t = (user_prompt or "").lower()

    supply_words = [
        "fournisseur", "fournisseurs", "vendor", "vendors",
        "retard", "retards", "en retard", "late", "delay", "delai", "délai",
        "commande", "commandes", "purchase order", "purchase orders",
        "achat", "achats", "montant d'achat", "montants d'achats",
        "line amount", "delay days", "late flag"
    ]
    return any(w in t for w in supply_words)


def _pick_supply_risk_rows_expr(schema: Optional[Dict[str, Any]], user_prompt: str) -> str:
    t = (user_prompt or "").lower()

    if any(x in t for x in ["fournisseur", "fournisseurs", "vendor", "vendors"]):
        return _members("DimVendor", "VendorName", schema)

    if any(x in t for x in ["produit", "produits", "product", "products"]):
        return _members("DimProduct", "ProductName", schema)

    if any(x in t for x in [
        "commande", "commandes", "purchase order", "purchase orders",
        "commande d'achat", "commandes d'achat", "commandes d achat"
    ]):
        return _members("DimPurchaseOrder", "PurchaseOrderID", schema)

    return _members("DimVendor", "VendorName", schema)


def _build_supply_risk_prompt_mdx(
    user_prompt: str,
    cube_name: str,
    schema: Optional[Dict[str, Any]]
) -> Optional[str]:
    t = (user_prompt or "").lower()

    late_flag_measure = _measure_unique_name("Late Flag", "FactSupplyRisk", schema)
    delay_days_measure = _measure_unique_name("Delay Days", "FactSupplyRisk", schema)
    line_amount_measure = _measure_unique_name("Line Amount", "FactSupplyRisk", schema)

    if (
        any(x in t for x in ["fournisseur", "fournisseurs", "vendor", "vendors"])
        and any(x in t for x in ["retard", "retards", "en retard", "late"])
    ):
        rows_expr = _members("DimVendor", "VendorName", schema)
        mdx = f"""
SELECT
{{ {late_flag_measure} }} ON COLUMNS,
NON EMPTY FILTER(
  {rows_expr},
  {late_flag_measure} > 0
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    if (
        any(x in t for x in ["produit", "produits", "product", "products"])
        and any(x in t for x in ["retard", "retards", "en retard", "late"])
        and any(x in t for x in ["plus touchés", "plus touches", "most affected", "les plus touchés"])
    ):
        rows_expr = _members("DimProduct", "ProductName", schema)
        mdx = f"""
SELECT
{{ {late_flag_measure}, {delay_days_measure} }} ON COLUMNS,
NON EMPTY TOPCOUNT(
  FILTER({rows_expr}, {late_flag_measure} > 0),
  10,
  {delay_days_measure}
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    if any(x in t for x in [
        "montants d’achats", "montants d'achats", "montant d’achat", "montant d'achat",
        "montants achats", "montant achat", "achats concernés", "achats concernes"
    ]):
        rows_expr = _members("DimPurchaseOrder", "PurchaseOrderID", schema)
        mdx = f"""
SELECT
{{ {line_amount_measure}, {late_flag_measure} }} ON COLUMNS,
NON EMPTY FILTER(
  {rows_expr},
  {late_flag_measure} > 0
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    if any(x in t for x in [
        "fréquence des commandes en retard", "frequence des commandes en retard",
        "fréquence", "frequence"
    ]) and any(x in t for x in ["commande", "commandes", "purchase order", "retard", "late"]):
        rows_expr = _members("DimPurchaseOrder", "PurchaseOrderID", schema)
        mdx = f"""
SELECT
{{ {late_flag_measure} }} ON COLUMNS,
NON EMPTY FILTER(
  {rows_expr},
  {late_flag_measure} > 0
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    if (
        any(x in t for x in ["retard", "retards", "late", "delay"])
        and any(x in t for x in ["fournisseur", "fournisseurs", "vendor", "vendors"])
    ):
        rows_expr = _members("DimVendor", "VendorName", schema)
        mdx = f"""
SELECT
{{ {late_flag_measure} }} ON COLUMNS,
NON EMPTY FILTER(
  {rows_expr},
  {late_flag_measure} > 0
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    return None


# =========================================================
# FINAL BUILDER
# =========================================================
# =========================================================
# SPECIAL PROMPT HANDLERS - SUJET 2 (PRODUCTION IMPACT)
# =========================================================

def _is_production_impact_prompt(user_prompt: str) -> bool:
    t = (user_prompt or "").lower()

    production_words = [
        "ordre de fabrication", "ordres de fabrication",
        "work order", "work orders",
        "production", "fabrication",
        "bloqué", "bloques", "bloqué", "bloquées", "bloques", "blocked",
        "fragilisé", "fragilises", "fragilisés", "fragilise", "fragilisée", "fragilisee",
        "approvisionnement", "supply", "problème d’approvisionnement", "probleme d'approvisionnement",
        "quantités planifiées", "quantite planifiee", "quantités menacées", "quantites menacees",
        "scrap", "scrapped", "scrapped qty", "scrap rate", "risk flag", "risque"
    ]
    return any(w in t for w in production_words)


def _build_production_impact_prompt_mdx(
    user_prompt: str,
    cube_name: str,
    schema: Optional[Dict[str, Any]]
) -> Optional[str]:
    t = (user_prompt or "").lower()

    risk_flag_measure = _measure_unique_name("Risk Flag", "FactProductionImpact", schema)
    order_qty_measure = _measure_unique_name("Order Qty", "FactProductionImpact", schema)
    scrapped_qty_measure = _measure_unique_name("Scrapped Qty", "FactProductionImpact", schema)
    scrap_rate_measure = _measure_unique_name("Scrap Rate", "FactProductionImpact", schema)

    workorder_rows = _members("DimWorkOrder", "WorkOrderID", schema)
    product_rows = _members("DimProduct", "ProductName", schema)

    # 1) Quels ordres de fabrication risquent d’être bloqués ?
    if (
        any(x in t for x in ["ordre de fabrication", "ordres de fabrication", "work order", "work orders"])
        and any(x in t for x in ["bloqué", "bloques", "bloqué", "bloquées", "blocked", "risquent"])
    ):
        mdx = f"""
SELECT
{{ {risk_flag_measure}, {scrap_rate_measure} }} ON COLUMNS,
NON EMPTY FILTER(
  {workorder_rows},
  {risk_flag_measure} > 0
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    # 2) Quels produits en production sont fragilisés ?
    if (
        any(x in t for x in ["produit", "produits", "product", "products"])
        and any(x in t for x in ["production", "fabrication"])
        and any(x in t for x in ["fragilisé", "fragilises", "fragilisés", "fragilise", "fragilisée", "fragilisee"])
    ):
        mdx = f"""
SELECT
{{ {risk_flag_measure}, {scrapped_qty_measure}, {scrap_rate_measure} }} ON COLUMNS,
NON EMPTY TOPCOUNT(
  FILTER({product_rows}, {risk_flag_measure} > 0),
  10,
  {scrapped_qty_measure}
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    # 3) Quel est l’effet des problèmes d’approvisionnement sur la fabrication ?
    if (
        any(x in t for x in ["approvisionnement", "supply", "problème d’approvisionnement", "probleme d'approvisionnement"])
        and any(x in t for x in ["fabrication", "production"])
    ):
        mdx = f"""
SELECT
{{ {risk_flag_measure}, {scrapped_qty_measure}, {scrap_rate_measure}, {order_qty_measure} }} ON COLUMNS,
NON EMPTY FILTER(
  {workorder_rows},
  {risk_flag_measure} > 0
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    # 4) Quelles quantités planifiées sont menacées ?
    if any(x in t for x in [
        "quantités planifiées", "quantite planifiee", "quantites planifiees",
        "quantités menacées", "quantites menacees", "planned quantities", "planned qty"
    ]):
        mdx = f"""
SELECT
{{ {order_qty_measure}, {risk_flag_measure}, {scrap_rate_measure} }} ON COLUMNS,
NON EMPTY FILTER(
  {workorder_rows},
  {risk_flag_measure} > 0
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    # variantes plus générales
    if (
        any(x in t for x in ["work order", "work orders", "ordre de fabrication", "ordres de fabrication"])
        and any(x in t for x in ["risque", "risk", "bloqué", "blocked"])
    ):
        mdx = f"""
SELECT
{{ {risk_flag_measure}, {scrap_rate_measure} }} ON COLUMNS,
NON EMPTY FILTER(
  {workorder_rows},
  {risk_flag_measure} > 0
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    if (
        any(x in t for x in ["produit", "produits", "product", "products"])
        and any(x in t for x in ["fragilisé", "fragilises", "fragilisés", "fragilise", "fragilisee", "menacé", "menaces", "menacés"])
    ):
        mdx = f"""
SELECT
{{ {risk_flag_measure}, {scrapped_qty_measure} }} ON COLUMNS,
NON EMPTY FILTER(
  {product_rows},
  {risk_flag_measure} > 0
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    return None

# =========================================================
# SPECIAL PROMPT HANDLERS - SUJET 2 (INVENTORY MOVEMENT)
# =========================================================

def _is_inventory_movement_prompt(user_prompt: str) -> bool:
    t = (user_prompt or "").lower()

    inventory_words = [
        "stock", "inventaire", "inventory",
        "mouvement", "mouvements", "movement", "movements",
        "sortent vite du stock", "rotation rapide", "sortie rapide",
        "fréquents", "frequents", "mouvements fréquents", "mouvements frequents",
        "instable", "instables", "stock instable",
        "tension d’inventaire", "tension d'inventaire", "inventory tension",
        "signes précoces", "signes precoces", "précoce", "precoce"
    ]
    return any(w in t for w in inventory_words)


def _build_inventory_movement_prompt_mdx(
    user_prompt: str,
    cube_name: str,
    schema: Optional[Dict[str, Any]]
) -> Optional[str]:
    t = (user_prompt or "").lower()

    quantity_measure = _measure_unique_name("Quantity", "FactInventoryMovement", schema)
    movement_count_measure = _measure_unique_name("Movement Count", "FactInventoryMovement", schema)
    actual_cost_measure = _measure_unique_name("Actual Cost", "FactInventoryMovement", schema)
    total_movement_cost_measure = _measure_unique_name("Total Movement Cost", "FactInventoryMovement", schema)

    product_rows = _members("DimProduct", "ProductName", schema)

    # 1) quels produits sortent vite du stock ?
    if any(x in t for x in [
        "sortent vite du stock", "sort vite du stock", "rotation rapide",
        "sortie rapide", "fast moving", "fast-moving"
    ]):
        mdx = f"""
SELECT
{{ {quantity_measure}, {movement_count_measure} }} ON COLUMNS,
NON EMPTY TOPCOUNT(
  {product_rows},
  10,
  {movement_count_measure}
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    # 2) quels produits ont des mouvements fréquents ?
    if any(x in t for x in [
        "mouvements fréquents", "mouvements frequents",
        "mouvements fréquents", "frequent movements", "mouvements"
    ]):
        mdx = f"""
SELECT
{{ {movement_count_measure}, {quantity_measure} }} ON COLUMNS,
NON EMPTY TOPCOUNT(
  {product_rows},
  10,
  {movement_count_measure}
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    # 3) quels produits ont un stock instable ?
    if any(x in t for x in [
        "stock instable", "stocks instables", "instable", "instables"
    ]):
        mdx = f"""
WITH
MEMBER [Measures].[Avg Movement Count] AS AVG({product_rows}, {movement_count_measure})
MEMBER [Measures].[Avg Quantity] AS AVG({product_rows}, {quantity_measure})
SELECT
{{ {movement_count_measure}, {quantity_measure}, {total_movement_cost_measure} }} ON COLUMNS,
FILTER(
  {product_rows},
  {movement_count_measure} > [Measures].[Avg Movement Count]
  AND
  ABS({quantity_measure}) > [Measures].[Avg Quantity]
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    # 4) y a-t-il des signes précoces de tension d’inventaire ?
    if any(x in t for x in [
        "signes précoces de tension d’inventaire",
        "signes précoces de tension d'inventaire",
        "signes precoces de tension d'inventaire",
        "tension d’inventaire", "tension d'inventaire", "inventory tension"
    ]):
        mdx = f"""
WITH
MEMBER [Measures].[Avg Movement Count] AS AVG({product_rows}, {movement_count_measure})
MEMBER [Measures].[Avg Quantity] AS AVG({product_rows}, {quantity_measure})
SELECT
{{ {movement_count_measure}, {quantity_measure}, {actual_cost_measure}, {total_movement_cost_measure} }} ON COLUMNS,
FILTER(
  {product_rows},
  {movement_count_measure} > [Measures].[Avg Movement Count]
  AND
  ABS({quantity_measure}) > [Measures].[Avg Quantity]
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    # variantes plus générales
    if any(x in t for x in ["mouvements", "movement"]) and any(x in t for x in ["produit", "produits", "product", "products"]):
        mdx = f"""
SELECT
{{ {movement_count_measure}, {quantity_measure} }} ON COLUMNS,
NON EMPTY TOPCOUNT(
  {product_rows},
  10,
  {movement_count_measure}
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    return None

def build_mdx(plan: Dict[str, Any], cube_name: str, user_prompt: str = "", schema: Optional[Dict[str, Any]] = None) -> str:
    safe_schema = schema or {}
    plan = normalize_plan_measure_facts(plan, safe_schema)
    plan = normalize_plan_dimensions(plan, safe_schema, user_prompt)

    measures = plan.get("measures", [])
    dims = plan.get("dimensions", [])
    fact_table = (plan.get("fact_table") or "").strip()

    pure_listing = _is_pure_dimension_listing_prompt(user_prompt)
    has_metric_intent = _has_metric_intent(user_prompt)
    is_listing_only = _is_listing_prompt(user_prompt) or pure_listing

    if pure_listing:
        has_metric_intent = False
        is_listing_only = True

    if has_metric_intent:
        is_listing_only = False

    review_count_by_category = (
        any(x in (user_prompt or "").lower() for x in ["nombre d’avis", "nombre d'avis", "reviews", "review count", "avis"])
        and any(x in (user_prompt or "").lower() for x in ["category", "catégorie", "categorie"])
    )

    if review_count_by_category:
        is_listing_only = False
        has_metric_intent = True

    mset = []
    if not is_listing_only:
        for m in measures:
            if not isinstance(m, dict):
                continue
            measure_name = (m.get("name") or m.get("caption") or m.get("column") or "").strip()
            if measure_name:
                measure_fact = _find_fact_for_measure(safe_schema, measure_name) or (m.get("fact_table") or fact_table)
                mset.append(_measure_unique_name(measure_name, measure_fact, safe_schema))

    if review_count_by_category and not mset:
        mset = [_measure_unique_name("Review Count", "FactReviews", safe_schema)]

    if not mset and not is_listing_only:
        default_measure, default_fact = _resolve_default_measure_name(user_prompt, safe_schema, fact_table)
        mset = [_measure_unique_name(default_measure, default_fact, safe_schema)]

    listing_support_measure = None
    if is_listing_only and _listing_needs_supporting_measure(user_prompt, dims):
        listing_support_measure = _resolve_supporting_measure(user_prompt, safe_schema, fact_table)

    columns = "{}" if is_listing_only else "{ " + ", ".join(mset) + " }"

    if (
        any(x in (user_prompt or "").lower() for x in ["produit", "produits", "product", "products"])
        and any(x in (user_prompt or "").lower() for x in ["retard", "retards", "late", "delay"])
        and any(x in (user_prompt or "").lower() for x in ["plus touchés", "plus touches", "most affected", "les plus touchés"])
    ):
        late_flag_measure = _measure_unique_name("Late Flag", "FactSupplyRisk", safe_schema)
        delay_days_measure = _measure_unique_name("Delay Days", "FactSupplyRisk", safe_schema)
        rows_expr = _members("DimProduct", "ProductName", safe_schema)

        mdx = f"""
SELECT
{{ {late_flag_measure}, {delay_days_measure} }} ON COLUMNS,
NON EMPTY TOPCOUNT(
  FILTER({rows_expr}, {late_flag_measure} > 0),
  10,
  {delay_days_measure}
) ON ROWS
FROM [{cube_name}];
"""
        return " ".join(mdx.split())

    if _is_supply_risk_prompt(user_prompt):
        supply_risk_mdx = _build_supply_risk_prompt_mdx(
            user_prompt=user_prompt,
            cube_name=cube_name,
            schema=safe_schema,
        )
        if supply_risk_mdx:
            return supply_risk_mdx
        
    special_mdx_v2 = _build_prompt_special_mdx_v2(
        user_prompt=user_prompt,
        cube_name=cube_name,
        dims=dims,
        schema=safe_schema,
        fact_table=fact_table,
    )
    if special_mdx_v2:
        return special_mdx_v2
    
    if _is_inventory_movement_prompt(user_prompt):
        inventory_movement_mdx = _build_inventory_movement_prompt_mdx(
            user_prompt=user_prompt,
            cube_name=cube_name,
            schema=safe_schema,
        )
        if inventory_movement_mdx:
            return inventory_movement_mdx
    if _is_production_impact_prompt(user_prompt):
        production_impact_mdx = _build_production_impact_prompt_mdx(
            user_prompt=user_prompt,
            cube_name=cube_name,
            schema=safe_schema,
        )
        if production_impact_mdx:
            return production_impact_mdx

    avg_compare = _resolve_average_comparison_prompt(
        user_prompt=user_prompt,
        schema=safe_schema,
        fact_table=fact_table,
    )
    if avg_compare:
        avg_conditions, avg_selected_measures = avg_compare
        rows_expr = _pick_primary_rows_expr_for_filter(dims, safe_schema, user_prompt)
        return _build_relative_filter_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            conditions=avg_conditions,
            selected_measures=avg_selected_measures,
        )

    special_mdx = _build_prompt_special_mdx(
        user_prompt=user_prompt,
        cube_name=cube_name,
        dims=dims,
        schema=safe_schema,
        fact_table=fact_table,
    )
    if special_mdx:
        return special_mdx

    semantic_candidates = _build_semantic_candidate_measures(user_prompt)

    candidate_measures = []
    for cm in semantic_candidates + list(measures):
        if not isinstance(cm, dict):
            continue
        name = (cm.get("name") or cm.get("column") or "").strip()
        if not name:
            continue
        if not any(_norm((x.get("name") or x.get("column") or "").strip()) == _norm(name) for x in candidate_measures):
            candidate_measures.append({"name": name})

    if not candidate_measures:
        candidate_measures = [
            {"name": "Line Total"},
            {"name": "Margin"},
            {"name": "Order Qty"},
            {"name": "Review Count"},
            {"name": "Rating"},
            {"name": "Unit Price"},
            {"name": "Standard Cost"},
            {"name": "Late Flag"},
            {"name": "Delay Days"},
            {"name": "Line Amount"},
        ]

    relative_conditions = _extract_relative_measure_conditions(
        user_prompt=user_prompt,
        measures=candidate_measures,
        fact_table=fact_table,
        schema=safe_schema,
    )

    if len(relative_conditions) >= 2:
        rows_expr = _pick_primary_rows_expr_for_filter(dims, safe_schema, user_prompt)
        selected_measures = []
        for measure_unique_name, _direction in relative_conditions:
            if measure_unique_name not in selected_measures:
                selected_measures.append(measure_unique_name)

        return _build_relative_filter_mdx(
            cube_name=cube_name,
            rows_expr=rows_expr,
            conditions=relative_conditions[:2],
            selected_measures=selected_measures[:2],
        )

    top_n = _find_top_n(user_prompt)
    year = _find_year_in_text(user_prompt)
    years = _find_all_years_in_text(user_prompt)
    wants_product_listing = _wants_product_listing(user_prompt)
    agg_intent = _detect_agg_intent(user_prompt)

    if year is not None and not _requested_year_available(safe_schema, year):
        raise ValueError(f"Avertissement : l'année {year} n'est pas disponible dans la base.")

    if len(years) >= 2 and not is_listing_only and not top_n:
        date_dim, year_attr = _resolve_date_dim_year(dims, safe_schema)
        if date_dim and year_attr:
            year_members = ", ".join(_member_key(date_dim, year_attr, y, safe_schema) for y in years[:2])
            rows_expr = f"{{ {year_members} }}"
            mdx = f"SELECT {columns} ON COLUMNS, {rows_expr} ON ROWS FROM {_br(cube_name)};"
            return " ".join(mdx.split())

    if safe_schema and top_n is None and _contains_any(user_prompt.lower(), ["tableau", "deux colonnes", "2 colonnes", "avec leur", "catégorie parente", "categorie parente"]):
        for d in dims:
            dim_name = (d.get("name") or d.get("dimension") or d.get("table") or "").strip()
            attrs = [a.strip() for a in (d.get("attributes") or d.get("levels") or []) if isinstance(a, str) and a.strip()]
            if len(attrs) >= 2:
                same_hierarchy = _same_hierarchy_levels(safe_schema, dim_name, attrs)
                if same_hierarchy:
                    dim_mdx_name, hierarchy_mdx_name, levels = same_hierarchy
                    where_clause = ""
                    if year is not None:
                        date_dim, year_attr = _resolve_date_dim_year(dims, safe_schema)
                        if date_dim and year_attr:
                            where_clause = f" WHERE ({_member_key(date_dim, year_attr, year, safe_schema)})"

                    return _build_same_hierarchy_table_mdx(
                        cube_name=cube_name,
                        dim_mdx_name=dim_mdx_name,
                        hierarchy_mdx_name=hierarchy_mdx_name,
                        levels=levels,
                        where_clause=where_clause
                    )

    if top_n is not None:
        ranking_measure = mset[0] if mset else _measure_unique_name("Line Total", "FactProductSales", safe_schema)

        rank_dim, rank_attr = _pick_ranking_dim_attr(dims, user_prompt)
        if not rank_dim or not rank_attr:
            rank_dim, rank_attr = _pick_ranking_dim_attr(
                [{"name": "DimProduct", "attributes": ["ProductName", "Category", "SubCategory"]}],
                user_prompt
            )

        rows_expr = f"TOPCOUNT({_members(rank_dim, rank_attr, safe_schema)}, {top_n}, {ranking_measure})"

        where_clause = ""
        if year is not None:
            date_dim, year_attr = _resolve_date_dim_year(dims, safe_schema)
            if date_dim and year_attr:
                where_clause = f" WHERE ({_member_key(date_dim, year_attr, year, safe_schema)})"

        top_columns = "{ " + ranking_measure + " }"
        mdx = f"SELECT {top_columns} ON COLUMNS, NON EMPTY {rows_expr} ON ROWS FROM {_br(cube_name)}{where_clause};"
        return " ".join(mdx.split())

    if wants_product_listing and not has_metric_intent:
        best_row_expr = _pick_best_single_row_expr(dims, safe_schema, user_prompt)
        if best_row_expr:
            year_member = None
            if year is not None:
                date_dim, year_attr = _resolve_date_dim_year(dims, safe_schema)
                if date_dim and year_attr:
                    year_member = _member_key(date_dim, year_attr, year, safe_schema)

            if listing_support_measure:
                filtered_rows = _build_listing_support_filter(best_row_expr, listing_support_measure, year_member)
                return f"SELECT {{}} ON COLUMNS, {filtered_rows} ON ROWS FROM {_br(cube_name)};"

            if year_member:
                return f"SELECT {{}} ON COLUMNS, {best_row_expr} ON ROWS FROM {_br(cube_name)} WHERE ({year_member});"

            return f"SELECT {{}} ON COLUMNS, {best_row_expr} ON ROWS FROM {_br(cube_name)};"

    rows_expr = _pick_best_single_row_expr(dims, safe_schema, user_prompt)
    if not rows_expr:
        rows_expr = _members("DimDate", "Year", safe_schema) if safe_schema else "[DimDate].[Year].Members"

    where_clause = ""
    if year is not None:
        same_hierarchy_rows = _same_date_hierarchy_filter_expr(safe_schema, rows_expr, year)
        if same_hierarchy_rows:
            rows_expr = same_hierarchy_rows
        else:
            date_dim, year_attr = _resolve_date_dim_year(dims, safe_schema)
            if date_dim and year_attr:
                year_members = _members(date_dim, year_attr, safe_schema)
                year_member = _member_key(date_dim, year_attr, year, safe_schema)

                if year_members == rows_expr:
                    rows_expr = year_member
                else:
                    where_clause = f" WHERE ({year_member})"

    if agg_intent and not is_listing_only and mset:
        primary_measure = _pick_primary_measure_name(mset)
        if primary_measure:
            explicit_agg_mdx = _build_explicit_agg_mdx(
                cube_name=cube_name,
                rows_expr=rows_expr,
                base_measure_name=primary_measure,
                agg_intent=agg_intent,
                where_clause=where_clause,
                schema=safe_schema
            )
            if explicit_agg_mdx:
                return explicit_agg_mdx

    if is_listing_only:
        if listing_support_measure:
            year_member = None
            if year is not None:
                date_dim, year_attr = _resolve_date_dim_year(dims, safe_schema)
                if date_dim and year_attr:
                    year_member = _member_key(date_dim, year_attr, year, safe_schema)
            filtered_rows = _build_listing_support_filter(rows_expr, listing_support_measure, year_member)
            mdx = f"SELECT {{}} ON COLUMNS, {filtered_rows} ON ROWS FROM {_br(cube_name)};"
        else:
            mdx = f"SELECT {{}} ON COLUMNS, {rows_expr} ON ROWS FROM {_br(cube_name)}{where_clause};"
    else:
        mdx = f"SELECT {columns} ON COLUMNS, NON EMPTY {rows_expr} ON ROWS FROM {_br(cube_name)}{where_clause};"

    return " ".join(mdx.split())
from __future__ import annotations

import json
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Dict, Any, List, Optional

from langchain_groq import ChatGroq
from langchain_core.messages import SystemMessage, HumanMessage

from config import settings
from metadata_extractor import get_db_metadata
from schema_builder import build_dw_schema_snapshot
from schema_store import load_schema_snapshot, save_schema_snapshot
from validator import validate_plan_against_schema


def get_dw_cfg(dw_id: str) -> Dict[str, Any] | None:
    return next((dw for dw in settings.DWS if dw.get("id") == dw_id), None)


def ensure_schema_snapshot(dw_id: str) -> Dict[str, Any]:
    """
    If snapshot missing, auto-extract + build + save.
    """
    snap = load_schema_snapshot(dw_id)
    if snap:
        return snap

    dw = get_dw_cfg(dw_id)
    if not dw:
        raise ValueError(f"Unknown DW '{dw_id}'")

    meta = get_db_metadata(database=dw["database"], schema=dw.get("schema", "dbo"))
    snapshot = build_dw_schema_snapshot(meta)
    save_schema_snapshot(dw_id, snapshot)
    return snapshot


def _norm_text(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    s = re.sub(r"[_\-]+", " ", s)
    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s)
    s = re.sub(r"\s+", " ", s)
    return s


def _tokenize(s: str) -> List[str]:
    s = _norm_text(s)
    return [t for t in re.split(r"[^a-z0-9]+", s) if t]


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, _norm_text(a), _norm_text(b)).ratio()


def _extract_schema_candidates(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extrait les expressions MDX candidates à partir du snapshot.
    Priorité aux niveaux issus de hiérarchies/natural_hierarchies.
    """
    candidates = []

    # 1) Attributs classiques dans dimensions
    dimensions = schema.get("dimensions", []) or []
    for dim in dimensions:
        dim_name = dim.get("name") or dim.get("dimension_name") or ""
        dim_unique = dim.get("unique_name") or f"[{dim_name}]"

        for attr in dim.get("attributes", []) or []:
            if isinstance(attr, dict):
                attr_name = attr.get("name") or attr.get("attribute_name") or ""
                attr_unique = attr.get("unique_name")
            else:
                attr_name = str(attr)
                attr_unique = None

            members_expr = f"{attr_unique}.Members" if attr_unique else f"{dim_unique}.[{attr_name}].Members"

            candidates.append({
                "dimension": dim_name,
                "hierarchy": None,
                "level": attr_name,
                "members_expr": members_expr,
                "kind": "attribute",
            })

        # hiérarchies classiques si elles existent
        for h in dim.get("hierarchies", []) or []:
            h_name = h.get("mdx_name") or h.get("name") or ""
            levels = h.get("levels", []) or []

            for lvl in levels:
                if isinstance(lvl, dict):
                    logical_level_name = lvl.get("name") or lvl.get("level_name") or ""
                    mdx_level_name = lvl.get("mdx_name") or logical_level_name
                else:
                    logical_level_name = str(lvl)
                    mdx_level_name = logical_level_name

                members_expr = f"{dim_unique}.[{h_name}].[{mdx_level_name}].Members"

                candidates.append({
                    "dimension": dim_name,
                    "hierarchy": h_name,
                    "level": logical_level_name,
                    "members_expr": members_expr,
                    "kind": "hierarchy_level",
                })

    # 2) natural_hierarchies
    natural_hierarchies = schema.get("natural_hierarchies", {}) or {}
    for dim_name, hier_list in natural_hierarchies.items():
        dim_unique = f"[{dim_name}]"

        for h in hier_list or []:
            h_name = h.get("mdx_name") or h.get("name") or ""
            levels = h.get("levels", []) or []

            for lvl in levels:
                if isinstance(lvl, dict):
                    logical_level_name = lvl.get("name") or ""
                    mdx_level_name = lvl.get("mdx_name") or logical_level_name
                else:
                    logical_level_name = str(lvl)
                    mdx_level_name = logical_level_name

                members_expr = f"{dim_unique}.[{h_name}].[{mdx_level_name}].Members"

                candidates.append({
                    "dimension": dim_name,
                    "hierarchy": h_name,
                    "level": logical_level_name,
                    "members_expr": members_expr,
                    "kind": "natural_hierarchy_level",
                })

    # déduplication
    uniq = {}
    for c in candidates:
        uniq[c["members_expr"]] = c

    return list(uniq.values())

def _guess_best_members_expr(user_prompt: str, schema: Dict[str, Any]) -> Optional[str]:
    candidates = _extract_schema_candidates(schema)
    if not candidates:
        return None

    prompt_norm = _norm_text(user_prompt)
    prompt_tokens = set(_tokenize(user_prompt))

    best = None
    best_score = -1.0

    for c in candidates:
        label = " ".join(
            [x for x in [c.get("dimension"), c.get("hierarchy"), c.get("level")] if x]
        )
        label_norm = _norm_text(label)
        label_tokens = set(_tokenize(label))

        score = 0.0
        score += len(prompt_tokens & label_tokens) * 10
        score += _similarity(prompt_norm, label_norm) * 6

        if c.get("kind") == "natural_hierarchy_level":
            score += 12
        elif c.get("kind") == "hierarchy_level":
            score += 10
        elif c.get("kind") == "hierarchy":
            score += 4
        elif c.get("kind") == "attribute":
            score += 1

        if score > best_score:
            best_score = score
            best = c

    return best["members_expr"] if best else None


def _extract_members_expressions(mdx: str) -> List[str]:
    if not mdx:
        return []
    found = re.findall(r"(?:\[[^\]]+\]\.){1,6}Members", mdx, flags=re.IGNORECASE)
    out = []
    seen = set()
    for f in found:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out


def _choose_best_replacement(expr: str, user_prompt: str, schema: Dict[str, Any]) -> Optional[str]:
    candidates = _extract_schema_candidates(schema)
    if not candidates:
        return None

    expr_norm = _norm_text(expr)
    prompt_norm = _norm_text(user_prompt)
    prompt_tokens = set(_tokenize(user_prompt))

    best = None
    best_score = -1.0

    for c in candidates:
        label = " ".join([x for x in [c.get("dimension"), c.get("hierarchy"), c.get("level")] if x])
        cand_expr = c["members_expr"]

        score = 0.0
        score += _similarity(expr_norm, cand_expr) * 8
        score += _similarity(prompt_norm, label) * 5
        score += len(prompt_tokens & set(_tokenize(label))) * 10

        if c.get("kind") == "natural_hierarchy_level":
            score += 12
        elif c.get("kind") == "hierarchy_level":
            score += 10
        elif c.get("kind") == "hierarchy":
            score += 4
        elif c.get("kind") == "attribute":
            score += 1

        if score > best_score:
            best_score = score
            best = c

    return best["members_expr"] if best else None

def _schema_dimension_aliases(schema: Dict[str, Any]) -> Dict[str, str]:
    aliases: Dict[str, str] = {}

    for d in schema.get("dimensions", []) or []:
        dim_name = (d.get("name") or d.get("dimension_name") or d.get("table") or "").strip()
        if not dim_name:
            continue
        aliases[_norm_text(dim_name)] = dim_name

        for a in d.get("attributes", []) or []:
            if isinstance(a, dict):
                for k in [a.get("name"), a.get("mdx_name"), a.get("attribute_name")]:
                    if k:
                        aliases[_norm_text(k)] = dim_name

    for dim_name, hierarchies in (schema.get("natural_hierarchies", {}) or {}).items():
        if dim_name:
            aliases[_norm_text(dim_name)] = dim_name

        for h in hierarchies or []:
            h_name = h.get("name")
            h_mdx = h.get("mdx_name")
            if h_name:
                aliases[_norm_text(h_name)] = dim_name
            if h_mdx:
                aliases[_norm_text(h_mdx)] = dim_name

    return aliases


def _schema_attribute_aliases(schema: Dict[str, Any], dim_name: str) -> Dict[str, str]:
    aliases: Dict[str, str] = {}
    dim_norm = _norm_text(dim_name)

    for d in schema.get("dimensions", []) or []:
        d_name = (d.get("name") or d.get("dimension_name") or d.get("table") or "").strip()
        if _norm_text(d_name) != dim_norm:
            continue

        for a in d.get("attributes", []) or []:
            if isinstance(a, dict):
                canonical = (a.get("name") or a.get("attribute_name") or "").strip()
                if not canonical:
                    continue
                aliases[_norm_text(canonical)] = canonical

                mdx_name = (a.get("mdx_name") or "").strip()
                if mdx_name:
                    aliases[_norm_text(mdx_name)] = canonical
            else:
                canonical = str(a).strip()
                if canonical:
                    aliases[_norm_text(canonical)] = canonical

    for natural_dim_name, hierarchies in (schema.get("natural_hierarchies", {}) or {}).items():
        if _norm_text(natural_dim_name) != dim_norm:
            continue

        for h in hierarchies or []:
            for lvl in h.get("levels", []) or []:
                if isinstance(lvl, dict):
                    canonical = (lvl.get("name") or "").strip()
                    mdx_name = (lvl.get("mdx_name") or "").strip()
                    if canonical:
                        aliases[_norm_text(canonical)] = canonical
                    if mdx_name and canonical:
                        aliases[_norm_text(mdx_name)] = canonical
                else:
                    canonical = str(lvl).strip()
                    if canonical:
                        aliases[_norm_text(canonical)] = canonical

    return aliases

def _extract_last_mdx_token(value: str) -> str:
    """
    Ex:
      [DimProduct].[Category] -> Category
      [DimProduct].[Product].[Sub Category] -> Sub Category
      [DimDate].[Calendar].[Year] -> Year
    """
    if not isinstance(value, str):
        return value

    matches = re.findall(r"\[([^\]]+)\]", value)
    if matches:
        return matches[-1].strip()

    return value.strip()


def _clean_plan_attr_value(attr: str) -> str:
    """
    Nettoie un attribut venant du LLM.
    Si c'est une expression MDX, on garde seulement le dernier niveau.
    """
    if not isinstance(attr, str):
        return attr

    attr = attr.strip()

    # Si le LLM renvoie un chemin MDX, on extrait le dernier token
    if "[" in attr and "]" in attr:
        attr = _extract_last_mdx_token(attr)

    return attr.strip()


def _looks_like_literal_value(value: str) -> bool:
    """
    Détecte une valeur littérale qui ne doit pas être considérée comme attribut.
    Exemples: 2013, 2022, Q1, January, Bikes
    Ici on traite surtout les années numériques.
    """
    if not isinstance(value, str):
        return False

    v = value.strip()

    # Année simple
    if re.fullmatch(r"20\d{2}", v):
        return True

    return False
def normalize_plan_to_schema(plan: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Corrige les noms du plan LLM pour les faire correspondre aux noms canoniques du snapshot.
    Supprime aussi les valeurs littérales des attributs (ex: 2013).
    """
    if not isinstance(plan, dict):
        return plan

    dim_aliases = _schema_dimension_aliases(schema)

    dims = plan.get("dimensions", []) or []
    normalized_dims = []

    for d in dims:
        if not isinstance(d, dict):
            continue

        raw_dim_name = (
            d.get("name")
            or d.get("dimension")
            or d.get("table")
            or ""
        ).strip()

        raw_dim_name = _clean_plan_attr_value(raw_dim_name)
        canonical_dim_name = dim_aliases.get(_norm_text(raw_dim_name), raw_dim_name)

        attr_aliases = _schema_attribute_aliases(schema, canonical_dim_name)

        raw_attrs = d.get("attributes") or d.get("levels") or []
        canonical_attrs = []

        for attr in raw_attrs:
            if not isinstance(attr, str):
                continue

            attr_clean = _clean_plan_attr_value(attr)

            # IMPORTANT: ignorer les valeurs littérales comme 2013
            if _looks_like_literal_value(attr_clean):
                continue

            canonical_attr = attr_aliases.get(_norm_text(attr_clean), attr_clean)

            if canonical_attr not in canonical_attrs:
                canonical_attrs.append(canonical_attr)

        normalized_dims.append({
            "table": canonical_dim_name,
            "attributes": canonical_attrs
        })

    plan["dimensions"] = normalized_dims
    return plan

def auto_fix_mdx_generic(mdx: str, user_prompt: str, schema: Dict[str, Any]) -> str:
    """
    Correction générique basée sur le snapshot.
    Préférence forte pour les niveaux de hiérarchie.
    """
    if not mdx or not isinstance(mdx, str):
        return mdx

    fixed = " ".join(mdx.split())
    found_exprs = _extract_members_expressions(fixed)
    guessed = _guess_best_members_expr(user_prompt, schema)

    # 1) remplace chaque expression .Members trouvée par une meilleure candidate du snapshot
    for expr in found_exprs:
        replacement = _choose_best_replacement(expr, user_prompt, schema)
        if replacement and replacement != expr:
            fixed = re.sub(re.escape(expr), replacement, fixed, flags=re.IGNORECASE)

    # 2) force ON ROWS à utiliser une vraie expression hiérarchique si possible
    if guessed:
        fixed = re.sub(
            r"NON\s+EMPTY\s+(.+?)\s+ON\s+ROWS",
            f"NON EMPTY {guessed} ON ROWS",
            fixed,
            flags=re.IGNORECASE,
        )

        # 3) force TOPCOUNT à utiliser une expression valide issue du snapshot
        fixed = re.sub(
            r"TOPCOUNT\s*\(\s*(.+?)\s*,",
            f"TOPCOUNT({guessed},",
            fixed,
            flags=re.IGNORECASE,
        )

    return " ".join(fixed.split())


def build_runtime_hints(schema: Dict[str, Any], user_prompt: str) -> str:
    candidates = _extract_schema_candidates(schema)
    if not candidates:
        return ""

    best = _guess_best_members_expr(user_prompt, schema)

    lines = [
        "RUNTIME GUIDANCE:",
        "- Use ONLY hierarchy/attribute expressions that exist in the schema snapshot.",
        "- When using MEMBERS, prefer a valid hierarchy level expression if available.",
        "- If an attribute belongs to a hierarchy, ALWAYS use the hierarchy level expression instead of the standalone attribute expression.",
        "- Never invent hierarchy names.",
        "- For row grouping, prefer the closest hierarchy level matching the user prompt.",
    ]

    if best:
        lines.append(f"- Best matching MEMBERS expression for this prompt: {best}")

    lines.append("- Available candidate MEMBERS expressions:")
    for c in candidates[:30]:
        lines.append(f"  * {c['members_expr']}")

    return "\n".join(lines)

def _strip_code_fences(text: str) -> str:
    if not isinstance(text, str):
        return text
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _safe_parse_llm_json(raw: str) -> Dict[str, Any]:
    """
    Parse robuste du JSON renvoyé par le LLM.
    """
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("Empty LLM response")

    cleaned = _strip_code_fences(raw)

    # corrige quelques erreurs fréquentes du LLM dans les chaînes JSON
    cleaned = cleaned.replace("\\*", "*")

    # 1) essai direct
    try:
        return json.loads(cleaned)
    except Exception:
        pass

    # 2) extraire le plus grand bloc {...}
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        candidate = cleaned[start:end + 1]

        # recorrection après extraction
        candidate = candidate.replace("\\*", "*")

        try:
            return json.loads(candidate)
        except Exception:
            cleaned = candidate

    # 3) petits nettoyages fréquents
    cleaned2 = cleaned
    cleaned2 = re.sub(r",\s*([}\]])", r"\1", cleaned2)  # trailing commas
    cleaned2 = cleaned2.replace("\u201c", "\"").replace("\u201d", "\"")
    cleaned2 = cleaned2.replace("\u2018", "'").replace("\u2019", "'")
    cleaned2 = cleaned2.replace("\\*", "*")

    try:
        return json.loads(cleaned2)
    except Exception as e:
        raise ValueError(f"LLM did not return valid JSON. Raw output:\n{raw}") from e
def _build_schema_for_llm(schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Réduit fortement la taille du schéma envoyé au LLM.
    On garde seulement ce qui est utile pour comprendre :
    - dimensions
    - faits / mesures
    - hiérarchies naturelles
    On exclut 'tables' car trop volumineux.
    """
    compact_dimensions = []
    for d in schema.get("dimensions", []) or []:
        compact_dimensions.append({
            "name": d.get("name"),
            "attributes": [
                a.get("name") if isinstance(a, dict) else str(a)
                for a in (d.get("attributes", []) or [])
            ],
        })

    compact_facts = []
    for f in schema.get("facts", []) or []:
        compact_facts.append({
            "name": f.get("name"),
            "measures": [
                {
                    "name": m.get("name"),
                    "column": m.get("column"),
                    "agg": m.get("agg"),
                }
                for m in (f.get("measures", []) or [])
            ],
        })

    compact_hierarchies = {}
    for dim_name, hier_list in (schema.get("natural_hierarchies", {}) or {}).items():
        compact_hierarchies[dim_name] = []
        for h in hier_list or []:
            compact_hierarchies[dim_name].append({
                "name": h.get("name"),
                "levels": [
                    lvl.get("name") if isinstance(lvl, dict) else str(lvl)
                    for lvl in (h.get("levels", []) or [])
                ]
            })

    return {
        "dimensions": compact_dimensions,
        "facts": compact_facts,
        "natural_hierarchies": compact_hierarchies,
    }


def _build_schema_for_llm(schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Réduit fortement la taille du schéma envoyé au LLM.
    On garde seulement ce qui est utile pour comprendre :
    - dimensions
    - faits / mesures
    - hiérarchies naturelles
    On exclut 'tables' car trop volumineux.
    """
    compact_dimensions = []
    for d in schema.get("dimensions", []) or []:
        compact_dimensions.append({
            "name": d.get("name"),
            "attributes": [
                a.get("name") if isinstance(a, dict) else str(a)
                for a in (d.get("attributes", []) or [])
            ],
        })

    compact_facts = []
    for f in schema.get("facts", []) or []:
        compact_facts.append({
            "name": f.get("name"),
            "measures": [
                {
                    "name": m.get("name"),
                    "column": m.get("column"),
                    "agg": m.get("agg"),
                }
                for m in (f.get("measures", []) or [])
            ],
        })

    compact_hierarchies = {}
    for dim_name, hier_list in (schema.get("natural_hierarchies", {}) or {}).items():
        compact_hierarchies[dim_name] = []
        for h in hier_list or []:
            compact_hierarchies[dim_name].append({
                "name": h.get("name"),
                "levels": [
                    lvl.get("name") if isinstance(lvl, dict) else str(lvl)
                    for lvl in (h.get("levels", []) or [])
                ]
            })

    return {
        "dimensions": compact_dimensions,
        "facts": compact_facts,
        "natural_hierarchies": compact_hierarchies,
    }


def ask_bi_agent(dw_id: str, user_prompt: str) -> Dict[str, Any]:
    dw = get_dw_cfg(dw_id)
    if not dw:
        return {"status": "error", "message": f"Unknown DW '{dw_id}'"}

    if not settings.GROQ_API_KEY:
        return {"status": "error", "message": "Missing GROQ_API_KEY in backend/.env"}

    schema = ensure_schema_snapshot(dw_id)

    # IMPORTANT : on réduit ce qu'on envoie au LLM
    schema_for_llm = _build_schema_for_llm(schema)

    runtime_hints = build_runtime_hints(schema_for_llm, user_prompt)

    llm = ChatGroq(
        temperature=0,
        groq_api_key=settings.GROQ_API_KEY,
        model_name=settings.GROQ_MODEL,
    )

    system_prompt = f"""
You are a BI architect. Use ONLY the following DW schema snapshot (JSON) to answer.
Your job:
1) Identify the analytic intent.
2) Choose fact table + dimensions + measures from the schema.
3) Output a PLAN JSON ONLY (no markdown, no text).

IMPORTANT RULES:
- Do NOT invent table names or columns.
- Use ONLY what exists in the schema snapshot.
- If the user asks for something impossible, set status="error" and explain.
- If the user explicitly mentions a business metric such as Line Total, revenue, margin, sales, cost, price, rating or count, return a non-empty "measures" list.
- If the user asks for products in a given year, keep "Year" as a filter and use the product dimension on rows.
- Return mdx as a single line string (no \\n).
- When using MEMBERS, prefer a valid hierarchy level expression from the schema snapshot.
- Do not use an attribute expression if a better matching hierarchy level exists in the snapshot.
- Do not use markdown code fences.
- Do not write explanations before or after the JSON.
- In the "mdx" field, do not escape the * character.
- Return valid JSON strings only.
- Never use technical keys such as SalesKey, ReviewKey, ProductKey, DateKey, CostHistKey, PriceHistKey as measures.
- If the user asks for a listing (for example products, categories, months, years), return an empty "measures" list.
- If the user asks for one business metric, return only that metric, not all available measures.
- Use only business measures when relevant.

OUTPUT JSON FORMAT:
{{
  "status":"success",
  "dw_id":"{dw_id}",
  "fact_table":"...",
  "measures":[{{"name":"...","column":"...","agg":"Sum|Avg|Count"}}],
  "dimensions":[{{"table":"...","attributes":["...","..."]}}],
  "mdx":"SELECT ... FROM [YourCubeName]"
}}

{runtime_hints}

SCHEMA SNAPSHOT:
{json.dumps(schema_for_llm, ensure_ascii=False)}
""".strip()

    raw = llm.invoke([
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt.strip())
    ]).content.strip()

    try:
        plan = _safe_parse_llm_json(raw)
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "raw": raw
        }

    # IMPORTANT : on normalise toujours avec le schéma COMPLET
    plan = normalize_plan_to_schema(plan, schema)

    if isinstance(plan.get("mdx"), str):
        plan["mdx"] = " ".join(plan["mdx"].split())
        plan["mdx"] = auto_fix_mdx_generic(plan["mdx"], user_prompt, schema)

    errors = validate_plan_against_schema(plan, schema)
    if errors:
        return {
            "status": "error",
            "message": "Plan invalid against DW schema snapshot: " + " | ".join(errors),
            "errors": errors,
            "plan": plan,
            "schema_dimensions": [d.get("name") for d in schema.get("dimensions", [])],
            "natural_hierarchies": schema.get("natural_hierarchies", {})
        }

    plan.setdefault("dw_id", dw_id)
    plan.setdefault("status", "success")
    return plan
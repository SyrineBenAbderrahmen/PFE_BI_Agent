from __future__ import annotations

import re
from typing import List, Optional

from models import IntentType, UserIntent


CREATE_CUBE_PATTERNS: List[str] = [
    r"\bcr[eé]e?r?\s+un\s+cube\b",
    r"\bg[eé]n[eé]rer?\s+un\s+cube\b",
    r"\bcreate\s+a\s+cube\b",
    r"\bbuild\s+a\s+cube\b",
]

ADD_MEASURE_PATTERNS: List[str] = [
    r"\bajoute?r?\s+une?\s+mesure\b",
    r"\bajoute?r?\s+la\s+mesure\b",
    r"\bajoute?r?\s+mesure\b",
    r"\bajouter\s+une?\s+mesure\b",
    r"\bajouter\s+la\s+mesure\b",
    r"\bajouter\s+mesure\b",
    r"\bcr[eé]e?r?\s+une?\s+mesure\b",
    r"\bcr[eé]e?r?\s+la\s+mesure\b",
    r"\badd\s+a\s+measure\b",
    r"\badd\s+measure\b",
    r"\bajoute?r?\s+un\s+kpi\b",
    r"\bajouter\s+un\s+kpi\b",
]

MODIFY_DIMENSION_PATTERNS: List[str] = [
    r"\bmodifie?r?\s+une?\s+dimension\b",
    r"\bmodifie?r?\s+la\s+dimension\b",
    r"\bmodifier\s+une?\s+dimension\b",
    r"\bmodifier\s+la\s+dimension\b",
    r"\bmodifier\s+dimension\b",
    r"\bmodifie\s+dimension\b",
    r"\bchange\s+dimension\b",
    r"\badd\s+hierarchy\b",
    r"\bajoute?r?\s+une?\s+hi[eé]rarchie\b",
    r"\bajouter\s+une?\s+hi[eé]rarchie\b",
]

PREVIEW_PATTERNS: List[str] = [
    r"\bpreview\b",
    r"\bpr[eé]visuali[sz]e?r?\b",
    r"\bvoir\s+le\s+cube\b",
]

DEPLOY_PATTERNS: List[str] = [
    r"\bd[eé]ployer?\b",
    r"\bdeploy\b",
    r"\bpublier\b",
]


def _normalize(text: str) -> str:
    return "".join(ch.lower() for ch in (text or "") if ch.isalnum())


def _extract_requested_tokens(prompt: str) -> dict:
    lowered = prompt.lower()
    measures = []
    dimensions = []

    simple_measure_words = [
        "sales", "amount", "quantity", "count", "cost", "profit",
        "montant", "quantité", "quantite", "nombre", "coût", "cout",
        "risk", "delay", "scrap", "inventory", "movement", "price",
        "lineamount", "actualcost", "receivedqty", "rejectedqty",
        "unitprice", "orderqty", "stockedqty", "delaydays"
    ]

    simple_dimension_words = [
        "date", "time", "product", "vendor", "customer", "location",
        "produit", "fournisseur", "client", "emplacement",
        "purchaseorder", "purchase order", "workorder", "work order",
        "dimdate", "dimproduct", "dimvendor", "dimpurchaseorder", "dimworkorder"
    ]

    for word in simple_measure_words:
        if word in lowered:
            measures.append(word)

    for word in simple_dimension_words:
        if word in lowered:
            dimensions.append(word)

    # extraction simple après "mesure ..."
    m = re.search(
        r"(?:mesure|measure)\s+([A-Za-z_À-ÿ][A-Za-z0-9_À-ÿ]*)",
        prompt,
        flags=re.IGNORECASE,
    )
    if m:
        token = m.group(1).strip()
        token_n = token.lower()
        if token_n not in {
            "dans", "du", "de", "la", "le", "une", "un", "cube", "sujet", "with", "for"
        }:
            measures.append(token)

    # extraction simple après "dimension ..."
    d = re.search(
        r"(?:dimension)\s+([A-Za-z_À-ÿ][A-Za-z0-9_À-ÿ]*)",
        prompt,
        flags=re.IGNORECASE,
    )
    if d:
        token = d.group(1).strip()
        token_n = token.lower()
        if token_n not in {
            "dans", "du", "de", "la", "le", "une", "un", "cube", "sujet"
        }:
            dimensions.append(token)

    return {
        "measures": list(dict.fromkeys(measures)),
        "dimensions": list(dict.fromkeys(dimensions)),
    }


def _extract_cube_name(prompt: str) -> Optional[str]:
    patterns = [
        r"\bqui\s+s['’]appelle\s+([A-Za-z0-9_]+)",
        r"\bnomm[ée]e?\s+([A-Za-z0-9_]+)",
        r"\bnomme\s+([A-Za-z0-9_]+)",
        r"\bnamed\s+([A-Za-z0-9_]+)",
        r"\bappel[ée]?\s+([A-Za-z0-9_]+)",
        r"\bcube\s+([A-Za-z0-9_]+)",
    ]

    for pattern in patterns:
        m = re.search(pattern, prompt, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()

    return None
    


def _extract_count(prompt: str, keywords: list[str]) -> Optional[int]:
    escaped = "|".join(re.escape(k) for k in keywords)
    patterns = [
        rf"\b(\d+)\s+(?:table[s]?\s+de\s+)?(?:{escaped})\b",
        rf"\b(?:exactement|avec|only|just)\s+(\d+)\s+(?:table[s]?\s+de\s+)?(?:{escaped})\b",
    ]

    for pattern in patterns:
        m = re.search(pattern, prompt, flags=re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None

    return None


def parse_intent(prompt: str) -> UserIntent:
    prompt_clean = prompt.strip()
    lowered = prompt_clean.lower()

    extracted = _extract_requested_tokens(prompt_clean)
    cube_name = _extract_cube_name(prompt_clean)

    max_facts = _extract_count(prompt_clean, ["fait", "faits", "fact", "facts"])
    max_dimensions = _extract_count(prompt_clean, ["dimension", "dimensions"])

    for pattern in CREATE_CUBE_PATTERNS:
        if re.search(pattern, lowered):
            return UserIntent(
                intent=IntentType.CREATE_CUBE,
                cube_name=cube_name,
                requested_measures=extracted["measures"],
                requested_dimensions=extracted["dimensions"],
                description_hint=prompt_clean,
                max_facts=max_facts,
                max_dimensions=max_dimensions,
                confidence=0.92,
            )

    for pattern in ADD_MEASURE_PATTERNS:
        if re.search(pattern, lowered):
            return UserIntent(
                intent=IntentType.ADD_MEASURE,
                cube_name=cube_name,
                requested_measures=extracted["measures"],
                requested_dimensions=extracted["dimensions"],
                extra_instructions=prompt_clean,
                max_facts=max_facts,
                max_dimensions=max_dimensions,
                confidence=0.88,
            )

    for pattern in MODIFY_DIMENSION_PATTERNS:
        if re.search(pattern, lowered):
            return UserIntent(
                intent=IntentType.MODIFY_DIMENSION,
                cube_name=cube_name,
                requested_measures=extracted["measures"],
                requested_dimensions=extracted["dimensions"],
                extra_instructions=prompt_clean,
                max_facts=max_facts,
                max_dimensions=max_dimensions,
                confidence=0.87,
            )

    for pattern in PREVIEW_PATTERNS:
        if re.search(pattern, lowered):
            return UserIntent(
                intent=IntentType.PREVIEW_CUBE,
                cube_name=cube_name,
                extra_instructions=prompt_clean,
                max_facts=max_facts,
                max_dimensions=max_dimensions,
                confidence=0.85,
            )

    for pattern in DEPLOY_PATTERNS:
        if re.search(pattern, lowered):
            return UserIntent(
                intent=IntentType.DEPLOY_CUBE,
                cube_name=cube_name,
                extra_instructions=prompt_clean,
                max_facts=max_facts,
                max_dimensions=max_dimensions,
                confidence=0.85,
            )

    # Fallback plus souple pour les formulations naturelles
    if "mesure" in lowered and any(v in lowered for v in ["ajout", "ajoute", "ajouter", "add", "créer", "creer"]):
        return UserIntent(
            intent=IntentType.ADD_MEASURE,
            cube_name=cube_name,
            requested_measures=extracted["measures"],
            requested_dimensions=extracted["dimensions"],
            extra_instructions=prompt_clean,
            max_facts=max_facts,
            max_dimensions=max_dimensions,
            confidence=0.80,
        )

    if "dimension" in lowered and any(v in lowered for v in ["modif", "modifier", "modifie", "change", "ajoute", "ajouter"]):
        return UserIntent(
            intent=IntentType.MODIFY_DIMENSION,
            cube_name=cube_name,
            requested_measures=extracted["measures"],
            requested_dimensions=extracted["dimensions"],
            extra_instructions=prompt_clean,
            max_facts=max_facts,
            max_dimensions=max_dimensions,
            confidence=0.80,
        )

    return UserIntent(
        intent=IntentType.UNKNOWN,
        cube_name=cube_name,
        extra_instructions=prompt_clean,
        requested_measures=extracted["measures"],
        requested_dimensions=extracted["dimensions"],
        max_facts=max_facts,
        max_dimensions=max_dimensions,
        confidence=0.30,
    )
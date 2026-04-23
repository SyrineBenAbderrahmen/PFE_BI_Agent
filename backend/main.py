# main.py
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from config import settings
from metadata_extractor import get_db_metadata
from schema_store import save_schema_snapshot, load_schema_snapshot
from schema_builder import build_dw_schema_snapshot
from history_store import save_prompt_history, get_prompt_history

from bi_agent import ask_bi_agent
from mdx_builder import build_mdx, analyze_prompt_guidance 

from cube_mutations import add_measure_to_cube, modify_dimension_in_cube
from intent_parser import parse_intent
from models import (
    CubeActionResponse,
    IntentType,
    PromptRequest,
    CubeModel,
    SchemaSnapshot,
)
from validator import validate_plan_against_schema
from cube_designer import create_cube_model, cube_model_from_registry
from cube_validator import validate_cube_model
from cube_store import save_cube_record, load_cube_record, cube_exists
from xmla_generator import generate_xmla
from xmla_updates import (
    generate_xmla_alter_add_measure,
    generate_xmla_alter_modify_dimension,
)

app = FastAPI(title="BI OLAP Agent API (Groq Llama)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class DeployRequest(BaseModel):
    dw: str
    xmla: str


def load_semantic_schema_fallback(dw_id: str):
    path = Path(settings.SNAPSHOT_DIR) / f"{dw_id}_semantic_latest.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/dws")
def list_dws():
    return settings.DWS


def get_dw_cfg(dw_id: str) -> Optional[Dict[str, Any]]:
    return next((dw for dw in settings.DWS if dw.get("id") == dw_id), None)


def ensure_schema_snapshot(dw_id: str):
    snap = load_schema_snapshot(dw_id)
    if snap:
        snap_dict = snap.model_dump() if hasattr(snap, "model_dump") else snap
        print(f"[DEBUG] Loaded existing snapshot for {dw_id}: keys={list(snap_dict.keys())}")
        print(f"[DEBUG] Existing snapshot tables count={len(snap_dict.get('tables', []))}")
        print(f"[DEBUG] Existing snapshot dimensions count={len(snap_dict.get('dimensions', []))}")
        print(f"[DEBUG] Existing snapshot facts count={len(snap_dict.get('facts', []))}")
        return snap

    dw_cfg = get_dw_cfg(dw_id)
    if not dw_cfg:
        return None

    meta = get_db_metadata(
        database=dw_cfg["database"],
        schema=dw_cfg.get("schema", "dbo"),
        ssas_database=dw_cfg.get("ssas_database"),
        cube_name=dw_cfg.get("cube_name"),
    )

    print(f"[DEBUG] Built snapshot for {dw_id}: keys={list(meta.keys())}")
    print(f"[DEBUG] Built snapshot tables count={len(meta.get('tables', []))}")

    save_schema_snapshot(dw_id, meta)
    return meta


def _schema_to_dict(schema: Any) -> Dict[str, Any]:
    return schema.model_dump() if hasattr(schema, "model_dump") else schema


def _ensure_cube_in_mdx(mdx: str, cube_name: str) -> str:
    if not mdx:
        return mdx

    cube_bracket = f"[{cube_name}]"

    if cube_bracket in mdx:
        return mdx

    mdx = mdx.replace("[YourCubeName]", cube_bracket).replace(
        "FROM YourCubeName",
        f"FROM {cube_bracket}"
    )

    mdx = re.sub(r"FROM\s+\[[^\]]+\]", f"FROM {cube_bracket}", mdx, flags=re.IGNORECASE)
    return mdx


def _format_guidance_plain_text(guidance: Dict[str, Any]) -> str:
    guided_questions = guidance.get("guided_questions") or []
    suggested_measures = guidance.get("suggested_measures") or []
    suggested_dimensions = guidance.get("suggested_dimensions") or []

    parts = [
        "Prompt vague ou incomplet.",
        "",
        guidance.get("help_message", ""),
    ]

    if guided_questions:
        parts.append("")
        parts.append("Questions guidées :")
        parts.extend([f"- {q}" for q in guided_questions])

    if suggested_measures:
        parts.append("")
        parts.append("Mesures suggérées :")
        parts.extend([f"- {m}" for m in suggested_measures])

    if suggested_dimensions:
        parts.append("")
        parts.append("Dimensions suggérées :")
        parts.extend([f"- {d}" for d in suggested_dimensions])

    return "\n".join(parts).strip()


def persist_history_cube(req: PromptRequest, response: CubeActionResponse) -> None:
    try:
        save_prompt_history(
            database=response.preview.get("cube_name") if response.preview else None,
            dw_id=req.dw,
            user_prompt=req.prompt,
            agent_response=response.message,
            generated_mdx=None,
            preview=response.preview if response.preview else None,
            status=response.status,
            intent=response.intent.value if hasattr(response.intent, "value") else str(response.intent),
            xmla_script=response.xmla_script,
        )
    except Exception as e:
        print("SAVE HISTORY ERROR (cube case):", str(e))


@app.post("/agent/prompt")
def agent_prompt(req: PromptRequest):
    dw_cfg = get_dw_cfg(req.dw)
    if not dw_cfg:
        return {"status": "error", "message": f"Unknown DW '{req.dw}'"}

    schema = ensure_schema_snapshot(req.dw)
    if not schema:
        return {
            "status": "error",
            "message": f"Unable to load schema snapshot for DW '{req.dw}'"
        }

    schema_for_use = _schema_to_dict(schema)

    guidance = analyze_prompt_guidance(
        dw_id=req.dw,
        prompt=req.prompt,
        schema=schema_for_use,
    )

    if guidance.get("is_vague"):
        response_data = {
            "status": "needs_clarification",
            "message": guidance.get("help_message"),
            "guidance": guidance,
            "json_structure": None,
            "suggested_mdx": None,
            "mdx": None,
            "cube_name_used": dw_cfg["cube_name"],
            "ssas_database_used": dw_cfg.get("ssas_database", "")
        }

        try:
            save_prompt_history(
                database=dw_cfg["ssas_database"],
                dw_id=req.dw,
                user_prompt=req.prompt,
                agent_response=guidance.get("help_message", "Prompt vague"),
                generated_mdx=None,
                preview=response_data,
                status="needs_clarification",
                intent="guidance",
                xmla_script=None,
            )
        except Exception as e:
            print("SAVE HISTORY ERROR (clarification case):", str(e))

        return response_data

    plan = ask_bi_agent(req.dw, req.prompt )

    if plan.get("status") == "error":
        try:
            save_prompt_history(
                database=dw_cfg["ssas_database"],
                dw_id=req.dw,
                user_prompt=req.prompt,
                agent_response=plan.get("message", "error"),
                generated_mdx=None,
                preview=plan,
                status="error",
                intent="query",
                xmla_script=None,
            )
        except Exception as e:
            print("SAVE HISTORY ERROR (error case):", str(e))

        return {
            "status": "error",
            "message": plan.get("message", "Unknown agent error"),
            "guidance": guidance
        }

    # IMPORTANT : validation avec user_prompt
    errors = validate_plan_against_schema(plan, schema_for_use, req.prompt)
    if errors:
        return {
            "status": "error",
            "workspace_mode": "query",
            "dw_id": req.dw,
            "message": f"Plan invalid against DW schema snapshot: {errors[0]}",
            "generation": {
                "mdx_generated": False,
                "xmla_generated": False
            }
        }

    cube_name = dw_cfg["cube_name"]
    mdx = build_mdx(
        plan,
        cube_name=cube_name,
        user_prompt=req.prompt,
        schema=schema_for_use
    )
    mdx = _ensure_cube_in_mdx(mdx, cube_name)

    response_data = {
        "status": "success",
        "message": "Requête générée avec succès.",
        "guidance": guidance,
        "json_structure": plan,
        "suggested_mdx": mdx,
        "mdx": mdx,
        "cube_name_used": cube_name,
        "ssas_database_used": dw_cfg.get("ssas_database", "")
    }

    try:
        save_prompt_history(
            database=dw_cfg["ssas_database"],
            dw_id=req.dw,
            user_prompt=req.prompt,
            agent_response="Requête générée avec succès.",
            generated_mdx=mdx,
            preview=response_data,
            status="success",
            intent="query",
            xmla_script=None,
        )
    except Exception as e:
        print("SAVE HISTORY ERROR (success case):", str(e))

    return response_data


@app.post("/agent/mdx-raw", response_class=PlainTextResponse)
def mdx_raw(req: PromptRequest):
    dw_cfg = get_dw_cfg(req.dw)
    if not dw_cfg:
        return f"Unknown DW '{req.dw}'"

    schema = ensure_schema_snapshot(req.dw)
    if not schema:
        return f"Unable to load schema snapshot for DW '{req.dw}'"

    schema_for_use = _schema_to_dict(schema)

    guidance = analyze_prompt_guidance(
        dw_id=req.dw,
        prompt=req.prompt,
        schema=schema_for_use
    )

    if guidance.get("is_vague"):
        return _format_guidance_plain_text(guidance)

    plan = ask_bi_agent(req.dw, req.prompt)
    if plan.get("status") == "error":
        return str(plan)

    # IMPORTANT : validation avec user_prompt
    errors = validate_plan_against_schema(plan, schema_for_use, req.prompt)
    if errors:
        return f"Plan invalid against DW schema snapshot: {errors[0]}"

    cube_name = dw_cfg["cube_name"]
    mdx = build_mdx(
        plan,
        cube_name=cube_name,
        user_prompt=req.prompt,
        schema=schema_for_use
    )
    mdx = _ensure_cube_in_mdx(mdx, cube_name)

    return mdx.strip()


@app.post("/agent/xmla-raw", response_class=PlainTextResponse)
def agent_xmla_raw(req: PromptRequest):
    dw_cfg = get_dw_cfg(req.dw)
    if not dw_cfg:
        return f"Unknown DW '{req.dw}'"

    schema = ensure_schema_snapshot(req.dw)
    if not schema:
        return f"Unable to load schema snapshot for DW '{req.dw}'"

    schema_for_use = _schema_to_dict(schema)

    guidance = analyze_prompt_guidance(
        dw_id=req.dw,
        prompt=req.prompt,
        schema=schema_for_use
    )

    if guidance.get("is_vague"):
        return _format_guidance_plain_text(guidance)

    plan = ask_bi_agent(req.dw, req.prompt)
    if plan.get("status") == "error":
        return str(plan)

    mdx = build_mdx(
        plan,
        cube_name=dw_cfg["cube_name"],
        user_prompt=req.prompt,
        schema=schema_for_use
    )
    mdx = _ensure_cube_in_mdx(mdx, dw_cfg["cube_name"])
    plan["mdx"] = mdx

    return generate_xmla(
        cube_model=plan,
        schema_snapshot=schema_for_use,
        cfg=dw_cfg
    )


@app.get("/dw/{dw_id}/extract-schema")
def extract_schema(dw_id: str):
    dw_cfg = get_dw_cfg(dw_id)
    if not dw_cfg:
        return {"status": "error", "message": f"Unknown DW '{dw_id}'"}

    meta = get_db_metadata(
        database=dw_cfg["database"],
        schema=dw_cfg.get("schema", "dbo"),
        ssas_database=dw_cfg.get("ssas_database"),
        cube_name=dw_cfg.get("cube_name"),
    )

    print(f"[DEBUG] extract-schema meta preview={str(meta)[:1000]}")
    save_schema_snapshot(dw_id, meta)

    print(f"[DEBUG] extract-schema snapshot keys={list(meta.keys())}")
    print(f"[DEBUG] extract-schema tables count={len(meta.get('tables', []))}")

    return {"status": "success", "schema": meta}


@app.get("/dw/{dw_id}/schema")
def get_schema(dw_id: str):
    snap = load_schema_snapshot(dw_id)

    if not snap:
        dw_cfg = get_dw_cfg(dw_id)
        if not dw_cfg:
            return {"status": "error", "message": f"Unknown DW '{dw_id}'"}

        meta = get_db_metadata(
            database=dw_cfg["database"],
            schema=dw_cfg.get("schema", "dbo"),
            ssas_database=dw_cfg.get("ssas_database"),
            cube_name=dw_cfg.get("cube_name"),
        )
        snap = build_dw_schema_snapshot(meta)
        save_schema_snapshot(dw_id, snap)

    return {"status": "success", "schema": snap}


@app.get("/test-history")
def test_history():
    dw_cfg = get_dw_cfg("dw_sujet1")
    if not dw_cfg:
        return {"status": "error", "message": "DW de test introuvable"}

    save_prompt_history(
        database=dw_cfg["ssas_database"],
        dw_id="dw_sujet1",
        user_prompt="test prompt",
        agent_response="test response",
        generated_mdx="SELECT {} ON COLUMNS FROM [TestCube]"
    )

    return {"status": "ok", "message": "test inserted"}


@app.post("/cube/action", response_model=CubeActionResponse)
def cube_action(req: PromptRequest) -> CubeActionResponse:
    dw_cfg = get_dw_cfg(req.dw)
    if not dw_cfg:
        response = CubeActionResponse(
            status="error",
            intent=IntentType.UNKNOWN,
            message=f"Unknown DW '{req.dw}'",
        )
        persist_history_cube(req, response)
        return response

    schema = ensure_schema_snapshot(req.dw)
    if not schema:
        response = CubeActionResponse(
            status="error",
            intent=IntentType.UNKNOWN,
            message=f"Unable to load schema snapshot for DW '{req.dw}'",
        )
        persist_history_cube(req, response)
        return response

    schema_dict = _schema_to_dict(schema)

    print(f"[DEBUG] cube_action schema keys = {list(schema_dict.keys())}")
    print(f"[DEBUG] cube_action tables count = {len(schema_dict.get('tables', []))}")

    if not schema_dict.get("tables"):
        semantic_schema = load_semantic_schema_fallback(req.dw)
        if semantic_schema:
            print(f"[DEBUG] semantic fallback loaded for {req.dw}")
            schema = semantic_schema
            schema_dict = semantic_schema
        else:
            print(f"[DEBUG] no semantic fallback found for {req.dw}")

    intent = parse_intent(req.prompt)

    if intent.intent == IntentType.UNKNOWN:
        response = CubeActionResponse(
            status="needs_clarification",
            intent=intent.intent,
            message="Intention non reconnue. Reformule avec une action explicite comme créer cube, ajouter mesure ou modifier dimension.",
        )
        persist_history_cube(req, response)
        return response

    ssas_database = dw_cfg.get("ssas_database")
    cube_name = intent.cube_name if getattr(intent, "cube_name", None) else None

    # CREATE_CUBE
    if intent.intent == IntentType.CREATE_CUBE:
        cube_model = create_cube_model(schema, intent)

        if cube_exists(req.dw, cube_model.cube_name):
            response = CubeActionResponse(
                status="error",
                intent=intent.intent,
                message=f"Le cube '{cube_model.cube_name}' existe déjà. Veuillez choisir un autre nom.",
                preview={"cube_name": cube_model.cube_name},
            )
            persist_history_cube(req, response)
            return response

        validation = validate_cube_model(cube_model)

        response = CubeActionResponse(
            status="success" if validation.is_valid else "invalid",
            intent=intent.intent,
            cube_model=cube_model,
            validation=validation,
            preview={
                "cube_name": cube_model.cube_name,
                "description": cube_model.description,
                "facts": [f.name for f in cube_model.facts],
                "dimensions": [d.name for d in cube_model.dimensions],
            },
            message=(
                "Structure de cube générée et validée."
                if validation.is_valid
                else "Structure de cube générée, mais validation échouée. XMLA non généré."
            ),
        )

        if validation.is_valid:
            xmla_script = generate_xmla(
                cube_model=cube_model,
                schema_snapshot=schema_dict,
                cfg=dw_cfg,
            )
            response.xmla_script = xmla_script

            save_cube_record(
                dw_id=req.dw,
                cube_name=cube_model.cube_name,
                ssas_database=ssas_database,
                cube_model=cube_model.model_dump(),
                status="created",
            )

        persist_history_cube(req, response)
        return response

    # LOAD EXISTING CUBE
    if not cube_name:
        existing = load_cube_record(req.dw)
    else:
        existing = load_cube_record(req.dw, cube_name=cube_name)

    if not existing:
        response = CubeActionResponse(
            status="error",
            intent=intent.intent,
            message="Aucun cube existant trouvé pour appliquer cette action.",
        )
        persist_history_cube(req, response)
        return response

    cube_model = cube_model_from_registry(existing)

    # ADD_MEASURE
    if intent.intent == IntentType.ADD_MEASURE:
        cube_model, target_measure = add_measure_to_cube(cube_model, intent)

        if not target_measure:
            response = CubeActionResponse(
                status="invalid",
                intent=intent.intent,
                cube_model=cube_model,
                preview={
                    "cube_name": cube_model.cube_name,
                    "facts": [f.name for f in cube_model.facts],
                    "dimensions": [d.name for d in cube_model.dimensions],
                },
                message="Impossible d'identifier la mesure cible.",
            )
            persist_history_cube(req, response)
            return response

        validation = validate_cube_model(cube_model)

        if validation.is_valid:
            xmla_script = generate_xmla_alter_add_measure(
                ssas_database=ssas_database,
                cube_name=cube_model.cube_name,
                measure=target_measure,
            )

            save_cube_record(
                dw_id=req.dw,
                cube_name=cube_model.cube_name,
                ssas_database=ssas_database,
                cube_model=cube_model.model_dump(),
                status="updated",
            )

            response = CubeActionResponse(
                status="success",
                intent=intent.intent,
                cube_model=cube_model,
                validation=validation,
                xmla_script=xmla_script,
                preview={
                    "cube_name": cube_model.cube_name,
                    "facts": [f.name for f in cube_model.facts],
                    "dimensions": [d.name for d in cube_model.dimensions],
                    "target_measure": target_measure.name if target_measure else None,
                },
                message="Mesure ajoutée dans le cube existant. XMLA ALTER généré.",
            )
        else:
            response = CubeActionResponse(
                status="invalid",
                intent=intent.intent,
                cube_model=cube_model,
                validation=validation,
                preview={
                    "cube_name": cube_model.cube_name,
                    "facts": [f.name for f in cube_model.facts],
                    "dimensions": [d.name for d in cube_model.dimensions],
                    "target_measure": target_measure.name if target_measure else None,
                },
                message="Mesure ajoutée mais validation échouée. XMLA ALTER non généré.",
            )

        persist_history_cube(req, response)
        return response

    # MODIFY_DIMENSION
    if intent.intent == IntentType.MODIFY_DIMENSION:
        cube_model, target_dimension = modify_dimension_in_cube(cube_model, intent)

        if not target_dimension:
            response = CubeActionResponse(
                status="invalid",
                intent=intent.intent,
                cube_model=cube_model,
                preview={
                    "cube_name": cube_model.cube_name,
                    "facts": [f.name for f in cube_model.facts],
                    "dimensions": [d.name for d in cube_model.dimensions],
                },
                message="Impossible d'identifier la dimension cible.",
            )
            persist_history_cube(req, response)
            return response

        validation = validate_cube_model(cube_model)

        if validation.is_valid:
            xmla_script = generate_xmla_alter_modify_dimension(
                ssas_database=ssas_database,
                dimension=target_dimension,
            )

            save_cube_record(
                dw_id=req.dw,
                cube_name=cube_model.cube_name,
                ssas_database=ssas_database,
                cube_model=cube_model.model_dump(),
                status="updated",
            )

            response = CubeActionResponse(
                status="success",
                intent=intent.intent,
                cube_model=cube_model,
                validation=validation,
                xmla_script=xmla_script,
                preview={
                    "cube_name": cube_model.cube_name,
                    "facts": [f.name for f in cube_model.facts],
                    "dimensions": [d.name for d in cube_model.dimensions],
                    "target_dimension": target_dimension.name if target_dimension else None,
                },
                message="Dimension modifiée dans le cube existant. XMLA ALTER généré.",
            )
        else:
            response = CubeActionResponse(
                status="invalid",
                intent=intent.intent,
                cube_model=cube_model,
                validation=validation,
                preview={
                    "cube_name": cube_model.cube_name,
                    "facts": [f.name for f in cube_model.facts],
                    "dimensions": [d.name for d in cube_model.dimensions],
                    "target_dimension": target_dimension.name if target_dimension else None,
                },
                message="Dimension modifiée mais validation échouée. XMLA ALTER non généré.",
            )

        persist_history_cube(req, response)
        return response

    response = CubeActionResponse(
        status="invalid",
        intent=intent.intent,
        message="Intention reconnue mais aucun traitement n'a été appliqué.",
    )
    persist_history_cube(req, response)
    return response


@app.get("/history/{dw_id}")
def get_history(dw_id: str):
    items = get_prompt_history(dw_id)
    return {
        "status": "success",
        "items": items
    }
import json
import os
import re
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from google.adk.memory import InMemoryMemoryService
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.cloud import bigquery
from google.genai.types import Content, Part
from pydantic import BaseModel

from .agent import root_agent

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "learn-mcp-490919")
SESSION_TABLE = f"{PROJECT_ID}.lease_lens.expansion_sessions"
APP_NAME = "lease_lens_ai"
USER_ID = "lease_lens_ui"

app = FastAPI(title="LeaseLens AI Backend")
session_service = InMemorySessionService()
memory_service = InMemoryMemoryService()
runner = Runner(
    agent=root_agent,
    app_name=APP_NAME,
    session_service=session_service,
    memory_service=memory_service,
)


class RecommendationRequest(BaseModel):
    business_type: str
    budget: str
    area_name: str = ""
    pincode: str = ""
    customer_type: str
    competition_tolerance: str


def ensure_session_table(client: bigquery.Client):
    schema = [
        bigquery.SchemaField("session_id", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("business_type", "STRING"),
        bigquery.SchemaField("budget", "STRING"),
        bigquery.SchemaField("customer_type", "STRING"),
        bigquery.SchemaField("competition_tolerance", "STRING"),
        bigquery.SchemaField("area_name", "STRING"),
        bigquery.SchemaField("pincode", "STRING"),
        bigquery.SchemaField("lead_market", "STRING"),
        bigquery.SchemaField("launch_thesis", "STRING"),
        bigquery.SchemaField("risk_watch", "STRING"),
        bigquery.SchemaField("next_milestone", "STRING"),
    ]
    try:
        client.get_table(SESSION_TABLE)
    except Exception:
        client.create_table(bigquery.Table(SESSION_TABLE, schema=schema), exists_ok=True)


def save_expansion_session(client: bigquery.Client, payload: RecommendationRequest, response: dict[str, Any], session_id: str):
    ensure_session_table(client)
    snapshot = response.get("decision_snapshot", {})
    rows = [{
        "session_id": session_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "business_type": payload.business_type,
        "budget": payload.budget,
        "customer_type": payload.customer_type,
        "competition_tolerance": payload.competition_tolerance,
        "area_name": payload.area_name,
        "pincode": payload.pincode,
        "lead_market": snapshot.get("lead_market", ""),
        "launch_thesis": snapshot.get("launch_thesis", ""),
        "risk_watch": snapshot.get("risk_watch", ""),
        "next_milestone": snapshot.get("next_milestone", ""),
    }]
    client.insert_rows_json(SESSION_TABLE, rows)


def extract_json_block(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?", "", stripped).strip()
        stripped = re.sub(r"```$", "", stripped).strip()
    match = re.search(r"\{.*\}", stripped, flags=re.S)
    if not match:
        raise ValueError("No JSON object found in agent response.")
    return json.loads(match.group(0))


def as_text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def as_number(value: Any, fallback: float = 0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def normalize_recommendation(item: dict[str, Any]) -> dict[str, Any]:
    area_name = as_text(item.get("area_name"), "Bangalore retail shortlist")
    pincode = as_text(item.get("pincode"), "Pincode N/A")
    final_score = round(as_number(item.get("final_score"), 0), 2)
    mall_count = int(as_number(item.get("mall_count"), 0))
    office_count = int(as_number(item.get("office_count"), 0))
    school_count = int(as_number(item.get("school_count"), 0))
    metro_count = int(as_number(item.get("metro_count"), 0))
    mg_road = int(as_number(item.get("traffic_minutes_to_mg_road"), 0))
    koramangala = int(as_number(item.get("traffic_minutes_to_koramangala"), 0))
    whitefield = int(as_number(item.get("traffic_minutes_to_whitefield"), 0))
    maps_url = as_text(
        item.get("maps_url"),
        f"https://www.google.com/maps/search/?api=1&query={area_name.replace(' ', '+')},+Bangalore",
    )

    return {
        "area_name": area_name,
        "pincode": pincode,
        "summary": as_text(
            item.get("summary"),
            f"{area_name} is a shortlisted Bangalore retail market with balanced demand, access, and rollout potential.",
        ),
        "positioning": as_text(item.get("positioning"), "balanced neighborhood retail"),
        "final_score": final_score,
        "market_intensity": as_text(item.get("market_intensity"), "Medium"),
        "market_strength": as_text(item.get("market_strength"), "promising"),
        "mall_count": mall_count,
        "office_count": office_count,
        "school_count": school_count,
        "metro_count": metro_count,
        "traffic_minutes_to_mg_road": mg_road,
        "traffic_minutes_to_koramangala": koramangala,
        "traffic_minutes_to_whitefield": whitefield,
        "maps_url": maps_url,
    }


def normalize_response(payload: RecommendationRequest, parsed: dict[str, Any]) -> dict[str, Any]:
    recommendations = parsed.get("recommendations")
    if not isinstance(recommendations, list):
        recommendations = []

    normalized_recommendations = [
        normalize_recommendation(item)
        for item in recommendations
        if isinstance(item, dict)
    ][:3]

    lead = normalized_recommendations[0] if normalized_recommendations else None
    decision_snapshot = parsed.get("decision_snapshot", {})
    execution_plan = parsed.get("execution_plan", {})

    return {
        "title": as_text(
            parsed.get("title"),
            f"LeaseLens AI Expansion Plan for a {payload.budget} budget {payload.business_type.title()} in Bangalore",
        ),
        "subtitle": as_text(
            parsed.get("subtitle"),
            f"Target customer: {payload.customer_type} | Competition tolerance: {payload.competition_tolerance}",
        ),
        "copilot_summary": as_text(
            parsed.get("copilot_summary"),
            "LeaseLens AI coordinated location intelligence, customer fit analysis, and launch planning for this expansion brief.",
        ),
        "decision_snapshot": {
            "lead_market": as_text(
                decision_snapshot.get("lead_market"),
                f"{lead['area_name']} ({lead['pincode']})" if lead else "Shortlist pending",
            ),
            "launch_thesis": as_text(
                decision_snapshot.get("launch_thesis"),
                f"{payload.business_type.title()} for {payload.customer_type.lower()} in a high-potential Bangalore micro-market."
                if lead else "",
            ),
            "risk_watch": as_text(
                decision_snapshot.get("risk_watch"),
                "Validate rent, frontage, and local crowding before finalizing the site.",
            ),
            "next_milestone": as_text(
                decision_snapshot.get("next_milestone"),
                f"Complete site validation for {lead['area_name']}." if lead else "Complete shortlist validation.",
            ),
        },
        "recommendations": normalized_recommendations,
        "execution_plan": {
            "coordinator_brief": as_text(
                execution_plan.get("coordinator_brief"),
                "The coordinator aligned the retail brief with Bangalore market intelligence and launch planning.",
            ),
            "intelligence_focus": as_text(
                execution_plan.get("intelligence_focus"),
                "The location analysis prioritized demand, accessibility, and commercial fit.",
            ),
            "next_steps": execution_plan.get("next_steps")
            if isinstance(execution_plan.get("next_steps"), list)
            else [
                "Validate the lead market with on-ground site visits.",
                "Compare rent and visibility across the shortlisted options.",
                "Review launch economics before locking the site.",
            ],
            "decision_checklist": execution_plan.get("decision_checklist")
            if isinstance(execution_plan.get("decision_checklist"), list)
            else [
                "Confirm rent range and deposit ceiling.",
                "Check frontage, walk-in visibility, and access.",
                "Validate neighborhood demand signals before finalizing the site.",
            ],
        },
    }


def build_prompt(payload: RecommendationRequest) -> str:
    area_constraint = payload.area_name or "none"
    pincode_constraint = payload.pincode or "none"
    return f"""
You are coordinating a Bangalore retail expansion decision.

Business brief:
- business_type: {payload.business_type}
- budget: {payload.budget}
- customer_type: {payload.customer_type}
- competition_tolerance: {payload.competition_tolerance}
- preferred_area: {area_constraint}
- pincode: {pincode_constraint}

Use the specialist sub-agents and available MCP tools to complete the workflow.

Return ONLY valid JSON with this exact shape:
{{
  "title": "string",
  "subtitle": "string",
  "copilot_summary": "string",
  "decision_snapshot": {{
    "lead_market": "string",
    "launch_thesis": "string",
    "risk_watch": "string",
    "next_milestone": "string"
  }},
  "recommendations": [
    {{
      "area_name": "string",
      "pincode": "string",
      "summary": "string",
      "positioning": "string",
      "final_score": "number or string",
      "market_intensity": "string",
      "market_strength": "string",
      "mall_count": "number",
      "office_count": "number",
      "school_count": "number",
      "metro_count": "number",
      "traffic_minutes_to_mg_road": "number",
      "traffic_minutes_to_koramangala": "number",
      "traffic_minutes_to_whitefield": "number",
      "maps_url": "string"
    }}
  ],
  "execution_plan": {{
    "coordinator_brief": "string",
    "intelligence_focus": "string",
    "next_steps": ["string"],
    "decision_checklist": ["string"]
  }}
}}

Rules:
- recommend top 3 areas unless the brief strongly narrows to fewer
- use BigQuery MCP and Maps MCP when helpful
- keep the response product-grade and concise
- do not output markdown or prose outside the JSON object
""".strip()


async def run_agent_json(payload: RecommendationRequest) -> dict[str, Any]:
    session_id = str(uuid4())
    await runner.session_service.create_session(
        app_name=APP_NAME,
        user_id=USER_ID,
        session_id=session_id,
    )

    prompt = build_prompt(payload)
    message = Content(parts=[Part(text=prompt)], role="user")
    final_response_text = ""

    async for event in runner.run_async(user_id=USER_ID, session_id=session_id, new_message=message):
        if event.is_final_response() and event.content and event.content.parts:
            final_response_text = "".join(
                part.text for part in event.content.parts if getattr(part, "text", None)
            )

    if not final_response_text:
        raise HTTPException(status_code=502, detail="Backend agent did not return a final response.")

    try:
        parsed = extract_json_block(final_response_text)
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Backend agent returned non-JSON output: {str(exc)}",
        ) from exc

    normalized = normalize_response(payload, parsed)
    normalized["_session_id"] = session_id
    return normalized


@app.get("/health")
async def health():
    return {"ok": True, "service": "lease-lens-ai"}


@app.get("/")
async def root():
    return {
        "service": "lease-lens-ai",
        "status": "ok",
        "entrypoints": ["/health", "/recommend-agent", "/recent-sessions"],
    }


@app.post("/recommend-agent")
async def recommend_agent(payload: RecommendationRequest):
    response = await run_agent_json(payload)
    session_id = response.pop("_session_id", str(uuid4()))
    client = bigquery.Client(project=PROJECT_ID)
    save_expansion_session(client, payload, response, session_id)
    return JSONResponse(response)


@app.get("/recent-sessions")
async def recent_sessions():
    client = bigquery.Client(project=PROJECT_ID)
    ensure_session_table(client)
    query = f"""
    SELECT
      created_at,
      business_type,
      budget,
      customer_type,
      lead_market,
      risk_watch,
      next_milestone
    FROM `{SESSION_TABLE}`
    ORDER BY created_at DESC
    LIMIT 5
    """
    rows = list(client.query(query).result())
    return JSONResponse({
        "sessions": [
            {
                "created_at": str(row.created_at),
                "business_type": row.business_type,
                "budget": row.budget,
                "customer_type": row.customer_type,
                "lead_market": row.lead_market,
                "risk_watch": row.risk_watch,
                "next_milestone": row.next_milestone,
            }
            for row in rows
        ]
    })

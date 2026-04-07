import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from google.cloud import bigquery
from pydantic import BaseModel

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "learn-mcp-490919")
AREA_TABLE = f"{PROJECT_ID}.lease_lens.area_live_scores_serving"
PROFILE_TABLE = f"{PROJECT_ID}.lease_lens.business_profiles"
SESSION_TABLE = f"{PROJECT_ID}.lease_lens.expansion_sessions"

app = FastAPI(title="LeaseLens AI Backend")


class RecommendationRequest(BaseModel):
    business_type: str
    budget: str
    area_name: str = ""
    pincode: str = ""
    customer_type: str
    competition_tolerance: str


def clean_pincode(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())[:6]


def commercial_intensity(row) -> str:
    score = (
        float(row.mall_count or 0) * 1.3
        + float(row.office_count or 0) * 1.1
        + float(row.metro_count or 0) * 0.9
    )
    if score >= 22:
        return "High"
    if score >= 12:
        return "Medium"
    return "Low"


def accessibility_band(row) -> str:
    avg_time = (
        float(row.traffic_minutes_to_mg_road or 0)
        + float(row.traffic_minutes_to_koramangala or 0)
        + float(row.traffic_minutes_to_whitefield or 0)
    ) / 3.0
    if avg_time <= 22:
        return "excellent"
    if avg_time <= 32:
        return "good"
    return "moderate"


def top_drivers(row):
    drivers = [
        ("mall traffic", float(row.mall_count or 0)),
        ("office demand", float(row.office_count or 0)),
        ("school-family demand", float(row.school_count or 0)),
        ("metro connectivity", float(row.metro_count or 0)),
    ]
    drivers.sort(key=lambda item: item[1], reverse=True)
    return drivers[:2]


def demand_mix(customer_type: str, business_type: str) -> dict[str, float]:
    customer_key = customer_type.lower()
    business_key = business_type.lower()

    mix = {
        "mall_count": 1.0,
        "office_count": 1.0,
        "school_count": 1.0,
        "metro_count": 1.0,
    }

    if customer_key == "families":
        mix["school_count"] += 0.9
        mix["mall_count"] += 0.2
    elif customer_key == "young professionals":
        mix["office_count"] += 0.9
        mix["metro_count"] += 0.4
    elif customer_key == "students":
        mix["school_count"] += 0.8
        mix["metro_count"] += 0.5
    elif customer_key == "premium customers":
        mix["mall_count"] += 1.0
        mix["office_count"] += 0.3
    elif customer_key == "daily commuters":
        mix["metro_count"] += 1.0
        mix["office_count"] += 0.3

    if business_key == "salon":
        mix["mall_count"] += 0.8
        mix["office_count"] += 0.5
    elif business_key == "pharmacy":
        mix["school_count"] += 0.5
        mix["metro_count"] += 0.4
    elif business_key == "grocery":
        mix["school_count"] += 0.8
        mix["metro_count"] += 0.2
    elif business_key == "boutique":
        mix["mall_count"] += 1.0
        mix["office_count"] += 0.3
    elif business_key == "clinic":
        mix["school_count"] += 0.7
        mix["metro_count"] += 0.3
    elif business_key in {"bakery", "cafe"}:
        mix["mall_count"] += 0.6
        mix["office_count"] += 0.4

    return mix


def accessibility_score(row, budget: str) -> float:
    avg_time = (
        float(row.traffic_minutes_to_mg_road or 0)
        + float(row.traffic_minutes_to_koramangala or 0)
        + float(row.traffic_minutes_to_whitefield or 0)
    ) / 3.0

    if avg_time <= 18:
        score = 100
    elif avg_time <= 24:
        score = 82
    elif avg_time <= 32:
        score = 66
    elif avg_time <= 40:
        score = 52
    else:
        score = 40

    if budget.lower() == "low":
        score += 6
    elif budget.lower() == "high":
        score -= 4

    return max(0, min(100, score))


def demand_score(row, customer_type: str, business_type: str) -> float:
    mix = demand_mix(customer_type, business_type)
    total = (
        float(row.mall_count or 0) * mix["mall_count"]
        + float(row.office_count or 0) * mix["office_count"]
        + float(row.school_count or 0) * mix["school_count"]
        + float(row.metro_count or 0) * mix["metro_count"]
    )
    return min(100.0, total * 4.2)


def fit_score(row, payload: RecommendationRequest) -> float:
    base = float(row.final_score or 0)
    demand = demand_score(row, payload.customer_type, payload.business_type)
    access = accessibility_score(row, payload.budget)
    quality = min(100.0, float(row.avg_rating or 0) * 20)
    return round(base * 0.5 + demand * 0.25 + access * 0.15 + quality * 0.10, 2)


def primary_signal(row, customer_type: str, business_type: str) -> str:
    mix = demand_mix(customer_type, business_type)
    weighted = [
        ("Lifestyle traffic", float(row.mall_count or 0) * mix["mall_count"]),
        ("Office catchment", float(row.office_count or 0) * mix["office_count"]),
        ("Family demand", float(row.school_count or 0) * mix["school_count"]),
        ("Transit access", float(row.metro_count or 0) * mix["metro_count"]),
    ]
    weighted.sort(key=lambda item: item[1], reverse=True)
    return weighted[0][0]


def watchout_label(row) -> str:
    intensity = commercial_intensity(row)
    access = accessibility_band(row)
    if intensity == "High" and access == "moderate":
        return "Crowded micro-market with slower cross-city access."
    if intensity == "High":
        return "High-activity zone, so frontage and differentiation matter."
    if access == "moderate":
        return "Works best with a strong local catchment rather than pass-through traffic."
    return "Low structural risk for a first-pass shortlist."


def market_narrative(row, payload: RecommendationRequest) -> str:
    signal = primary_signal(row, payload.customer_type, payload.business_type)
    access = accessibility_band(row)
    return (
        f"Best for {signal.lower()} with {access} accessibility for a "
        f"{payload.customer_type.lower()}-focused {payload.business_type.lower()} format."
    )


def build_summary(row, display_name: str, fit_text: str, positioning_hint: str) -> str:
    drivers = top_drivers(row)
    primary = drivers[0][0]
    secondary = drivers[1][0]
    access = accessibility_band(row)
    intensity = commercial_intensity(row).lower()

    if primary == "office demand":
        opener = f"{row.area_name} stands out for a {display_name.lower()} because it benefits from strong weekday office footfall"
    elif primary == "mall traffic":
        opener = f"{row.area_name} is a strong fit for a {display_name.lower()} because it captures high lifestyle and destination traffic"
    elif primary == "metro connectivity":
        opener = f"{row.area_name} is attractive for a {display_name.lower()} because metro-led accessibility improves daily visit potential"
    else:
        opener = f"{row.area_name} works well for a {display_name.lower()} because it benefits from neighborhood-driven local demand"

    return (
        f"{opener}. The area is supported by {primary} and {secondary}, has {access} cross-city accessibility, "
        f"and shows {intensity} market activity. This makes it suitable for {positioning_hint} and aligns well with "
        f"{fit_text}."
    )


def market_strength_label(row) -> str:
    score = float(row.final_score or 0)
    if score >= 78:
        return "high-conviction"
    if score >= 62:
        return "promising"
    return "watchlist"


def risk_signal(recommendation: dict[str, Any], competition_tolerance: str) -> str:
    intensity = recommendation["market_intensity"]
    avg_time = (
        float(recommendation["traffic_minutes_to_mg_road"] or 0)
        + float(recommendation["traffic_minutes_to_koramangala"] or 0)
        + float(recommendation["traffic_minutes_to_whitefield"] or 0)
    ) / 3.0
    tolerance = competition_tolerance.lower()

    if intensity == "High" and tolerance == "low":
        return "High market crowding risk"
    if intensity == "High" and avg_time > 32:
        return "Crowded zone with slower cross-city access"
    if avg_time > 32:
        return "Execution depends on localized catchment"
    return "Balanced risk for current brief"


def build_decision_snapshot(recommendations, display_name: str, customer_type: str, competition_tolerance: str):
    lead = recommendations[0]
    return {
        "lead_market": f"{lead['area_name']} ({lead['pincode']})",
        "launch_thesis": (
            f"{display_name} for {customer_type.lower()} with {lead['market_intensity'].lower()} market activity."
        ),
        "risk_watch": risk_signal(lead, competition_tolerance),
        "next_milestone": f"Complete broker validation and on-ground visit for {lead['area_name']}.",
    }


def build_execution_plan(recommendations, display_name: str, customer_type: str, budget: str):
    lead = recommendations[0]
    area_label = f"{lead['area_name']} ({lead['pincode']})"
    return {
        "coordinator_brief": (
            f"The coordinator agent shortlisted {len(recommendations)} Bangalore options for a "
            f"{budget} budget {display_name.lower()} and is prioritizing {area_label} as the lead expansion zone."
        ),
        "intelligence_focus": (
            f"The location intelligence agent found that {lead['area_name']} offers the best balance of demand, "
            f"accessibility, and market intensity for {customer_type.lower()}."
        ),
        "next_steps": [
            f"Validate {area_label} with a broker shortlist and one on-ground site visit this week.",
            f"Compare rent, frontage, and walk-in visibility across the top {len(recommendations)} shortlisted areas.",
            f"Build a launch P&L for a {display_name.lower()} in {lead['area_name']} using the {budget.lower()} budget assumption.",
            f"Prepare a neighborhood-specific offer mix for {customer_type.lower()} before the final site decision.",
        ],
        "decision_checklist": [
            "Confirm target rent range and deposit ceiling.",
            "Verify frontage, parking, and peak-footfall windows.",
            "Check nearby anchors such as malls, offices, schools, and metro exits.",
            "Review licensing, staffing, and launch timeline assumptions before locking the site.",
        ],
    }


def candidate_query(filters: list[str]) -> str:
    return f"""
    SELECT
      area_name,
      pincode,
      final_score,
      mall_count,
      office_count,
      school_count,
      metro_count,
      avg_rating,
      traffic_minutes_to_mg_road,
      traffic_minutes_to_koramangala,
      traffic_minutes_to_whitefield
    FROM `{AREA_TABLE}`
    WHERE {' AND '.join(filters)}
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY LOWER(area_name), REGEXP_EXTRACT(CAST(pincode AS STRING), r'(\\d{{6}})')
      ORDER BY scored_at DESC, final_score DESC
    ) = 1
    ORDER BY final_score DESC
    LIMIT 12
    """


def valid_area_clause() -> str:
    return """
    area_name IS NOT NULL
    AND TRIM(area_name) != ''
    AND NOT REGEXP_CONTAINS(
      LOWER(area_name),
      r'(\\bso\\b|\\bbo\\b|\\bgpo\\b|\\bho\\b|post office|campus|corporation|building|quarters|station)'
    )
    AND LOWER(TRIM(area_name)) NOT IN ('bangalore', 'bengaluru')
    """


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


def save_expansion_session(
    client: bigquery.Client,
    payload: RecommendationRequest,
    decision_snapshot: dict[str, str],
    session_id: str,
):
    ensure_session_table(client)
    rows = [{
        "session_id": session_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "business_type": payload.business_type,
        "budget": payload.budget,
        "customer_type": payload.customer_type,
        "competition_tolerance": payload.competition_tolerance,
        "area_name": payload.area_name,
        "pincode": payload.pincode,
        "lead_market": decision_snapshot.get("lead_market", ""),
        "launch_thesis": decision_snapshot.get("launch_thesis", ""),
        "risk_watch": decision_snapshot.get("risk_watch", ""),
        "next_milestone": decision_snapshot.get("next_milestone", ""),
    }]
    client.insert_rows_json(SESSION_TABLE, rows)


def run_backend_engine(payload: RecommendationRequest) -> dict[str, Any]:
    client = bigquery.Client(project=PROJECT_ID)
    business_type = payload.business_type.strip()
    budget = payload.budget.strip()
    area_name = payload.area_name.strip()
    pincode = clean_pincode(payload.pincode)
    customer_type = payload.customer_type.strip()
    competition_tolerance = payload.competition_tolerance.strip()

    if not business_type or not budget or not customer_type or not competition_tolerance:
        raise HTTPException(status_code=400, detail="Please complete all required fields.")

    if area_name and pincode:
        raise HTTPException(status_code=400, detail="Choose either Preferred Area or Bangalore Pincode, not both.")

    if pincode and len(pincode) != 6:
        raise HTTPException(status_code=400, detail="Please enter a valid 6-digit Bangalore pincode or leave it blank.")

    profile_query = f"""
    SELECT display_name, fit_text, positioning_hint
    FROM `{PROFILE_TABLE}`
    WHERE LOWER(business_type) = LOWER(@business_type)
       OR EXISTS (
         SELECT 1
         FROM UNNEST(aliases) alias
         WHERE LOWER(alias) = LOWER(@business_type)
       )
    LIMIT 1
    """
    profile_cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("business_type", "STRING", business_type)]
    )
    profile_rows = list(client.query(profile_query, job_config=profile_cfg).result())

    if profile_rows:
        profile = profile_rows[0]
        display_name = profile.display_name
        fit_text = profile.fit_text
        positioning_hint = profile.positioning_hint
    else:
        display_name = business_type.title()
        fit_text = "balanced urban demand and neighborhood accessibility"
        positioning_hint = "well-positioned neighborhood retail format"

    filters = [valid_area_clause()]
    params = []

    if area_name:
        filters.append("LOWER(area_name) LIKE LOWER(@area_name)")
        params.append(bigquery.ScalarQueryParameter("area_name", "STRING", f"%{area_name}%"))

    if pincode:
        filters.append("REGEXP_EXTRACT(CAST(pincode AS STRING), r'(\\d{6})') = @pincode")
        params.append(bigquery.ScalarQueryParameter("pincode", "STRING", pincode))

    cfg = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(client.query(candidate_query(filters), job_config=cfg).result())

    if not rows:
        raise HTTPException(status_code=404, detail="No matching recommendation found.")

    ranked_rows = sorted(
        rows,
        key=lambda row: (
            fit_score(row, payload),
            float(row.final_score or 0),
            float(row.avg_rating or 0),
        ),
        reverse=True,
    )[:3]

    recommendations = []
    for row in ranked_rows:
        recommendations.append({
            "area_name": row.area_name,
            "pincode": clean_pincode(row.pincode) if row.pincode else "Pincode N/A",
            "summary": build_summary(row, display_name, fit_text, positioning_hint),
            "positioning": positioning_hint,
            "final_score": fit_score(row, payload),
            "market_intensity": commercial_intensity(row),
            "market_strength": market_strength_label(row),
            "market_narrative": market_narrative(row, payload),
            "primary_signal": primary_signal(row, customer_type, business_type),
            "watchout": watchout_label(row),
            "mall_count": int(row.mall_count or 0),
            "office_count": int(row.office_count or 0),
            "school_count": int(row.school_count or 0),
            "metro_count": int(row.metro_count or 0),
            "traffic_minutes_to_mg_road": int(row.traffic_minutes_to_mg_road or 0),
            "traffic_minutes_to_koramangala": int(row.traffic_minutes_to_koramangala or 0),
            "traffic_minutes_to_whitefield": int(row.traffic_minutes_to_whitefield or 0),
            "maps_url": "https://www.google.com/maps/search/?api=1&query=" + quote_plus(f"{row.area_name}, Bangalore"),
        })

    decision_snapshot = build_decision_snapshot(recommendations, display_name, customer_type, competition_tolerance)
    execution_plan = build_execution_plan(recommendations, display_name, customer_type, budget)
    save_expansion_session(client, payload, decision_snapshot, str(uuid4()))

    return {
        "title": f"LeaseLens AI Expansion Plan for a {budget} budget {display_name} in Bangalore",
        "subtitle": f"Target customer: {customer_type} | Competition tolerance: {competition_tolerance}",
        "copilot_summary": (
            "LeaseLens AI coordinated location intelligence, customer-fit analysis, and launch planning "
            "through a backend execution workflow to produce this expansion recommendation."
        ),
        "decision_snapshot": decision_snapshot,
        "recommendations": recommendations,
        "execution_plan": execution_plan,
    }


@app.get("/")
async def root():
    return {
        "service": "lease-lens-ai",
        "status": "ok",
        "entrypoints": ["/health", "/recommend-agent", "/recent-sessions"],
    }


@app.get("/health")
async def health():
    return {"ok": True, "service": "lease-lens-ai"}


@app.post("/recommend-agent")
async def recommend_agent(payload: RecommendationRequest):
    return JSONResponse(run_backend_engine(payload))


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

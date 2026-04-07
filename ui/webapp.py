import os
from pathlib import Path
from urllib.parse import quote_plus

from fastapi import FastAPI, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from google.cloud import bigquery

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "learn-mcp-490919")
AREA_TABLE = f"{PROJECT_ID}.lease_lens.area_live_scores_serving"
PROFILE_TABLE = f"{PROJECT_ID}.lease_lens.business_profiles"

app = FastAPI(title="LeaseLens UI")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


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


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(request, "index.html", {})


@app.get("/area-suggestions")
async def area_suggestions(q: str = Query("", min_length=0, max_length=50)):
    client = bigquery.Client(project=PROJECT_ID)
    q = q.strip()

    if q:
        query = f"""
        SELECT area_name
        FROM `{AREA_TABLE}`
        WHERE {valid_area_clause()}
          AND LOWER(area_name) LIKE LOWER(@query)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(area_name) ORDER BY scored_at DESC, final_score DESC) = 1
        ORDER BY area_name
        LIMIT 8
        """
        cfg = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("query", "STRING", f"%{q}%")]
        )
        rows = list(client.query(query, job_config=cfg).result())
    else:
        query = f"""
        SELECT area_name
        FROM `{AREA_TABLE}`
        WHERE {valid_area_clause()}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(area_name) ORDER BY scored_at DESC, final_score DESC) = 1
        ORDER BY area_name
        LIMIT 8
        """
        rows = list(client.query(query).result())

    return JSONResponse({"areas": [row.area_name for row in rows]})


@app.get("/pincode-suggestions")
async def pincode_suggestions(q: str = Query("", min_length=0, max_length=10)):
    client = bigquery.Client(project=PROJECT_ID)
    q = clean_pincode(q)

    if q:
        query = f"""
        SELECT DISTINCT REGEXP_EXTRACT(CAST(pincode AS STRING), r'(\\d{{6}})') AS clean_pincode
        FROM `{AREA_TABLE}`
        WHERE REGEXP_EXTRACT(CAST(pincode AS STRING), r'(\\d{{6}})') IS NOT NULL
          AND REGEXP_EXTRACT(CAST(pincode AS STRING), r'(\\d{{6}})') LIKE @query
        ORDER BY clean_pincode
        LIMIT 8
        """
        cfg = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("query", "STRING", f"{q}%")]
        )
        rows = list(client.query(query, job_config=cfg).result())
    else:
        query = f"""
        SELECT DISTINCT REGEXP_EXTRACT(CAST(pincode AS STRING), r'(\\d{{6}})') AS clean_pincode
        FROM `{AREA_TABLE}`
        WHERE REGEXP_EXTRACT(CAST(pincode AS STRING), r'(\\d{{6}})') IS NOT NULL
        ORDER BY clean_pincode
        LIMIT 8
        """
        rows = list(client.query(query).result())

    return JSONResponse({"pincodes": [row.clean_pincode for row in rows if row.clean_pincode]})


@app.post("/recommend")
async def recommend(
    business_type: str = Form(...),
    budget: str = Form(...),
    area_name: str = Form(""),
    pincode: str = Form(""),
    customer_type: str = Form(...),
    competition_tolerance: str = Form(...),
):
    business_type = business_type.strip()
    budget = budget.strip()
    area_name = area_name.strip()
    pincode = clean_pincode(pincode)
    customer_type = customer_type.strip()
    competition_tolerance = competition_tolerance.strip()

    if not business_type or not budget or not customer_type or not competition_tolerance:
        return JSONResponse({"error": "Please complete all required fields."}, status_code=400)

    if area_name and pincode:
        return JSONResponse({"error": "Choose either Preferred Area or Bangalore Pincode, not both."}, status_code=400)

    if pincode and len(pincode) != 6:
        return JSONResponse({"error": "Please enter a valid 6-digit Bangalore pincode or leave it blank."}, status_code=400)

    client = bigquery.Client(project=PROJECT_ID)

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

    query = f"""
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
    QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(area_name), REGEXP_EXTRACT(CAST(pincode AS STRING), r'(\\d{{6}})') ORDER BY scored_at DESC, final_score DESC) = 1
    ORDER BY final_score DESC
    LIMIT 3
    """

    cfg = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(client.query(query, job_config=cfg).result())

    if not rows:
        return JSONResponse({"error": "No matching recommendation found."}, status_code=404)

    recommendations = []
    for row in rows:
        maps_url = "https://www.google.com/maps/search/?api=1&query=" + quote_plus(f"{row.area_name}, Bangalore")
        recommendations.append({
            "area_name": row.area_name,
            "pincode": clean_pincode(row.pincode) if row.pincode else "Pincode N/A",
            "summary": build_summary(row, display_name, fit_text, positioning_hint),
            "positioning": positioning_hint,
            "final_score": row.final_score,
            "market_intensity": commercial_intensity(row),
            "mall_count": row.mall_count,
            "office_count": row.office_count,
            "school_count": row.school_count,
            "metro_count": row.metro_count,
            "traffic_minutes_to_mg_road": row.traffic_minutes_to_mg_road,
            "traffic_minutes_to_koramangala": row.traffic_minutes_to_koramangala,
            "traffic_minutes_to_whitefield": row.traffic_minutes_to_whitefield,
            "maps_url": maps_url,
        })

    return JSONResponse({
        "title": f"LeaseLens AI Recommendations for a {budget} budget {display_name} in Bangalore",
        "subtitle": f"Target customer: {customer_type} | Competition tolerance: {competition_tolerance}",
        "recommendations": recommendations,
    })

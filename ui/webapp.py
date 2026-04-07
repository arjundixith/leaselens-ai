import os
from pathlib import Path

import httpx
from fastapi import FastAPI, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from google.cloud import bigquery

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "learn-mcp-490919")
AREA_TABLE = f"{PROJECT_ID}.lease_lens.area_live_scores_serving"
BACKEND_URL = os.getenv(
    "LEASELENS_BACKEND_URL",
    "https://lease-lens-ai-6093695901.asia-south1.run.app",
).rstrip("/")

app = FastAPI(title="LeaseLens UI")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def clean_pincode(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())[:6]


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


async def call_backend(method: str, path: str, **kwargs):
    url = f"{BACKEND_URL}{path}"
    async with httpx.AsyncClient(timeout=httpx.Timeout(90.0, connect=10.0)) as client:
        response = await client.request(method, url, **kwargs)
    return response


@app.get("/recent-sessions")
async def recent_sessions():
    try:
        response = await call_backend("GET", "/recent-sessions")
    except httpx.HTTPError:
        return JSONResponse(
            {"sessions": [], "error": "Unable to reach the backend session service right now."},
            status_code=502,
        )

    if response.is_success:
        return JSONResponse(response.json())

    return JSONResponse(
        {"sessions": [], "error": "Unable to load recent sessions from the backend."},
        status_code=502,
    )


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(request, "index.html", {})


@app.get("/engine-details", response_class=HTMLResponse)
async def engine_details(request: Request):
    return templates.TemplateResponse(request, "engine_details.html", {})


@app.get("/area-suggestions")
async def area_suggestions(q: str = Query("", min_length=0, max_length=50)):
    client = bigquery.Client(project=PROJECT_ID)
    q = q.strip()

    if q:
        query = f"""
        SELECT DISTINCT area_name
        FROM `{AREA_TABLE}`
        WHERE {valid_area_clause()}
          AND STARTS_WITH(LOWER(area_name), LOWER(@query))
        ORDER BY area_name
        LIMIT 8
        """
        cfg = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("query", "STRING", q)]
        )
        rows = list(client.query(query, job_config=cfg).result())
    else:
        query = f"""
        SELECT area_name
        FROM (
          SELECT
            area_name,
            final_score,
            ROW_NUMBER() OVER (PARTITION BY LOWER(area_name) ORDER BY final_score DESC) AS rn
          FROM `{AREA_TABLE}`
          WHERE {valid_area_clause()}
        )
        WHERE rn = 1
        ORDER BY final_score DESC, area_name
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

    payload = {
        "business_type": business_type,
        "budget": budget,
        "area_name": area_name,
        "pincode": pincode,
        "customer_type": customer_type,
        "competition_tolerance": competition_tolerance,
    }

    try:
        response = await call_backend("POST", "/recommend-agent", json=payload)
    except httpx.HTTPError:
        return JSONResponse(
            {"error": "LeaseLens AI could not reach the backend coordinator. Please try again."},
            status_code=502,
        )

    if response.is_success:
        return JSONResponse(response.json())

    try:
        error_payload = response.json()
    except ValueError:
        error_payload = {}

    detail = error_payload.get("detail") or error_payload.get("error")
    return JSONResponse(
        {"error": detail or "Unable to generate an expansion plan right now."},
        status_code=500 if response.status_code < 400 else response.status_code,
    )

import os
import time
import asyncio
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

SUGGESTION_TTL_SECONDS = 1800
SUGGESTION_LIMIT = 8
_suggestion_lock = asyncio.Lock()
_suggestion_cache = {
    "areas": [],
    "pincodes": [],
    "loaded_at": 0.0,
}


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


async def get_suggestion_catalog() -> dict:
    now = time.time()
    if _suggestion_cache["areas"] and now - _suggestion_cache["loaded_at"] < SUGGESTION_TTL_SECONDS:
        return _suggestion_cache

    async with _suggestion_lock:
        now = time.time()
        if _suggestion_cache["areas"] and now - _suggestion_cache["loaded_at"] < SUGGESTION_TTL_SECONDS:
            return _suggestion_cache

        client = bigquery.Client(project=PROJECT_ID)
        area_query = f"""
        SELECT area_name
        FROM (
          SELECT
            area_name,
            MAX(final_score) AS top_score
          FROM `{AREA_TABLE}`
          WHERE {valid_area_clause()}
          GROUP BY area_name
        )
        ORDER BY top_score DESC, area_name
        LIMIT 2500
        """
        pincode_query = f"""
        SELECT DISTINCT REGEXP_EXTRACT(CAST(pincode AS STRING), r'(\\d{{6}})') AS clean_pincode
        FROM `{AREA_TABLE}`
        WHERE REGEXP_EXTRACT(CAST(pincode AS STRING), r'(\\d{{6}})') IS NOT NULL
        ORDER BY clean_pincode
        """

        area_rows = list(client.query(area_query).result())
        pincode_rows = list(client.query(pincode_query).result())

        _suggestion_cache["areas"] = [row.area_name for row in area_rows if row.area_name]
        _suggestion_cache["pincodes"] = [row.clean_pincode for row in pincode_rows if row.clean_pincode]
        _suggestion_cache["loaded_at"] = now

    return _suggestion_cache


def ranked_area_matches(query: str, areas: list[str]) -> list[str]:
    if not query:
        return areas[:SUGGESTION_LIMIT]

    lowered = query.lower()
    starts = []
    word_starts = []
    contains = []

    for area in areas:
        area_lower = area.lower()
        if area_lower.startswith(lowered):
            starts.append(area)
        elif any(part.startswith(lowered) for part in area_lower.split()):
            word_starts.append(area)
        elif lowered in area_lower:
            contains.append(area)

        if len(starts) + len(word_starts) + len(contains) >= 40:
            # enough candidates gathered from a ranked source list
            continue

    ordered = starts + word_starts + contains
    seen = set()
    deduped = []
    for area in ordered:
        if area in seen:
            continue
        seen.add(area)
        deduped.append(area)
        if len(deduped) >= SUGGESTION_LIMIT:
            break
    return deduped


def ranked_pincode_matches(query: str, pincodes: list[str]) -> list[str]:
    if not query:
        return pincodes[:SUGGESTION_LIMIT]
    return [pincode for pincode in pincodes if pincode.startswith(query)][:SUGGESTION_LIMIT]


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
    catalog = await get_suggestion_catalog()
    q = q.strip()
    return JSONResponse({"areas": ranked_area_matches(q, catalog["areas"])})


@app.get("/pincode-suggestions")
async def pincode_suggestions(q: str = Query("", min_length=0, max_length=10)):
    catalog = await get_suggestion_catalog()
    q = clean_pincode(q)
    return JSONResponse({"pincodes": ranked_pincode_matches(q, catalog["pincodes"])})


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

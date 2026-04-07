import os
import time
import pandas as pd
import googlemaps
from google.cloud import bigquery

PROJECT_ID = "learn-mcp-490919"
DATASET = "lease_lens"
AREA_TABLE = f"{PROJECT_ID}.{DATASET}.area_live_scores"

MAPS_API_KEY = os.getenv("MAPS_API_KEY")
if not MAPS_API_KEY:
    raise SystemExit("MAPS_API_KEY not set")

gmaps = googlemaps.Client(key=MAPS_API_KEY)
bq = bigquery.Client(project=PROJECT_ID)

OUT = "peer_competition_counts.csv"
KEYWORDS = {
    "salon_competitor_count": "salon",
    "pharmacy_competitor_count": "pharmacy",
    "grocery_competitor_count": "grocery store",
    "boutique_competitor_count": "boutique",
    "clinic_competitor_count": "clinic",
}


def clean_pincode(value):
    return "".join(ch for ch in str(value or "") if ch.isdigit())[:6]


def nearby_count(lat, lng, keyword):
    try:
        result = gmaps.places_nearby(
            location=(lat, lng),
            radius=1200,
            keyword=keyword,
        )
        return len(result.get("results", []))
    except Exception as exc:
        print(f"Failed keyword={keyword} lat={lat} lng={lng}: {exc}")
        return 0


def geocode_area(area_name, pincode):
    query = f"{area_name}, Bangalore, Karnataka, India {pincode}"
    try:
        results = gmaps.geocode(query)
        if results:
            loc = results[0]["geometry"]["location"]
            return loc["lat"], loc["lng"]
    except Exception as exc:
        print(f"Geocode failed for {area_name} {pincode}: {exc}")
    return None, None


def load_existing():
    if os.path.exists(OUT):
        return pd.read_csv(OUT)
    return pd.DataFrame(columns=[
        "area_name", "pincode",
        "salon_competitor_count",
        "pharmacy_competitor_count",
        "grocery_competitor_count",
        "boutique_competitor_count",
        "clinic_competitor_count",
    ])


query = f"""
SELECT
  area_name,
  REGEXP_EXTRACT(CAST(pincode AS STRING), r'(\\d{{6}})') AS pincode
FROM `{AREA_TABLE}`
WHERE area_name IS NOT NULL
  AND TRIM(area_name) != ''
  AND REGEXP_EXTRACT(CAST(pincode AS STRING), r'(\\d{{6}})') IS NOT NULL
  AND NOT REGEXP_CONTAINS(LOWER(area_name), r'(\\bso\\b|\\bbo\\b|\\bgpo\\b|\\bho\\b|post office|campus|extn|extension)')
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY LOWER(area_name)
  ORDER BY final_score DESC, scored_at DESC
) = 1
ORDER BY final_score DESC
LIMIT 60
"""


rows = list(bq.query(query).result())
done = load_existing()
done_keys = set(zip(done["area_name"], done["pincode"]))

records = done.to_dict("records")

for i, row in enumerate(rows, start=1):
    area_name = row.area_name
    pincode = clean_pincode(row.pincode)

    if (area_name, pincode) in done_keys:
        print(f"[{i}/{len(rows)}] Skip {area_name} {pincode}")
        continue

    print(f"[{i}/{len(rows)}] Processing {area_name} {pincode}")

    lat, lng = geocode_area(area_name, pincode)
    if lat is None or lng is None:
        print(f"Skipping {area_name} {pincode} due to missing geocode")
        continue

    record = {
        "area_name": area_name,
        "pincode": pincode,
    }

    for col, keyword in KEYWORDS.items():
        record[col] = nearby_count(lat, lng, keyword)
        time.sleep(0.2)

    records.append(record)
    pd.DataFrame(records).to_csv(OUT, index=False)
    print(f"Saved checkpoint for {area_name} {pincode}")

print(f"Done. Saved {len(records)} rows to {OUT}")

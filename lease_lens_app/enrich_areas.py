import os
from datetime import datetime, timezone

from google.cloud import bigquery
from google.maps import places_v1
from google.maps.places_v1.types import SearchTextRequest

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "learn-mcp-490919")
DATASET = "lease_lens"
CANDIDATE_TABLE = f"{PROJECT_ID}.{DATASET}.candidate_areas"
SCORES_TABLE = f"{PROJECT_ID}.{DATASET}.area_live_scores"


def safe_rating(value):
    return round(float(value), 2) if value else 0.0


def clamp_non_negative(value):
    return max(0.0, float(value))


def classify_positioning(avg_rating, office_count, family_count, competitor_count):
    if avg_rating >= 4.2 and office_count >= 8:
        return "premium"
    if family_count >= 8 and competitor_count <= 10:
        return "family"
    if competitor_count <= 6:
        return "budget"
    return "neighborhood"


def summary_text(area_name, positioning, competitor_count, office_count, family_count):
    return (
        f"{area_name} is best suited for a {positioning} bakery concept with "
        f"{competitor_count} nearby bakery competitors, office score {office_count}, "
        f"and family score {family_count}."
    )


def count_places_stub(area_name, place_type):
    # Temporary production-style scoring stub.
    # Replace with live Places calls if you want a deeper enrichment pass.
    seed = abs(hash(f"{area_name}:{place_type}")) % 100
    return (seed % 9) + 3


def rating_stub(area_name):
    seed = abs(hash(f"rating:{area_name}")) % 100
    return 3.8 + (seed % 12) / 10.0, 80 + seed * 3


def travel_stub(area_name, target):
    seed = abs(hash(f"{area_name}:{target}:traffic")) % 100
    return 18 + (seed % 35)


def compute_score(row):
    bakery_competitor_count = count_places_stub(row["area_name"], "bakery")
    cafe_competitor_count = count_places_stub(row["area_name"], "cafe")
    mall_count = count_places_stub(row["area_name"], "mall")
    office_count = count_places_stub(row["area_name"], "office")
    school_count = count_places_stub(row["area_name"], "school")
    metro_count = count_places_stub(row["area_name"], "metro")

    avg_rating, rating_count = rating_stub(row["area_name"])

    traffic_minutes_to_mg_road = travel_stub(row["area_name"], "MG Road")
    traffic_minutes_to_koramangala = travel_stub(row["area_name"], "Koramangala")
    traffic_minutes_to_whitefield = travel_stub(row["area_name"], "Whitefield")

    demand_score = (office_count * 1.5) + (mall_count * 1.2) + (school_count * 1.0) + (metro_count * 1.3)
    competition_penalty = (bakery_competitor_count * 1.6) + (cafe_competitor_count * 0.8)
    access_score = max(0.0, 25 - (traffic_minutes_to_mg_road * 0.3))
    quality_score = avg_rating * 4.0

    final_score = round(demand_score + access_score + quality_score - competition_penalty, 2)
    positioning = classify_positioning(avg_rating, office_count, school_count, bakery_competitor_count)
    summary = summary_text(
        row["area_name"],
        positioning,
        bakery_competitor_count,
        office_count,
        school_count,
    )

    return {
        "area_name": row["area_name"],
        "city": row["city"],
        "business_type": row["business_type"],
        "scored_at": datetime.now(timezone.utc).isoformat(),
        "bakery_competitor_count": bakery_competitor_count,
        "cafe_competitor_count": cafe_competitor_count,
        "mall_count": mall_count,
        "office_count": office_count,
        "school_count": school_count,
        "metro_count": metro_count,
        "avg_rating": safe_rating(avg_rating),
        "rating_count": rating_count,
        "traffic_minutes_to_mg_road": float(traffic_minutes_to_mg_road),
        "traffic_minutes_to_koramangala": float(traffic_minutes_to_koramangala),
        "traffic_minutes_to_whitefield": float(traffic_minutes_to_whitefield),
        "final_score": final_score,
        "positioning": positioning,
        "summary": summary,
    }


def main():
    client = bigquery.Client(project=PROJECT_ID)

    rows = client.query(
        f"""
        SELECT area_name, city, business_type, lat, lng
        FROM `{CANDIDATE_TABLE}`
        ORDER BY area_name
        """
    ).result()

    scored_rows = [compute_score(row) for row in rows]

    errors = client.insert_rows_json(SCORES_TABLE, scored_rows)
    if errors:
        print("Insert errors:", errors)
    else:
        print(f"Inserted {len(scored_rows)} rows into {SCORES_TABLE}")


if __name__ == "__main__":
    main()

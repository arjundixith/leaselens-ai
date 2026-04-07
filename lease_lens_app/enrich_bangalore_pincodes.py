import os
from datetime import datetime, timezone

from google.cloud import bigquery

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "learn-mcp-490919")
DATASET = "lease_lens"
PINCODE_TABLE = f"{PROJECT_ID}.{DATASET}.bangalore_pincodes"
SCORES_TABLE = f"{PROJECT_ID}.{DATASET}.area_live_scores"


def score_seed(text):
    return abs(hash(text)) % 100


def count_stub(label, area_name, pincode):
    return (score_seed(f"{label}:{area_name}:{pincode}") % 9) + 2


def rating_stub(area_name, pincode):
    seed = score_seed(f"rating:{area_name}:{pincode}")
    return round(3.8 + (seed % 13) / 10.0, 1), 80 + seed * 2


def travel_stub(area_name, pincode, dest):
    seed = score_seed(f"travel:{area_name}:{pincode}:{dest}")
    return 15 + (seed % 40)


def classify_positioning(avg_rating, office_count, school_count, competitor_count):
    if avg_rating >= 4.4 and office_count >= 8:
        return "premium"
    if school_count >= 8 and competitor_count <= 8:
        return "family"
    if competitor_count <= 5:
        return "budget"
    return "neighborhood"


def summarize(area_name, pincode, positioning, competitor_count, office_count, school_count):
    return (
        f"{area_name} ({pincode}) is suitable for a {positioning} retail format with "
        f"{competitor_count} nearby bakery competitors, office score {office_count}, "
        f"and school/family score {school_count}."
    )


def main():
    client = bigquery.Client(project=PROJECT_ID)

    rows = client.query(
        f"""
        SELECT area_name, pincode, city, lat, lng
        FROM `{PINCODE_TABLE}`
        ORDER BY pincode, area_name
        """
    ).result()

    scored_rows = []

    for row in rows:
        bakery_competitor_count = count_stub("bakery", row.area_name, row.pincode)
        cafe_competitor_count = count_stub("cafe", row.area_name, row.pincode)
        mall_count = count_stub("mall", row.area_name, row.pincode)
        office_count = count_stub("office", row.area_name, row.pincode)
        school_count = count_stub("school", row.area_name, row.pincode)
        metro_count = count_stub("metro", row.area_name, row.pincode)

        avg_rating, rating_count = rating_stub(row.area_name, row.pincode)

        traffic_minutes_to_mg_road = travel_stub(row.area_name, row.pincode, "MG Road")
        traffic_minutes_to_koramangala = travel_stub(row.area_name, row.pincode, "Koramangala")
        traffic_minutes_to_whitefield = travel_stub(row.area_name, row.pincode, "Whitefield")

        demand_score = (office_count * 1.5) + (mall_count * 1.2) + (school_count * 1.0) + (metro_count * 1.3)
        competition_penalty = (bakery_competitor_count * 1.6) + (cafe_competitor_count * 0.8)
        access_score = max(0.0, 25 - (traffic_minutes_to_mg_road * 0.25))
        quality_score = avg_rating * 4.0

        final_score = round(demand_score + access_score + quality_score - competition_penalty, 2)
        positioning = classify_positioning(avg_rating, office_count, school_count, bakery_competitor_count)
        summary = summarize(
            row.area_name,
            row.pincode,
            positioning,
            bakery_competitor_count,
            office_count,
            school_count,
        )

        scored_rows.append({
            "area_name": row.area_name,
            "pincode": row.pincode,
            "city": row.city,
            "business_type": "retail",
            "scored_at": datetime.now(timezone.utc).isoformat(),
            "bakery_competitor_count": bakery_competitor_count,
            "cafe_competitor_count": cafe_competitor_count,
            "mall_count": mall_count,
            "office_count": office_count,
            "school_count": school_count,
            "metro_count": metro_count,
            "avg_rating": avg_rating,
            "rating_count": rating_count,
            "traffic_minutes_to_mg_road": float(traffic_minutes_to_mg_road),
            "traffic_minutes_to_koramangala": float(traffic_minutes_to_koramangala),
            "traffic_minutes_to_whitefield": float(traffic_minutes_to_whitefield),
            "final_score": final_score,
            "positioning": positioning,
            "summary": summary,
        })

    errors = client.insert_rows_json(SCORES_TABLE, scored_rows)
    if errors:
        print("Insert errors:", errors)
    else:
        print(f"Inserted {len(scored_rows)} rows into {SCORES_TABLE}")


if __name__ == "__main__":
    main()

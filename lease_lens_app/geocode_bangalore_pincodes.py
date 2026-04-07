import os
import time
import pandas as pd
import googlemaps

MAPS_API_KEY = os.getenv("MAPS_API_KEY")
if not MAPS_API_KEY:
    raise SystemExit("MAPS_API_KEY not set")

gmaps = googlemaps.Client(key=MAPS_API_KEY)

df = pd.read_csv("bangalore_pincodes_raw.csv")
rows = []

for _, row in df.iterrows():
    query = f"{row['area_name']}, Bangalore, Karnataka, India, {row['pincode']}"
    try:
        results = gmaps.geocode(query)
        if results:
            loc = results[0]["geometry"]["location"]
            rows.append({
                "area_name": row["area_name"],
                "pincode": str(row["pincode"]),
                "city": "Bangalore",
                "lat": loc["lat"],
                "lng": loc["lng"],
            })
        time.sleep(0.1)
    except Exception as e:
        print("Failed:", query, e)

out = pd.DataFrame(rows).drop_duplicates(subset=["area_name", "pincode"])
out.to_csv("bangalore_pincodes_geocoded.csv", index=False)
print(f"Saved {len(out)} geocoded rows")
print(out.head(20).to_string(index=False))

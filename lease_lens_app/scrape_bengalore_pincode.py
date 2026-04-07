import re
from io import StringIO

import pandas as pd
import requests

URLS = [
    "https://calculator.name/pincode/karnataka/bangalore/bangalore",
    "https://www.pincodify.com/pincode/karnataka/bangalore/bangalore",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

OUT = "bangalore_pincodes_raw.csv"


def clean_text(value):
    value = "" if pd.isna(value) else str(value).strip()
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_table(df):
    cols = {str(c).strip().lower(): c for c in df.columns}
    locality_col = None
    pincode_col = None

    for candidate in ["locality/village", "locality", "village", "post office", "office name", "office"]:
        if candidate in cols:
            locality_col = cols[candidate]
            break

    for candidate in ["pin code", "pincode"]:
        if candidate in cols:
            pincode_col = cols[candidate]
            break

    if not locality_col or not pincode_col:
        return None

    out = df[[locality_col, pincode_col]].copy()
    out.columns = ["area_name", "pincode"]
    out["area_name"] = out["area_name"].map(clean_text)
    out["pincode"] = out["pincode"].astype(str).str.extract(r"(\d{6})", expand=False)
    out = out.dropna(subset=["pincode"])
    out = out[out["area_name"] != ""]
    return out


frames = []

for url in URLS:
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        tables = pd.read_html(StringIO(response.text))
        for table in tables:
            norm = normalize_table(table)
            if norm is not None and not norm.empty:
                frames.append(norm)
        print(f"Parsed {url}")
    except Exception as e:
        print(f"Failed to parse {url}: {e}")

if not frames:
    raise SystemExit("No tables found")

df = pd.concat(frames, ignore_index=True)
df["city"] = "Bangalore"
df["area_name"] = df["area_name"].str.replace(r"\s*\(.*?\)", "", regex=True).str.strip()
df = df.drop_duplicates(subset=["area_name", "pincode"]).sort_values(["pincode", "area_name"])

df.to_csv(OUT, index=False)
print(f"Saved {len(df)} rows to {OUT}")
print(df.head(20).to_string(index=False))

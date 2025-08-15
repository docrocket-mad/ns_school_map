# seed_cache_from_csv.py
import re, pandas as pd
from pathlib import Path

INPUT_CSV = "ns_schools_map_editable.csv"   # the file you edited with lat/lon
CACHE_OUT = "geocode_cache.csv"             # cache file v5 uses

POBOX_PAT = re.compile(r'\b(P\.?\s*O\.?\s*Box|PO\s*Box|Box\s+\d+)\b', re.I)
RR_PAT = re.compile(r'\bRR\s*\d+\b', re.I)
STN_PAT = re.compile(r'\bStn\.?\b', re.I)

def normalize_address(addr: str) -> str:
    if not isinstance(addr, str):
        return ""
    s = addr.strip()
    s = POBOX_PAT.sub("", s)
    s = RR_PAT.sub("", s)
    s = STN_PAT.sub("Station", s)
    s = re.sub(r"\s+", " ", s).strip(", ").strip()
    if "Nova Scotia" not in s and re.search(r"\bNS\b", s):
        s = s.replace(" NS", ", Nova Scotia")
    if "Nova Scotia" not in s:
        s += ", Nova Scotia"
    if "Canada" not in s:
        s += ", Canada"
    return s

# Load your edited CSV
df = pd.read_csv(INPUT_CSV)

# Try to find lat/lon columns (case-insensitive)
cols = {c.lower(): c for c in df.columns}
lat_col = cols.get("lat") or cols.get("latitude")
lon_col = cols.get("lon") or cols.get("longitude")

if not lat_col or not lon_col:
    raise SystemExit("Could not find 'lat' and 'lon' columns in ns_schools_map_editable.csv")

# coerce to numeric
df["__lat__"] = pd.to_numeric(df[lat_col], errors="coerce")
df["__lon__"] = pd.to_numeric(df[lon_col], errors="coerce")

# Build cache rows only for valid coords
rows = []
for _, r in df.iterrows():
    if pd.notna(r["__lat__"]) and pd.notna(r["__lon__"]):
        addr_raw = str(r.get("Address") or r.get("address") or "")
        norm = normalize_address(addr_raw)
        if norm:
            rows.append({"address": norm, "lat": float(r["__lat__"]), "lon": float(r["__lon__"])})

if not rows:
    raise SystemExit("No valid lat/lon found to seed cache. Double-check the numbers and column names.")

# If an existing cache exists, keep the best entries (prefer existing non-null)
cache_path = Path(CACHE_OUT)
if cache_path.exists():
    old = pd.read_csv(cache_path)
    old = old[["address","lat","lon"]]
    # merge: prefer old non-null, otherwise use new
    merged = pd.merge(pd.DataFrame(rows), old, on="address", how="outer", suffixes=("_new","_old"))
    merged["lat"] = merged.apply(lambda x: x["lat_old"] if pd.notna(x["lat_old"]) else x["lat_new"], axis=1)
    merged["lon"] = merged.apply(lambda x: x["lon_old"] if pd.notna(x["lon_old"]) else x["lon_new"], axis=1)
    cache = merged[["address","lat","lon"]]
else:
    cache = pd.DataFrame(rows)

cache.drop_duplicates(subset=["address"], keep="first").to_csv(CACHE_OUT, index=False)
print(f"Seeded {cache.shape[0]} cache entries to {CACHE_OUT}")

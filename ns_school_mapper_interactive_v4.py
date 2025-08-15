#!/usr/bin/env python3
"""
ns_school_mapper_interactive_v4.py

No-CDN, no-plugins version:
- Uses Leaflet CircleMarkers (native) instead of AwesomeMarkers.
- Live color changes via setStyle(...).
- Still outputs *.failed_geocodes.csv for addresses that don't pin.

Usage:
  python ns_school_mapper_interactive_v4.py --input "2024_2025 Elementary Schools.xlsx" --output "ns_schools_map_editable.html"

Optional flags:
  --min-delay-seconds 1.5
  --regeocode-failed
  --no-cache
  --max 0
"""

import argparse
import pandas as pd
import time
import re
from pathlib import Path
from typing import Optional, Tuple, Dict

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError

import folium

STATUS_COL = "Status"
COLOR = {
    "none": "#808080",   # gray
    "recent": "#1f77b4", # blue
    "active": "#2ca02c", # green
    "both": "#9467bd"    # purple
}

# ---------- Load workbook ----------

def load_workbook(input_path: Path, district_col: Optional[str]) -> pd.DataFrame:
    xls = pd.ExcelFile(input_path)
    frames = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(input_path, sheet_name=sheet)
        if district_col is None:
            df["District"] = sheet
        frames.append(df)
    if not frames:
        raise RuntimeError("No sheets found in workbook.")
    return pd.concat(frames, ignore_index=True)

# ---------- Address cleanup ----------

POBOX_PAT = re.compile(r'\b(P\.?\s*O\.?\s*Box|PO\s*Box|Box\s+\d+)\b', re.I)
RR_PAT = re.compile(r'\bRR\s*\d+\b', re.I)
STN_PAT = re.compile(r'\bStn\.?\b', re.I)

def normalize_address(addr: str) -> str:
    if not isinstance(addr, str):
        return ""
    s = addr.strip()
    s = POBOX_PAT.sub('', s)
    s = RR_PAT.sub('', s)
    s = STN_PAT.sub('Station', s)  # Cambridge Stn. -> Cambridge Station
    s = re.sub(r'\s+', ' ', s).strip(', ').strip()
    if "Nova Scotia" not in s and re.search(r'\bNS\b', s):
        s = s.replace(" NS", ", Nova Scotia")
    if "Nova Scotia" not in s:
        s += ", Nova Scotia"
    if "Canada" not in s:
        s += ", Canada"
    return s

# ---------- Cache helpers ----------

def load_cache(cache_path: Path):
    if cache_path.exists():
        df = pd.read_csv(cache_path)
        out = {}
        for _, row in df.iterrows():
            lat = row.get("lat"); lon = row.get("lon")
            out[row["address"]] = (lat if pd.notna(lat) else None, lon if pd.notna(lon) else None)
        return out
    return {}

def save_cache(cache_path: Path, cache):
    if not cache:
        return
    rows = [{"address": a, "lat": v[0], "lon": v[1]} for a, v in cache.items()]
    pd.DataFrame(rows).to_csv(cache_path, index=False)

# ---------- Geocoding ----------

def geocode_address(geocode_fn, address: str, retries: int = 3, backoff: float = 2.0):
    last_err = None
    for i in range(retries):
        try:
            loc = geocode_fn(address)
            if loc:
                return loc.latitude, loc.longitude
            return None, None
        except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError) as e:
            last_err = e
            time.sleep(backoff * (i + 1))
        except Exception as e:
            last_err = e
            break
    return None, None

# ---------- Status ----------

def derive_status(row: pd.Series) -> str:
    if STATUS_COL in row and isinstance(row[STATUS_COL], str) and row[STATUS_COL].strip():
        v = row[STATUS_COL].strip().lower()
        if v in {"none","recent","active","both"}:
            return v
    recent = str(row.get("Recent Relationship", "")).strip().lower() in {"1","true","yes","y"}
    active = str(row.get("Current Work", "")).strip().lower() in {"1","true","yes","y"}
    if recent and active: return "both"
    if active: return "active"
    if recent: return "recent"
    return "none"

# ---------- Map ----------

def build_map(df: pd.DataFrame, output_html: Path):
    m = folium.Map(location=[45.2, -62.99], zoom_start=7)

    for col in ["School","Address","District","Phone","Principal","E-Mail","Email",STATUS_COL,"lat","lon"]:
        if col not in df.columns:
            df[col] = ""

    js_records = []
    for idx, row in df.dropna(subset=["lat","lon"]).reset_index(drop=True).iterrows():
        school = row["School"]
        addr = row["Address"]
        district = row["District"]
        phone = row["Phone"] if isinstance(row["Phone"], str) else ""
        principal = row["Principal"] if isinstance(row["Principal"], str) else ""
        email = row["E-Mail"] if isinstance(row["E-Mail"], str) else (row["Email"] if isinstance(row["Email"], str) else "")
        status = row[STATUS_COL] if isinstance(row[STATUS_COL], str) else "none"
        color = COLOR.get(status, "#808080")

        popup_html = f"""
        <div style='min-width:280px'>
          <div style="font-weight:700">{school}</div>
          <div style="font-size:12px; margin:4px 0;">{addr}</div>
          <div style="font-size:12px; color:#666;">{district}</div>
          <div style="font-size:12px; margin-top:6px;">{phone}</div>
          <div style="font-size:12px;">{principal}</div>
          <div style="font-size:12px;">{email}</div>
          <hr/>
          <div style="font-size:12px; margin-bottom:6px;">Set status:</div>
          <div style="display:flex; gap:6px; flex-wrap:wrap;">
            <button onclick="window._setStatus({idx}, 'none')" style="padding:4px 8px">None</button>
            <button onclick="window._setStatus({idx}, 'recent')" style="padding:4px 8px">Recent</button>
            <button onclick="window._setStatus({idx}, 'active')" style="padding:4px 8px">Active</button>
            <button onclick="window._setStatus({idx}, 'both')" style="padding:4px 8px">Both</button>
          </div>
        </div>
        """

        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=7,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.9,
            weight=2,
            tooltip=school,
            popup=folium.Popup(popup_html, max_width=360)
        ).add_to(m)

        js_records.append({
            "lat": float(row["lat"]),
            "lon": float(row["lon"]),
            "Status": status,
            "School": school
        })

    import json
    js_data = json.dumps(js_records)

    legend = """
    <div style="position: fixed; bottom: 20px; left: 20px; z-index: 9999; background: white; padding: 10px 12px; border: 1px solid #ccc; border-radius: 8px; box-shadow: 0 2px 6px rgba(0,0,0,0.15); font-family: sans-serif; font-size: 13px;">
      <div style="font-weight:700; margin-bottom:6px;">Status Legend</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:#808080;margin-right:6px;border-radius:50%;"></span> None</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:#1f77b4;margin-right:6px;border-radius:50%;"></span> Recent</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:#2ca02c;margin-right:6px;border-radius:50%;"></span> Active</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:#9467bd;margin-right:6px;border-radius:50%;"></span> Both</div>
      <hr style="margin:8px 0;">
      <button onclick="window._downloadCSV()" style="padding:6px 10px; font-weight:600;">Download CSV</button>
    </div>
    """

    js = f"""
    <script>
    function findMap() {{
      for (var k in window) {{
        try {{ if (window[k] && window[k] instanceof L.Map) return window[k]; }} catch(e) {{}}
      }}
      return null;
    }}
    const COLOR={{none:"#808080",recent:"#1f77b4",active:"#2ca02c",both:"#9467bd"}};
    window._rows = {js_data};
    window._circleIndex = {{}};
    function keyFor(ll) {{ return ll.lat.toFixed(6)+","+ll.lng.toFixed(6); }}
    function collectCircles(map) {{
      window._circleIndex={{}};
      map.eachLayer(function(layer){{
        if (layer instanceof L.CircleMarker) {{
          const ll = layer.getLatLng();
          window._circleIndex[keyFor(ll)] = layer;
        }}
        if (layer && layer.getLayers) {{
          layer.getLayers().forEach(function(sub){{
            if (sub instanceof L.CircleMarker) {{
              const ll = sub.getLatLng();
              window._circleIndex[keyFor(ll)] = sub;
            }}
          }});
        }}
      }});
    }}
    function applyInitialColors() {{
      const map = findMap();
      if (!map) {{ setTimeout(applyInitialColors, 300); return; }}
      collectCircles(map);
      window._rows.forEach(function(r) {{
        const key = Number(r.lat).toFixed(6)+","+Number(r.lon).toFixed(6);
        const c = window._circleIndex[key];
        if (c) c.setStyle({{color: COLOR[r.Status] || "#808080", fillColor: COLOR[r.Status] || "#808080"}});
      }});
    }}
    window._setStatus = function(idx, status) {{
      if (!window._rows[idx]) return;
      window._rows[idx].Status = status;
      const key = Number(window._rows[idx].lat).toFixed(6)+","+Number(window._rows[idx].lon).toFixed(6);
      const c = window._circleIndex[key];
      if (c) c.setStyle({{color: COLOR[status] || "#808080", fillColor: COLOR[status] || "#808080"}});
    }}
    window._downloadCSV = function() {{
      if (!window._rows || !window._rows.length) return;
      const headers = Object.keys(window._rows[0]);
      const lines = [headers.join(",")];
      window._rows.forEach(function(row){{
        const vals = headers.map(function(h){{
          let v = row[h] == null ? "" : String(row[h]).replace(/"/g,'""');
          if (/[",\\n]/.test(v)) v = '"' + v + '"';
          return v;
        }});
        lines.push(vals.join(","));
      }});
      const csv = lines.join("\\n");
      const blob = new Blob([csv], {{type: "text/csv"}});
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url; a.download = "ns_schools_status.csv";
      document.body.appendChild(a); a.click(); document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }}
    setTimeout(applyInitialColors, 400);
    </script>
    """

    from branca.element import Element
    m.get_root().html.add_child(Element(legend))
    m.get_root().html.add_child(Element(js))
    m.save(str(output_html))

# ---------- Main ----------

def main():
    parser = argparse.ArgumentParser(description="Editable NS schools map (no-CDN colors).")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", default="ns_schools_map_editable.html")
    parser.add_argument("--address-col", default="Address")
    parser.add_argument("--school-col", default="School")
    parser.add_argument("--district-col", default=None)
    parser.add_argument("--cache", default="geocode_cache.csv")
    parser.add_argument("--max", type=int, default=0)
    parser.add_argument("--min-delay-seconds", type=float, default=1.1)
    parser.add_argument("--regeocode-failed", action="store_true")
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    output_html = Path(args.output).expanduser().resolve()
    cache_path = Path(args.cache).expanduser().resolve()

    df = load_workbook(input_path, args.district_col)

    for col in [args.address_col, args.school_col, (args.district_col or "District")]:
        if col not in df.columns:
            if col == (args.district_col or "District") and "District" in df.columns:
                continue
            raise KeyError(f"Required column '{col}' not found.")

    df["__full_addr__"] = df[args.address_col].astype(str).map(normalize_address)

    cache = {} if args.no_cache else load_cache(cache_path)
    geolocator = Nominatim(user_agent="ns_schools_mapper_v4")
    geocode_fn = RateLimiter(geolocator.geocode, min_delay_seconds=args.min_delay_seconds)

    if args.max > 0:
        df = df.head(args.max)

    lats, lons, failed = [], [], []
    for _, row in df.iterrows():
        addr = row["__full_addr__"]
        if not args.no_cache and addr in cache and cache[addr][0] is not None and cache[addr][1] is not None:
            lat, lon = cache[addr]
        else:
            lat, lon = geocode_address(geocode_fn, addr)
            if not args.no_cache:
                cache[addr] = (lat, lon)
        lats.append(lat); lons.append(lon)
        if lat is None or lon is None:
            failed.append({
                "School": row.get(args.school_col, ""),
                "Address": row.get(args.address_col, ""),
                "Normalized": addr
            })
        if not args.no_cache and len(cache) % 25 == 0:
            save_cache(cache_path, cache)

    if not args.no_cache:
        save_cache(cache_path, cache)

    df["lat"] = lats; df["lon"] = lons
    if failed:
        pd.DataFrame(failed).to_csv(output_html.with_suffix(".failed_geocodes.csv"), index=False)

    if STATUS_COL not in df.columns:
        df[STATUS_COL] = df.apply(derive_status, axis=1)
    else:
        df[STATUS_COL] = df[STATUS_COL].astype(str).str.lower().map(
            lambda s: s if s in {"none","recent","active","both"} else "none"
        )

    df.to_csv(output_html.with_suffix(".csv"), index=False)

    mapped = df.dropna(subset=["lat","lon"]).copy()
    build_map(mapped, output_html)

    print(f"Done. Map: {output_html}")
    if failed:
        print(f"Geocoding failures: {output_html.with_suffix('.failed_geocodes.csv')}")
    print(f"Reference data (CSV): {output_html.with_suffix('.csv')}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
ns_school_mapper_interactive_v3.py

Fixes & improvements:
- Robust Leaflet map detection (folium names the map var e.g. map_xxxxx).
- Colors change immediately on status update (AwesomeMarkers loaded and applied).
- Exports 'failed_geocodes.csv' so you can see which addresses need fixing.
- Better geocoding: strips PO Box / RR tokens before lookup.
- Cache control:
    --regeocode-failed  (retries addresses that are in cache with null coords)
    --no-cache          (ignores cache entirely for this run)

Usage:
  python ns_school_mapper_interactive_v3.py --input "2024_2025 Elementary Schools.xlsx" --output "ns_schools_map_editable.html"

Columns used if present: School, Address, District, Phone, Principal, E-Mail/Email, Status
Status values: none, recent, active, both
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
from folium.plugins import MarkerCluster

STATUS_COL = "Status"

# -------------------- Data loading --------------------

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

# -------------------- Address normalization --------------------

POBOX_PAT = re.compile(r'\b(P\.?\s*O\.?\s*Box|PO\s*Box|Box\s+\d+)\b', re.I)
RR_PAT = re.compile(r'\bRR\s*\d+\b', re.I)
STN_PAT = re.compile(r'\bStn\.?\b', re.I)

def normalize_address(addr: str) -> str:
    if not isinstance(addr, str):
        return ""
    s = addr.strip()
    # Remove tokens that confuse geocoders in Canada
    s = POBOX_PAT.sub('', s)
    s = RR_PAT.sub('', s)
    s = STN_PAT.sub('Station', s)  # Cambridge Stn. -> Cambridge Station
    s = re.sub(r'\s+', ' ', s).strip(', ').strip()
    # Ensure province & country
    if "Nova Scotia" not in s and re.search(r'\bNS\b', s):
        s = s.replace(" NS", ", Nova Scotia")
    if "Nova Scotia" not in s:
        s += ", Nova Scotia"
    if "Canada" not in s:
        s += ", Canada"
    return s

# -------------------- Cache helpers --------------------

def load_cache(cache_path: Path) -> Dict[str, Tuple[Optional[float], Optional[float]]]:
    if cache_path.exists():
        df = pd.read_csv(cache_path)
        out = {}
        for _, row in df.iterrows():
            lat = row.get("lat"); lon = row.get("lon")
            out[row["address"]] = (lat if pd.notna(lat) else None, lon if pd.notna(lon) else None)
        return out
    return {}

def save_cache(cache_path: Path, cache: Dict[str, Tuple[Optional[float], Optional[float]]]) -> None:
    if not cache:
        return
    rows = [{"address": a, "lat": v[0], "lon": v[1]} for a, v in cache.items()]
    pd.DataFrame(rows).to_csv(cache_path, index=False)

# -------------------- Geocoding --------------------

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

# -------------------- Status utilities --------------------

def derive_status(row: pd.Series) -> str:
    if STATUS_COL in row and isinstance(row[STATUS_COL], str) and row[STATUS_COL].strip():
        val = row[STATUS_COL].strip().lower()
        if val in {"none","recent","active","both"}:
            return val
    recent = str(row.get("Recent Relationship", "")).strip().lower() in {"1","true","yes","y"}
    active = str(row.get("Current Work", "")).strip().lower() in {"1","true","yes","y"}
    if recent and active: return "both"
    if active: return "active"
    if recent: return "recent"
    return "none"

# -------------------- Map build --------------------

def build_map_editable(df: pd.DataFrame, output_html: Path) -> None:
    m = folium.Map(location=[45.2, -62.99], zoom_start=7)
    cluster = MarkerCluster().add_to(m)

    # Ensure columns
    for col in ["School","Address","District",STATUS_COL,"lat","lon","Phone","Principal","E-Mail","Email"]:
        if col not in df.columns:
            df[col] = ""

    # Create vanilla markers; we'll upgrade to AwesomeMarkers in JS
    for idx, row in df.dropna(subset=["lat","lon"]).reset_index(drop=True).iterrows():
        school = row["School"]
        addr = row["Address"]
        district = row["District"]
        phone = row["Phone"] if isinstance(row["Phone"], str) else ""
        principal = row["Principal"] if isinstance(row["Principal"], str) else ""
        email = row["E-Mail"] if isinstance(row["E-Mail"], str) else (row["Email"] if isinstance(row["Email"], str) else "")
        status = row[STATUS_COL] if isinstance(row[STATUS_COL], str) else "none"

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
        folium.Marker(
            location=[row["lat"], row["lon"]],
            tooltip=school,
            popup=folium.Popup(popup_html, max_width=360)
        ).add_to(cluster)

    # Data payload for JS
    mapped = df.dropna(subset=["lat","lon"]).reset_index(drop=True).copy()
    records = mapped[["School","Address","District","lat","lon",STATUS_COL]].to_dict(orient="records")

    import json
    js_data = json.dumps(records)

    # Include AwesomeMarkers assets
    awesome_css = """
    <link rel="stylesheet" href="https://unpkg.com/leaflet.awesome-markers@2.0.5/dist/leaflet.awesome-markers.css">
    """
    awesome_js = """
    <script src="https://unpkg.com/leaflet.awesome-markers@2.0.5/dist/leaflet.awesome-markers.js"></script>
    """

    control_html = """
    <div id="legend" style="position: fixed; bottom: 20px; left: 20px; z-index: 9999; background: white; padding: 10px 12px; border: 1px solid #ccc; border-radius: 8px; box-shadow: 0 2px 6px rgba(0,0,0,0.15); font-family: sans-serif; font-size: 13px;">
      <div style="font-weight:700; margin-bottom:6px;">Status Legend</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:gray;margin-right:6px;border-radius:2px;"></span> None</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:blue;margin-right:6px;border-radius:2px;"></span> Recent</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:green;margin-right:6px;border-radius:2px;"></span> Active</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:purple;margin-right:6px;border-radius:2px;"></span> Both</div>
      <hr style="margin:8px 0;">
      <button onclick="window._downloadCSV()" style="padding:6px 10px; font-weight:600;">Download CSV</button>
    </div>
    """

    js_helpers = f"""
    <script>
    // Find the Leaflet map object that folium created (named like map_xxxxx)
    function findFoliumMap() {{
      for (var k in window) {{
        try {{
          if (window[k] && window[k] instanceof L.Map) return window[k];
        }} catch(e) {{ }}
      }}
      return null;
    }}

    function statusToColor(s) {{
      if (s === 'recent') return 'blue';
      if (s === 'active') return 'green';
      if (s === 'both') return 'purple';
      return 'gray';
    }}

    window._records = {js_data};
    window._markers = [];
    window._markerIndex = {{}};

    function collectMarkers(map) {{
      window._markers = [];
      window._markerIndex = {{}};
      map.eachLayer(function(layer) {{
        if (layer && layer.getLayers) {{
          layer.getLayers().forEach(function(sub) {{
            if (sub && sub.eachLayer) {{
              sub.eachLayer(function (m) {{
                if (m instanceof L.Marker) {{
                  window._markers.push(m);
                  var ll = m.getLatLng();
                  var key = ll.lat.toFixed(6)+','+ll.lng.toFixed(6);
                  window._markerIndex[key] = m;
                }}
              }});
            }}
          }});
        }}
      }});
    }}

    function applyInitialIcons() {{
      var map = findFoliumMap();
      if (!map || !L.AwesomeMarkers) {{ setTimeout(applyInitialIcons, 300); return; }}
      collectMarkers(map);
      // Convert all to AwesomeMarkers with current status colors
      window._records.forEach(function(rec) {{
        var key = Number(rec.lat).toFixed(6)+','+Number(rec.lon).toFixed(6);
        var marker = window._markerIndex[key];
        if (marker) {{
          var icon = L.AwesomeMarkers.icon({{
            icon: 'info-sign', prefix: 'glyphicon',
            markerColor: statusToColor(rec.{STATUS_COL})
          }});
          marker.setIcon(icon);
        }}
      }});
    }}

    window._setStatus = function(idx, status) {{
      if (!window._records[idx]) return;
      window._records[idx].{STATUS_COL} = status;
      var key = Number(window._records[idx].lat).toFixed(6)+','+Number(window._records[idx].lon).toFixed(6);
      var marker = window._markerIndex[key];
      if (marker && L.AwesomeMarkers) {{
        var icon = L.AwesomeMarkers.icon({{
          icon: 'info-sign', prefix: 'glyphicon',
          markerColor: statusToColor(status)
        }});
        marker.setIcon(icon);
      }}
    }}

    window._downloadCSV = function() {{
      if (!window._records || !window._records.length) return;
      var headers = Object.keys(window._records[0]);
      var lines = [headers.join(',')];
      window._records.forEach(function(row) {{
        var values = headers.map(function(h) {{
          var v = row[h] == null ? '' : String(row[h]).replace(/"/g,'""');
          if (v.search(/[",\\n]/) >= 0) v = '"' + v + '"';
          return v;
        }});
        lines.push(values.join(','));
      }});
      var csv = lines.join('\\n');
      var blob = new Blob([csv], {{type: 'text/csv'}});
      var url = URL.createObjectURL(blob);
      var a = document.createElement('a');
      a.href = url;
      a.download = 'ns_schools_status.csv';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }}

    // Load AwesomeMarkers & then apply
    setTimeout(applyInitialIcons, 600);
    </script>
    """

    from branca.element import Element
    m.get_root().header.add_child(Element(awesome_css))
    m.get_root().header.add_child(Element(awesome_js))
    m.get_root().html.add_child(Element(control_html))
    m.get_root().html.add_child(Element(js_helpers))
    m.save(str(output_html))

# -------------------- Main --------------------

def main():
    parser = argparse.ArgumentParser(description="Editable NS schools map with robust coloring and diagnostics.")
    parser.add_argument("--input", required=True, help="Excel workbook (tabs per district).")
    parser.add_argument("--output", default="ns_schools_map_editable.html", help="Output HTML map.")
    parser.add_argument("--address-col", default="Address", help="Column with full address.")
    parser.add_argument("--school-col", default="School", help="Column with school name.")
    parser.add_argument("--district-col", default=None, help="Column for district (defaults to sheet names).")
    parser.add_argument("--cache", default="geocode_cache.csv", help="CSV cache for geocoding.")
    parser.add_argument("--max", type=int, default=0, help="Limit rows for testing (0 = all).")
    parser.add_argument("--min-delay-seconds", type=float, default=1.1, help="Min delay between geocoding calls.")
    parser.add_argument("--regeocode-failed", action="store_true", help="Retry addresses that cached as null lat/lon.")
    parser.add_argument("--no-cache", action="store_true", help="Ignore cache for this run.")
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    output_html = Path(args.output).expanduser().resolve()
    cache_path = Path(args.cache).expanduser().resolve()

    df = load_workbook(input_path, args.district_col)

    # Check columns
    need_cols = [args.address_col, args.school_col, (args.district_col or "District")]
    for col in need_cols:
        if col not in df.columns:
            if col == (args.district_col or "District") and "District" in df.columns:
                continue
            raise KeyError(f"Required column '{col}' not found in workbook.")

    # Normalize addresses
    df["__full_addr__"] = df[args.address_col].astype(str).map(normalize_address)

    # Load cache
    cache = {} if args.no_cache else load_cache(cache_path)

    geolocator = Nominatim(user_agent="ns_schools_mapper_v3")
    geocode_fn = RateLimiter(geolocator.geocode, min_delay_seconds=args.min_delay_seconds)

    if args.max > 0:
        df = df.head(args.max)

    lats, lons = [], []
    failed = []

    for _, row in df.iterrows():
        addr = row["__full_addr__"]
        lat = lon = None

        use_cache = (not args.no_cache) and (addr in cache) and (cache[addr][0] is not None and cache[addr][1] is not None)
        failed_cached = (not args.no_cache) and (addr in cache) and (cache[addr][0] is None or cache[addr][1] is None)

        if use_cache:
            lat, lon = cache[addr]
        elif failed_cached and args.regeocode_failed:
            lat, lon = geocode_address(geocode_fn, addr)
            cache[addr] = (lat, lon)
        elif failed_cached and not args.regeocode_failed:
            lat, lon = cache[addr]
        else:
            lat, lon = geocode_address(geocode_fn, addr)
            cache[addr] = (lat, lon)

        lats.append(lat)
        lons.append(lon)
        if lat is None or lon is None:
            failed.append({
                "School": row.get(args.school_col, ""),
                "Address": row.get(args.address_col, ""),
                "Normalized": addr
            })

        if len(cache) % 25 == 0 and not args.no_cache:
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
    build_map_editable(mapped, output_html)

    print(f"Done. Map: {output_html}")
    print(f"Reference data (CSV): {output_html.with_suffix('.csv')}")
    if failed:
        print(f"Geocoding failures: {output_html.with_suffix('.failed_geocodes.csv')} (fix these addresses)")

if __name__ == "__main__":
    main()

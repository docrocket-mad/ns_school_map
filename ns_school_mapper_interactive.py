#!/usr/bin/env python3
# (trimmed header for brevity – full code same as previously provided)
import argparse
import pandas as pd
import time
from pathlib import Path
from typing import Optional, Tuple, Dict
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError
import folium
from folium.plugins import MarkerCluster

STATUS_COL = "Status"

def load_workbook(input_path: Path, district_col: Optional[str]) -> pd.DataFrame:
    xls = pd.ExcelFile(input_path)
    frames = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(input_path, sheet_name=sheet)
        if district_col is None:
            df["District"] = sheet
        frames.append(df)
    return pd.concat(frames, ignore_index=True)

def normalize_address(addr: str) -> str:
    if not isinstance(addr, str): return ""
    addr_std = addr.strip()
    if "Nova Scotia" not in addr_std and "NS" not in addr_std:
        addr_std += ", Nova Scotia"
    if "Canada" not in addr_std:
        addr_std += ", Canada"
    return addr_std

def load_cache(cache_path: Path):
    if cache_path.exists():
        df = pd.read_csv(cache_path)
        return {row["address"]: (row["lat"], row["lon"]) for _, row in df.iterrows()}
    return {}

def save_cache(cache_path: Path, cache):
    if not cache: return
    rows = [{"address": a, "lat": v[0], "lon": v[1]} for a, v in cache.items()]
    pd.DataFrame(rows).to_csv(cache_path, index=False)

def geocode_address(geocode_fn, address: str, retries: int = 3, backoff: float = 2.0):
    last_err = None
    for i in range(retries):
        try:
            loc = geocode_fn(address)
            if loc: return loc.latitude, loc.longitude
            return None, None
        except (GeocoderTimedOut, GeocoderUnavailable) as e:
            last_err = e; time.sleep(backoff * (i + 1))
        except Exception as e:
            last_err = e; break
    return None, None

def derive_status(row: pd.Series) -> str:
    if STATUS_COL in row and isinstance(row[STATUS_COL], str) and row[STATUS_COL].strip():
        val = row[STATUS_COL].strip().lower()
        if val in {"none","recent","active","both"}: return val
    recent = str(row.get("Recent Relationship","")).strip().lower() in {"1","true","yes","y"}
    active = str(row.get("Current Work","")).strip().lower() in {"1","true","yes","y"}
    if recent and active: return "both"
    if active: return "active"
    if recent: return "recent"
    return "none"

def icon_color(status: str) -> str:
    return {"none":"gray","recent":"blue","active":"green","both":"purple"}.get(status,"gray")

def build_map_editable(df: pd.DataFrame, output_html: Path) -> None:
    m = folium.Map(location=[45.2,-62.99], zoom_start=7)
    cluster = MarkerCluster().add_to(m)
    for col in ["School","Address","District",STATUS_COL,"lat","lon"]:
        if col not in df.columns: df[col] = ""
    for idx, row in df.dropna(subset=["lat","lon"]).reset_index(drop=True).iterrows():
        school, addr, district = row["School"], row["Address"], row["District"]
        status = row[STATUS_COL] if isinstance(row[STATUS_COL], str) else "none"
        col = icon_color(status)
        popup_html = f"""
        <div style='min-width:260px'>
          <div style="font-weight:600">{school}</div>
          <div style="font-size:12px; margin:4px 0;">{addr}</div>
          <div style="font-size:12px; color:#666;">{district}</div>
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
            popup=folium.Popup(popup_html, max_width=320),
            icon=folium.Icon(color=col)
        ).add_to(cluster)

    mapped = df.dropna(subset=["lat","lon"]).reset_index(drop=True).copy()
    records = mapped[["School","Address","District","lat","lon",STATUS_COL]].to_dict(orient="records")
    import json
    js_data = json.dumps(records)

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
    window._records = {js_data};
    window._markers = [];
    function collectMarkers() {{
      window._markers = [];
      map.eachLayer(function(layer) {{
        if (layer && layer.getLayers) {{
          layer.getLayers().forEach(function(sub) {{
            if (sub && sub.eachLayer) {{
              sub.eachLayer(function (m) {{
                if (m instanceof L.Marker) window._markers.push(m);
              }});
            }}
          }});
        }}
      }});
      window._markerIndex = {{}}
      window._markers.forEach(function(m) {{
        var ll = m.getLatLng();
        var key = ll.lat.toFixed(6)+','+ll.lng.toFixed(6);
        window._markerIndex[key] = m;
      }});
    }}
    function statusToColor(s) {{
      if (s === 'recent') return 'blue';
      if (s === 'active') return 'green';
      if (s === 'both') return 'purple';
      return 'gray';
    }}
    window._setStatus = function(idx, status) {{
      if (!window._records[idx]) return;
      window._records[idx].Status = status;
      var key = Number(window._records[idx].lat).toFixed(6)+','+Number(window._records[idx].lon).toFixed(6);
      var marker = window._markerIndex[key];
      if (marker) {{
        var icon = L.AwesomeMarkers.icon({{
          icon: 'info-sign',
          markerColor: statusToColor(status)
        }});
        marker.setIcon(icon);
      }}
    }}
    window._downloadCSV = function() {{
      if (!window._records || !window._records.length) return;
      var headers = Object.keys(window._records[0]);
      var lines = [];
      lines.push(headers.join(','));
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
    setTimeout(collectMarkers, 500);
    </script>
    """
    from branca.element import Element
    m.get_root().html.add_child(Element(control_html))
    m.get_root().html.add_child(Element(js_helpers))
    m.save(str(output_html))

def main():
    parser = argparse.ArgumentParser(description="Editable NS schools map with status colors.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", default="ns_schools_map_editable.html")
    parser.add_argument("--address-col", default="Address")
    parser.add_argument("--school-col", default="School")
    parser.add_argument("--district-col", default=None)
    parser.add_argument("--cache", default="geocode_cache.csv")
    parser.add_argument("--max", type=int, default=0)
    parser.add_argument("--min-delay-seconds", type=float, default=1.1)
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    output_html = Path(args.output).expanduser().resolve()
    cache_path = Path(args.cache).expanduser().resolve()

    df = load_workbook(input_path, args.district_col)
    for col in [args.address_col, args.school_col, (args.district_col or "District")]:
        if col not in df.columns:
            if col == (args.district_col or "District") and "District" in df.columns:
                continue
            raise KeyError(f"Required column '{col}' not found in workbook.")
    df["__full_addr__"] = df[args.address_col].astype(str).map(normalize_address)

    cache = load_cache(cache_path)
    geolocator = Nominatim(user_agent="ns_schools_mapper_editable")
    geocode_fn = RateLimiter(geolocator.geocode, min_delay_seconds=args.min_delay_seconds)

    lats, lons = [], []
    if args.max > 0:
        df = df.head(args.max)
    for addr in df["__full_addr__"]:
        if addr in cache:
            lat, lon = cache[addr]
        else:
            lat, lon = geocode_address(geocode_fn, addr)
            cache[addr] = (lat, lon)
            if len(cache) % 25 == 0: save_cache(cache_path, cache)
        lats.append(lat); lons.append(lon)
    save_cache(cache_path, cache)

    df["lat"] = lats; df["lon"] = lons
    if STATUS_COL not in df.columns:
        df[STATUS_COL] = df.apply(derive_status, axis=1)
    else:
        df[STATUS_COL] = df[STATUS_COL].astype(str).str.lower().map(lambda s: s if s in {"none","recent","active","both"} else "none")

    ref_csv = output_html.with_suffix(".csv"); df.to_csv(ref_csv, index=False)
    mapped = df.dropna(subset=["lat","lon"]).copy()
    build_map_editable(mapped, output_html)
    print(f"Done. Map: {output_html}")
    print(f"Reference data (CSV): {ref_csv}")
    print("Use the 'Download CSV' button on the map to export your edits.")

if __name__ == "__main__":
    main()

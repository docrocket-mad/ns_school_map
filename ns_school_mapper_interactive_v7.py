#!/usr/bin/env python3
"""
ns_school_mapper_interactive_v7.py

Adds:
- Add Mode with address geocoding (client-side via Nominatim) to place the pin at a typed address
- Delete pin from popup (removes from DATA and map; excluded from Download CSV)

Keeps:
- Filters (Status, District, optional Group/Board)
- Search + Zoom to first
- Add-on-map form (still can use click location if you skip geocoding)
- Status editing with live color updates
- NS-biased geocoding on the Python side for initial dataset build (cache + failed export)
- Honors existing lat/lon in your Excel

Run:
  python ns_school_mapper_interactive_v7.py --input "2024_2025 Elementary Schools.xlsx" --output "ns_schools_map_editable.html" --regeocode-failed --min-delay-seconds 2.0
"""

import argparse
import pandas as pd
import time
import re
from pathlib import Path
from typing import Optional

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

import folium

STATUS_COL = "Status"
GROUP_COLS = ["Group", "Board", "System"]
COLOR = {"none":"#808080","recent":"#1f77b4","active":"#2ca02c","both":"#9467bd"}

# ---------- Workbook ----------
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
    # Drop truly empty sheets/frames (future-proofing a pandas warning)
    frames = [f for f in frames if not (len(f.columns) == 0 or f.dropna(how="all").empty)]
    return pd.concat(frames, ignore_index=True)

# ---------- Address cleanup ----------
POBOX_PAT = re.compile(r'\b(P\.?\s*O\.?\s*Box|PO\s*Box|Box\s+\d+)\b', re.I)
RR_PAT = re.compile(r'\bRR\s*\d+\b', re.I)
STN_PAT = re.compile(r'\bStn\.?\b', re.I)
POSTAL_PAT = re.compile(r"\b[ABCEGHJ-NPRSTVXY]\d[ABCEGHJ-NPRSTV-Z]\s?\d[ABCEGHJ-NPRSTV-Z]\d\b", re.I)

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

# ---------- Cache ----------
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

# ---------- NS-biased geocoding (Python side) ----------
NS_VIEWBOX = {"west": -66.5, "south": 43.0, "east": -59.0, "north": 47.2}

def _geocode_once(geocode_fn, q: str):
    return geocode_fn(
        q,
        country_codes="ca",
        viewbox=((NS_VIEWBOX["south"], NS_VIEWBOX["west"]),
                 (NS_VIEWBOX["north"], NS_VIEWBOX["east"])),
        bounded=True,
        addressdetails=False,
        exactly_one=True,
        timeout=10,
    )

def geocode_address(geocode_fn, address: str, retries: int = 3, backoff: float = 2.0):
    variants = [address]
    no_postal = POSTAL_PAT.sub("", address).replace("  ", " ").strip(", ").strip()
    if no_postal != address:
        variants.append(no_postal)
    short = re.sub(r",\s*Nova Scotia.*", ", Nova Scotia, Canada", address)
    if short != address and short not in variants:
        variants.append(short)

    last_err = None
    for v in variants:
        for i in range(retries):
            try:
                loc = _geocode_once(geocode_fn, v)
                if loc:
                    return loc.latitude, loc.longitude
            except (GeocoderTimedOut, GeocoderUnavailable) as e:
                last_err = e
                time.sleep(backoff * (i + 1))
            except Exception as e:
                last_err = e
                time.sleep(backoff * (i + 1))
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

    # Which group/board column (if any)?
    group_col = None
    for gc in GROUP_COLS:
        if gc in df.columns:
            group_col = gc
            break

    cols = ["School","Address","District","lat","lon",STATUS_COL]
    if group_col: cols.append(group_col)

    mapped = df.dropna(subset=["lat","lon"]).reset_index(drop=True).copy()
    records = mapped[cols].to_dict(orient="records")

    import json
    js_data = json.dumps(records)
    districts = sorted([x for x in mapped["District"].dropna().unique()])
    js_districts = json.dumps(districts)
    groups = sorted([x for x in mapped[group_col].dropna().unique()]) if group_col else []
    js_groups = json.dumps(groups)
    group_key = group_col or ""

    # Controls UI
    controls_html = f"""
    <div id="panel" style="position: fixed; top: 20px; left: 20px; z-index: 9999; background: white; padding: 12px; border: 1px solid #ccc; border-radius: 10px; box-shadow: 0 2px 6px rgba(0,0,0,0.15); font-family: system-ui, sans-serif; font-size: 13px; width: 300px; max-height: 80vh; overflow:auto;">
      <div style="font-weight:700; font-size:14px; margin-bottom:8px;">Filters & Tools</div>

      <div style="margin-bottom:8px;">
        <div style="font-weight:600; margin-bottom:4px;">Status</div>
        <label><input type="checkbox" class="statusChk" value="none" checked> None</label><br>
        <label><input type="checkbox" class="statusChk" value="recent" checked> Recent</label><br>
        <label><input type="checkbox" class="statusChk" value="active" checked> Active</label><br>
        <label><input type="checkbox" class="statusChk" value="both" checked> Both</label>
      </div>

      <div style="margin-bottom:8px;">
        <div style="font-weight:600; margin-bottom:4px;">District</div>
        <select id="districtSel" multiple size="6" style="width:100%;"></select>
        <button id="districtClear" style="margin-top:6px;">Clear</button>
      </div>

      {"<div style='margin-bottom:8px;'><div style='font-weight:600; margin-bottom:4px;'>Group/Board</div><select id='groupSel' multiple size='5' style='width:100%;'></select><button id='groupClear' style='margin-top:6px;'>Clear</button></div>" if group_col else ""}

      <div style="margin-bottom:8px;">
        <div style="font-weight:600; margin-bottom:4px;">Search</div>
        <input id="searchBox" type="text" placeholder="School or address..." style="width:100%; padding:6px;">
        <div style="display:flex; gap:6px; margin-top:6px;">
          <button id="btnSearch">Apply</button>
          <button id="btnClearSearch">Clear</button>
          <button id="btnZoomFirst">Zoom to first</button>
        </div>
      </div>

      <hr style="margin:10px 0;">
      <div style="font-weight:700; margin-bottom:6px;">Add a School/Location</div>
      <div>Toggle “Add Mode” to type an address (or click the map). Geocode to place the pin at the typed address.</div>
      <div style="margin-top:6px; display:flex; flex-direction:column; gap:6px;">
        <button id="btnAddMode">Add Mode: OFF</button>
        <input id="addAddress" type="text" placeholder="123 Main St, Town, NS" style="width:100%; padding:6px;">
        <div style="display:flex; gap:6px;">
          <button id="btnGeocode">Geocode address</button>
          <button id="btnUseClick">Use last clicked</button>
        </div>
      </div>
      <div id="toast" style="display:none; margin-top:8px; padding:6px 8px; background:#e8f5e9; color:#2e7d32; border:1px solid #c8e6c9; border-radius:6px;">Saved!</div>

      <hr style="margin:10px 0;">
      <button id="btnDownload" style="width:100%; padding:8px; font-weight:700;">Download CSV</button>
    </div>

    <div id="legend" style="position: fixed; bottom: 20px; left: 20px; z-index: 9999; background: white; padding: 10px 12px; border: 1px solid #ccc; border-radius: 10px; box-shadow: 0 2px 6px rgba(0,0,0,0.15); font-family: system-ui, sans-serif; font-size: 13px;">
      <div style="font-weight:700; margin-bottom:6px;">Status Legend</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:{COLOR['none']};margin-right:6px;border-radius:50%;"></span> None</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:{COLOR['recent']};margin-right:6px;border-radius:50%;"></span> Recent</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:{COLOR['active']};margin-right:6px;border-radius:50%;"></span> Active</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:{COLOR['both']};margin-right:6px;border-radius:50%;"></span> Both</div>
    </div>
    """

    # ---- JS split: head (f-string) + body (plain) to avoid ${...} f-string collisions ----
    js_head = f"""
<script>
const COLOR={{none:"#808080",recent:"#1f77b4",active:"#2ca02c",both:"#9467bd"}};
const DATA={js_data};
const DISTRICTS={js_districts};
const GROUPS={js_groups};
const GROUP_KEY="{group_key}";
let addMode=false;

// Client-side geocoder (Nominatim) bounded to Nova Scotia
async function geocodeClient(q) {{
  if (!q) return null;
  const params = new URLSearchParams({{
    format: "jsonv2",
    q,
    countrycodes: "ca",
    viewbox: "-66.5,47.2,-59.0,43.0", // west,north,east,south
    bounded: "1",
    limit: "1",
    addressdetails: "0"
  }});
  const url = "https://nominatim.openstreetmap.org/search?" + params.toString();
  try {{
    const res = await fetch(url, {{
      headers: {{
        "Accept": "application/json"
      }}
    }});
    if (!res.ok) return null;
    const json = await res.json();
    if (Array.isArray(json) && json.length > 0) {{
      const r = json[0];
      const lat = Number(r.lat), lon = Number(r.lon);
      if (Number.isFinite(lat) && Number.isFinite(lon)) return {{lat, lon}};
    }}
  }} catch (_) {{}}
  return null;
}}

function findMap() {{
  for (var k in window) {{
    try {{ if (window[k] && window[k] instanceof L.Map) return window[k]; }} catch(e) {{}}
  }}
  return null;
}}

const districtSelInit = () => {{
  const sel = document.getElementById('districtSel');
  DISTRICTS.forEach(d => {{
    const opt = document.createElement('option');
    opt.value = d; opt.textContent = d; opt.selected = false;
    sel.appendChild(opt);
  }});
}};
const groupSelInit = () => {{
  if (!GROUP_KEY) return;
  const sel = document.getElementById('groupSel');
  GROUPS.forEach(g => {{
    const opt = document.createElement('option');
    opt.value = g; opt.textContent = g; opt.selected = false;
    sel.appendChild(opt);
  }});
}};

let markers = [];
let lastClickLL = null; // remember last map click
"""

    js_body = """
function addCircleFor(rec, idx, map) {
  const color = COLOR[rec.Status] || "#808080";
  const html = `
    <div style='min-width:280px'>
      <div style="font-weight:700">${rec.School || ''}</div>
      <div style="font-size:12px; margin:4px 0;">${rec.Address || ''}</div>
      <div style="font-size:12px; color:#666;">${rec.District || ''}</div>
      ${GROUP_KEY ? `<div style="font-size:12px; color:#666;">${rec[GROUP_KEY] || ''}</div>` : ``}
      <hr/>
      <div style="font-size:12px; margin-bottom:6px;">Set status:</div>
      <div style="display:flex; gap:6px; flex-wrap:wrap; margin-bottom:8px;">
        <button onclick="window._setStatus(${idx}, 'none')" style="padding:4px 8px">None</button>
        <button onclick="window._setStatus(${idx}, 'recent')" style="padding:4px 8px">Recent</button>
        <button onclick="window._setStatus(${idx}, 'active')" style="padding:4px 8px">Active</button>
        <button onclick="window._setStatus(${idx}, 'both')" style="padding:4px 8px">Both</button>
      </div>
      <div style="text-align:right;">
        <button onclick="window._delete(${idx})" style="padding:4px 8px; color:#b00020;">Delete</button>
      </div>
    </div>`;
  const cm = L.circleMarker([rec.lat, rec.lon], {
    radius: 7, color: color, fillColor: color, fillOpacity: 0.9, weight: 2
  }).bindPopup(html);
  cm.addTo(map);
  markers.push({marker: cm, rec});
}

function rebuildMarkers(map) {
  markers.forEach(m => map.removeLayer(m.marker));
  markers = [];
  DATA.forEach((rec, idx) => {
    if (Number.isFinite(rec.lat) && Number.isFinite(rec.lon)) {
      addCircleFor(rec, idx, map);
    }
  });
  applyFilters(map);
}

window._setStatus = function(idx, status) {
  if (!DATA[idx]) return;
  DATA[idx].Status = status;
  const m = markers[idx] && markers[idx].marker;
  if (m) m.setStyle({color: COLOR[status]||"#808080", fillColor: COLOR[status]||"#808080"});
  applyFilters(findMap());
}

window._delete = function(idx) {
  if (!DATA[idx]) return;
  DATA.splice(idx, 1); // remove record
  const map = findMap();
  rebuildMarkers(map); // rebuild to refresh idx bindings and popups
}

function getSelectedMulti(selId) {
  const sel = document.getElementById(selId);
  if (!sel) return [];
  return Array.from(sel.selectedOptions).map(o => o.value);
}
function getCheckedStatuses() {
  return Array.from(document.querySelectorAll('.statusChk'))
    .filter(x => x.checked)
    .map(x => x.value);
}

function applyFilters(map) {
  const statuses = new Set(getCheckedStatuses());
  const districts = new Set(getSelectedMulti('districtSel'));
  const groups = GROUP_KEY ? new Set(getSelectedMulti('groupSel')) : null;
  const q = (document.getElementById('searchBox').value || '').trim().toLowerCase();

  let firstVisible = null;

  markers.forEach(({marker, rec}) => {
    let show = statuses.has((rec.Status || 'none').toLowerCase());
    if (show && districts.size > 0) {
      show = districts.has(rec.District || '');
    }
    if (show && groups && groups.size > 0) {
      const gval = rec[GROUP_KEY] || '';
      show = groups.has(gval);
    }
    if (show && q) {
      const hay = ((rec.School||'') + ' ' + (rec.Address||'')).toLowerCase();
      show = hay.includes(q);
    }

    if (show) {
      marker.addTo(map);
      if (!firstVisible) firstVisible = marker;
    } else {
      map.removeLayer(marker);
    }
  });
  return firstVisible;
}

function zoomFirst() {
  const map = findMap();
  const first = applyFilters(map);
  if (first) {
    const ll = first.getLatLng();
    map.setView(ll, 14);
    first.openPopup();
  }
}

function downloadCSV() {
  if (!DATA || !DATA.length) return;
  const headers = Object.keys(DATA[0]);
  const lines = [headers.join(",")];
  DATA.forEach(row => {
    const vals = headers.map(h => {
      let v = row[h] == null ? "" : String(row[h]).replace(/"/g,'""');
      if (/[",\\n]/.test(v)) v = '"' + v + '"';
      return v;
    });
    lines.push(vals.join(","));
  });
  const blob = new Blob([lines.join("\\n")], {type:"text/csv"});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = "ns_schools_status.csv";
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function toggleAddMode() {
  addMode = !addMode;
  document.getElementById('btnAddMode').textContent = "Add Mode: " + (addMode ? "ON" : "OFF");
}

function showToast(msg="Saved!") {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.display = "block";
  setTimeout(() => t.style.display = "none", 1400);
}

function mapClickHandler(e) {
  if (!addMode) { lastClickLL = e.latlng; return; }
  lastClickLL = e.latlng;
  openAddForm(lastClickLL.lat, lastClickLL.lng, null, ""); // default to click location
}

function openAddForm(lat, lon, geocodedLL, usedAddress) {
  const form = document.createElement('div');
  form.style.cssText = "position:fixed; top:50%; left:50%; transform:translate(-50%,-50%); background:white; padding:12px; border:1px solid #ccc; border-radius:10px; z-index:10000; width:340px; box-shadow:0 2px 8px rgba(0,0,0,0.2); font-family:system-ui, sans-serif; font-size:13px;";
  form.innerHTML = `
    <div style="font-weight:700; margin-bottom:8px;">Add School/Location</div>
    <label>School<br><input id="f_school" style="width:100%" placeholder="Name"></label><br><br>
    <label>Address<br><input id="f_address" style="width:100%" placeholder="Street, Town, NS" value="${usedAddress || ''}"></label>
    <div style="display:flex; gap:6px; margin:6px 0 8px 0;">
      <button id="f_geocode">Geocode</button>
      <span id="f_geo_status" style="font-size:12px; color:#666;"></span>
    </div>
    <label>District<br><input id="f_district" style="width:100%" placeholder="e.g., HRCE"></label><br><br>
    ${GROUP_KEY ? '<label>Group/Board<br><input id="f_group" style="width:100%" placeholder="e.g., HRCE / AVRCE / CSAP / Private"></label><br><br>' : ''}
    <label>Status<br>
      <select id="f_status" style="width:100%">
        <option value="none">none</option>
        <option value="recent">recent</option>
        <option value="active">active</option>
        <option value="both">both</option>
      </select>
    </label><br><br>
    <div>Lat: <span id="f_lat">${Number(lat).toFixed(6)}</span> &nbsp; Lon: <span id="f_lon">${Number(lon).toFixed(6)}</span></div>
    <div style="display:flex; gap:8px; margin-top:10px; justify-content:flex-end;">
      <button id="f_cancel">Cancel</button>
      <button id="f_save">Save</button>
    </div>
  `;
  document.body.appendChild(form);

  const statusSpan = form.querySelector('#f_geo_status');

  form.querySelector('#f_geocode').onclick = async () => {
    const q = form.querySelector('#f_address').value || '';
    statusSpan.textContent = "Looking up…";
    const ll = await geocodeClient(q);
    if (ll) {
      form.querySelector('#f_lat').textContent = ll.lat.toFixed(6);
      form.querySelector('#f_lon').textContent = ll.lon.toFixed(6);
      statusSpan.textContent = "Found ✓";
    } else {
      statusSpan.textContent = "Not found (using map click)";
    }
  };

  form.querySelector('#f_cancel').onclick = () => form.remove();
  form.querySelector('#f_save').onclick = () => {
    const rec = {
      School: form.querySelector('#f_school').value || 'New Location',
      Address: form.querySelector('#f_address').value || '',
      District: form.querySelector('#f_district').value || '',
      lat: Number(form.querySelector('#f_lat').textContent),
      lon: Number(form.querySelector('#f_lon').textContent),
      Status: form.querySelector('#f_status').value || 'none'
    };
    if (GROUP_KEY) {
      const gInput = form.querySelector('#f_group');
      rec[GROUP_KEY] = gInput ? gInput.value : '';
    }
    const map = findMap();
    DATA.push(rec);
    addCircleFor(rec, DATA.length - 1, map);
    applyFilters(map);
    form.remove();
    showToast("Added!");
  };
}

function init() {
  const map = findMap();
  if (!map) { setTimeout(init, 300); return; }

  districtSelInit();
  groupSelInit();

  DATA.forEach((rec, idx) => {
    if (Number.isFinite(rec.lat) && Number.isFinite(rec.lon)) {
      addCircleFor(rec, idx, map);
    }
  });

  document.getElementById('districtClear').onclick = () => {
    const sel = document.getElementById('districtSel');
    Array.from(sel.options).forEach(o => o.selected = false);
    applyFilters(map);
  };
  if (GROUP_KEY) {
    const gc = document.getElementById('groupClear');
    if (gc) gc.onclick = () => {
      const sel = document.getElementById('groupSel');
      Array.from(sel.options).forEach(o => o.selected = false);
      applyFilters(map);
    };
  }

  Array.from(document.querySelectorAll('.statusChk')).forEach(cb => cb.addEventListener('change', () => applyFilters(map)));
  document.getElementById('districtSel').addEventListener('change', () => applyFilters(map));
  if (GROUP_KEY && document.getElementById('groupSel')) document.getElementById('groupSel').addEventListener('change', () => applyFilters(map));
  document.getElementById('btnSearch').onclick = () => applyFilters(map);
  document.getElementById('btnClearSearch').onclick = () => { document.getElementById('searchBox').value=''; applyFilters(map); };
  document.getElementById('btnZoomFirst').onclick = () => zoomFirst();
  document.getElementById('btnDownload').onclick = () => downloadCSV();

  document.getElementById('btnAddMode').onclick = () => toggleAddMode();
  map.on('click', mapClickHandler);

  // Add-mode helpers
  document.getElementById('btnGeocode').onclick = async () => {
    if (!addMode) { toggleAddMode(); }
    const addr = (document.getElementById('addAddress').value || '').trim();
    if (!addr) { document.getElementById('addAddress').focus(); return; }
    const ll = await geocodeClient(addr);
    if (ll) {
      openAddForm(ll.lat, ll.lon, ll, addr);
      map.setView([ll.lat, ll.lon], 14);
    } else {
      // if geocode fails, open at last click if we have it
      const base = lastClickLL ? {lat: lastClickLL.lat, lon: lastClickLL.lng} : {lat: map.getCenter().lat, lon: map.getCenter().lng};
      openAddForm(base.lat, base.lon, null, addr);
    }
  };
  document.getElementById('btnUseClick').onclick = () => {
    if (!addMode) { toggleAddMode(); }
    const base = lastClickLL ? {lat: lastClickLL.lat, lon: lastClickLL.lng} : {lat: map.getCenter().lat, lon: map.getCenter().lng};
    const addr = (document.getElementById('addAddress').value || '').trim();
    openAddForm(base.lat, base.lon, null, addr);
  };

  applyFilters(map);
}

setTimeout(init, 400);
</script>
"""

    # Build group field placeholder used inside JS form (already inside js_body template)
    # (no need here because js_body constructs form itself)

    from branca.element import Element
    m.get_root().html.add_child(Element(controls_html))
    m.get_root().html.add_child(Element(js_head + js_body))
    m.save(str(output_html))

# ---------- Main ----------
def main():
    parser = argparse.ArgumentParser(description="Editable NS schools map (filters, search, add-on-map + address geocode + delete).")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", default="ns_schools_map_editable.html")
    parser.add_argument("--address-col", default="Address")
    parser.add_argument("--school-col", default="School")
    parser.add_argument("--district-col", default=None)
    parser.add_argument("--cache", default="geocode_cache.csv")
    parser.add_argument("--max", type=int, default=0)
    parser.add_argument("--min-delay-seconds", type=float, default=1.5)
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
    geolocator = Nominatim(user_agent="ns_schools_mapper_v7")
    geocode_fn = RateLimiter(geolocator.geocode, min_delay_seconds=args.min_delay_seconds, swallow_exceptions=False)

    if args.max > 0:
        df = df.head(args.max)

    # Honor existing lat/lon if present; otherwise use cache/geocode
    has_latlon = ("lat" in df.columns) and ("lon" in df.columns)
    lats, lons, failed = [], [], []
    for _, row in df.iterrows():
        addr = row["__full_addr__"]

        prev_lat = prev_lon = None
        if has_latlon:
            try:
                prev_lat = float(row["lat"])
                prev_lon = float(row["lon"])
            except Exception:
                prev_lat = prev_lon = None

        if prev_lat is not None and prev_lon is not None:
            lat, lon = prev_lat, prev_lon
            if not args.no_cache:
                cache[addr] = (lat, lon)
        elif (not args.no_cache) and addr in cache and cache[addr][0] is not None and cache[addr][1] is not None:
            lat, lon = cache[addr]
        else:
            if (not args.no_cache) and (addr in cache) and (cache[addr][0] is None or cache[addr][1] is None) and (not args.regeocode_failed):
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
        df[STATUS_COL] = df[STATUS_COL].astype(str).str.lower().map(lambda s: s if s in {"none","recent","active","both"} else "none")

    df.to_csv(output_html.with_suffix(".csv"), index=False)

    mapped = df.dropna(subset=["lat","lon"]).copy()
    build_map(mapped, output_html)

    print(f"Done. Map: {output_html}")
    if failed:
        print(f"Geocoding failures: {output_html.with_suffix('.failed_geocodes.csv')}")
    print(f"Reference data (CSV): {output_html.with_suffix('.csv')}")

if __name__ == "__main__":
    main()

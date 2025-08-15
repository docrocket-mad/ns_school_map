#!/usr/bin/env python3
"""
ns_school_mapper_interactive_v9.py

Adds:
- Toggleable List View synchronized with filters/search.
- Clicking a school in the list zooms to its pin and opens the popup.

Keeps:
- Status values: None / Recent / Current (legacy mapped automatically)
- Group filter (replaces District). Custom groups on add.
- Search, Zoom to first, Download CSV
- Add mode: type address (client geocode) or map click
- Delete with Undo
- Move pin (click "Move pin", then click map)
- NS-biased Python geocoding for initial dataset + cache + failed export

Run:
  python ns_school_mapper_interactive_v9.py --input "2024_2025 Elementary Schools.xlsx" --output "ns_schools_map_editable.html" --regeocode-failed --min-delay-seconds 2.0
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

# ---- Config ----
STATUS_COL = "Status"
COLOR = {"none":"#808080","recent":"#1f77b4","current":"#2ca02c"}
GROUP_CANDIDATES = ["Group", "Board", "System", "District"]

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

# ---------- Workbook ----------
def load_workbook(input_path: Path, district_col: Optional[str]) -> pd.DataFrame:
    xls = pd.ExcelFile(input_path)
    frames = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(input_path, sheet_name=sheet)
        if district_col is None and "District" not in df.columns:
            df["District"] = sheet
        frames.append(df)
    if not frames:
        raise RuntimeError("No sheets found in workbook.")
    frames = [f for f in frames if not (len(f.columns) == 0 or f.dropna(how="all").empty)]
    return pd.concat(frames, ignore_index=True)

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

# ---------- NS-biased geocoding ----------
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
def normalize_status_value(val) -> str:
    v = str(val).strip().lower()
    if v in {"current", "active", "both"}: return "current"
    if v == "recent": return "recent"
    return "none"

def derive_status(row: pd.Series) -> str:
    raw = str(row.get(STATUS_COL, "")).strip().lower()
    if raw:
        return normalize_status_value(raw)
    recent = str(row.get("Recent Relationship", "")).strip().lower() in {"1","true","yes","y"}
    active = str(row.get("Current Work", "")).strip().lower() in {"1","true","yes","y"}
    if active: return "current"
    if recent: return "recent"
    return "none"

# ---------- Group ----------
def pick_group_column(df: pd.DataFrame) -> str:
    for c in GROUP_CANDIDATES:
        if c in df.columns:
            return c
    if "District" in df.columns:
        df["Group"] = df["District"]
        return "Group"
    df["Group"] = ""
    return "Group"

# ---------- Map ----------
def build_map(df: pd.DataFrame, output_html: Path):
    m = folium.Map(location=[45.2, -62.99], zoom_start=7)

    for col in ["School","Address","lat","lon"]:
        if col not in df.columns:
            df[col] = ""

    group_col = pick_group_column(df)

    if STATUS_COL not in df.columns:
        df[STATUS_COL] = df.apply(derive_status, axis=1)
    else:
        df[STATUS_COL] = df[STATUS_COL].apply(normalize_status_value)

    cols = ["School","Address",group_col,"lat","lon",STATUS_COL]
    mapped = df.dropna(subset=["lat","lon"]).reset_index(drop=True).copy()
    records = mapped[cols].rename(columns={group_col: "Group"}).to_dict(orient="records")

    import json
    js_data = json.dumps(records)
    groups = sorted([x for x in mapped[group_col].dropna().astype(str).unique() if str(x).strip() != ""])
    js_groups = json.dumps(groups)

    # ---------- Controls + List UI ----------
    controls_html = f"""
    <div id="panel" style="position: fixed; top: 20px; left: 20px; z-index: 9999; background: white; padding: 12px; border: 1px solid #ccc; border-radius: 10px; box-shadow: 0 2px 6px rgba(0,0,0,0.15); font-family: system-ui, sans-serif; font-size: 13px; width: 340px; max-height: 84vh; overflow:auto;">
      <div style="display:flex; justify-content:space-between; align-items:center; gap:8px;">
        <div style="font-weight:700; font-size:14px;">Filters & Tools</div>
        <button id="btnToggleList">List View: OFF</button>
      </div>

      <div id="listContainer" style="display:none; margin:10px 0; border:1px solid #ddd; border-radius:8px; max-height:220px; overflow:auto;">
        <div style="padding:6px 8px; font-weight:600; border-bottom:1px solid #eee;">Schools</div>
        <div id="schoolList" style="max-height:180px; overflow:auto;"></div>
      </div>

      <div style="margin-bottom:8px;">
        <div style="font-weight:600; margin-bottom:4px;">Status</div>
        <label><input type="checkbox" class="statusChk" value="none" checked> None</label><br>
        <label><input type="checkbox" class="statusChk" value="recent" checked> Recent</label><br>
        <label><input type="checkbox" class="statusChk" value="current" checked> Current</label>
      </div>

      <div style="margin-bottom:8px;">
        <div style="font-weight:600; margin-bottom:4px;">Group</div>
        <select id="groupSel" multiple size="7" style="width:100%;"></select>
        <div style="display:flex; gap:6px; margin-top:6px;">
          <button id="groupClear">Clear</button>
          <button id="groupSelectAll">Select all</button>
        </div>
      </div>

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

      <hr style="margin:10px 0;">
      <div style="display:flex; gap:6px; align-items:center;">
        <button id="btnUndoDelete" title="Undo the most recent delete">Undo Delete</button>
        <button id="btnDownload" style="flex:1; font-weight:700;">Download CSV</button>
      </div>

      <div id="toast" style="display:none; margin-top:8px; padding:6px 8px; background:#e8f5e9; color:#2e7d32; border:1px solid #c8e6c9; border-radius:6px;">Saved!</div>
    </div>

    <div id="legend" style="position: fixed; bottom: 20px; left: 20px; z-index: 9999; background: white; padding: 10px 12px; border: 1px solid #ccc; border-radius: 10px; box-shadow: 0 2px 6px rgba(0,0,0,0.15); font-family: system-ui, sans-serif; font-size: 13px;">
      <div style="font-weight:700; margin-bottom:6px;">Status Legend</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:{COLOR['none']};margin-right:6px;border-radius:50%;"></span> None</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:{COLOR['recent']};margin-right:6px;border-radius:50%;"></span> Recent</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:{COLOR['current']};margin-right:6px;border-radius:50%;"></span> Current</div>
    </div>
    """

    # ---- JS head (needs Python vars) + body (plain string so ${...} is safe) ----
    js_head = f"""
<script>
const COLOR={{none:"#808080",recent:"#1f77b4",current:"#2ca02c"}};
const DATA={js_data};  // {{School, Address, Group, lat, lon, Status}}
const GROUPS={js_groups};
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
    const res = await fetch(url, {{ headers: {{ "Accept": "application/json" }} }});
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
"""

    js_body = r"""
let markers = [];           // [{marker, rec}]
let lastClickLL = null;
let UNDO_STACK = [];        // {type:'delete', rec, idx}
let moveIndex = null;       // when set, next map click moves that pin
let LIST_ON = false;

// ---------- Helpers ----------
function normalizeStatus(s) {
  const v = String(s || '').trim().toLowerCase();
  if (v === 'recent') return 'recent';
  if (v === 'current' || v === 'active' || v === 'both') return 'current';
  return 'none';
}

function groupSelInit() {
  const sel = document.getElementById('groupSel');
  sel.innerHTML = '';
  const uniq = new Set((GROUPS || []).filter(Boolean).map(x => String(x)));
  DATA.forEach(r => { if (r.Group && String(r.Group).trim() !== '') uniq.add(String(r.Group)); });
  Array.from(uniq).sort().forEach(g => {
    const opt = document.createElement('option');
    opt.value = g; opt.textContent = g; opt.selected = false;
    sel.appendChild(opt);
  });
}

function visiblePredicate(rec) {
  const statuses = new Set(Array.from(document.querySelectorAll('.statusChk'))
    .filter(x => x.checked).map(x => x.value));
  const groupsSel = document.getElementById('groupSel');
  const groups = new Set(groupsSel ? Array.from(groupsSel.selectedOptions).map(o => o.value) : []);
  const q = (document.getElementById('searchBox').value || '').trim().toLowerCase();

  let show = statuses.has(normalizeStatus(rec.Status));
  if (show && groups.size > 0) {
    show = groups.has(rec.Group || '');
  }
  if (show && q) {
    const hay = ((rec.School||'') + ' ' + (rec.Address||'')).toLowerCase();
    show = hay.includes(q);
  }
  return show;
}

// ---------- Markers ----------
function addCircleFor(rec, idx, map) {
  const color = COLOR[normalizeStatus(rec.Status)];
  const html = `
    <div style='min-width:280px'>
      <div style="font-weight:700">${rec.School || ''}</div>
      <div style="font-size:12px; margin:4px 0;">${rec.Address || ''}</div>
      <div style="font-size:12px; color:#666;">${rec.Group || ''}</div>
      <hr/>
      <div style="font-size:12px; margin-bottom:6px;">Set status:</div>
      <div style="display:flex; gap:6px; flex-wrap:wrap; margin-bottom:8px;">
        <button onclick="window._setStatus(${idx}, 'none')" style="padding:4px 8px">None</button>
        <button onclick="window._setStatus(${idx}, 'recent')" style="padding:4px 8px">Recent</button>
        <button onclick="window._setStatus(${idx}, 'current')" style="padding:4px 8px">Current</button>
      </div>
      <div style="display:flex; gap:6px; justify-content:flex-end;">
        <button onclick="window._move(${idx})" style="padding:4px 8px;">Move pin</button>
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

// ---------- Status / Delete / Move ----------
window._setStatus = function(idx, status) {
  if (!DATA[idx]) return;
  DATA[idx].Status = normalizeStatus(status);
  const m = markers[idx] && markers[idx].marker;
  if (m) m.setStyle({color: COLOR[DATA[idx].Status], fillColor: COLOR[DATA[idx].Status]});
  const map = findMap();
  applyFilters(map);
};

window._delete = function(idx) {
  if (!DATA[idx]) return;
  const rec = DATA[idx];
  UNDO_STACK.push({type:'delete', rec: {...rec}, idx});
  DATA.splice(idx, 1); // remove
  const map = findMap();
  rebuildMarkers(map); // rebind indexes
  showToast("Deleted. Click Undo Delete to restore.");
};

window._move = function(idx) {
  moveIndex = idx;
  showToast("Move mode: click on the map to set new location.");
};

// ---------- Filters / Search ----------
function applyFilters(map) {
  let firstVisible = null;
  markers.forEach(({marker, rec}) => {
    const show = visiblePredicate(rec);
    if (show) {
      marker.addTo(map);
      if (!firstVisible) firstVisible = marker;
    } else {
      map.removeLayer(marker);
    }
  });
  rebuildList(); // keep list synced
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

// ---------- List View ----------
function toggleList() {
  LIST_ON = !LIST_ON;
  const btn = document.getElementById('btnToggleList');
  const box = document.getElementById('listContainer');
  btn.textContent = "List View: " + (LIST_ON ? "ON" : "OFF");
  box.style.display = LIST_ON ? "block" : "none";
  if (LIST_ON) rebuildList();
}

function rebuildList() {
  const wrap = document.getElementById('schoolList');
  if (!wrap) return;
  wrap.innerHTML = '';
  if (!LIST_ON) return;

  // Build filtered set with their current indices
  const rows = [];
  DATA.forEach((rec, idx) => {
    if (!Number.isFinite(rec.lat) || !Number.isFinite(rec.lon)) return;
    if (!visiblePredicate(rec)) return;
    rows.push({rec, idx});
  });

  // Sort A→Z by School name
  rows.sort((a, b) => String(a.rec.School||'').localeCompare(String(b.rec.School||'')));

  if (!rows.length) {
    const empty = document.createElement('div');
    empty.textContent = "No schools match the current filters.";
    empty.style.cssText = "padding:8px; color:#666;";
    wrap.appendChild(empty);
    return;
  }

  rows.forEach(({rec, idx}) => {
    const item = document.createElement('div');
    item.style.cssText = "padding:8px; border-bottom:1px solid #eee; cursor:pointer; display:flex; justify-content:space-between; align-items:center; gap:8px;";
    const left = document.createElement('div');
    left.innerHTML = `<div style="font-weight:600">${rec.School || ''}</div>
                      <div style="font-size:12px; color:#666;">${rec.Group || ''}</div>
                      <div style="font-size:12px; color:#666;">${rec.Address || ''}</div>`;
    const dot = document.createElement('span');
    dot.style.cssText = `display:inline-block;width:10px;height:10px;border-radius:50%;background:${COLOR[normalizeStatus(rec.Status)]}; flex:0 0 auto;`;
    item.appendChild(left);
    item.appendChild(dot);
    item.onclick = () => {
      const map = findMap();
      const m = markers[idx] && markers[idx].marker;
      if (m) {
        const ll = m.getLatLng();
        map.setView(ll, 14);
        m.openPopup();
      }
    };
    wrap.appendChild(item);
  });
}

// ---------- Download ----------
function downloadCSV() {
  if (!DATA || !DATA.length) return;
  const headers = ["School","Address","Group","lat","lon","Status"];
  const lines = [headers.join(",")];
  DATA.forEach(row => {
    const vals = headers.map(h => {
      let v = row[h] == null ? "" : String(row[h]).replace(/"/g,'""');
      if (/[",\n]/.test(v)) v = '"' + v + '"';
      return v;
    });
    lines.push(vals.join(","));
  });
  const blob = new Blob([lines.join("\n")], {type:"text/csv"});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = "ns_schools_status.csv";
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ---------- Add Mode / Geocode / Move / Undo ----------
function toggleAddMode() {
  addMode = !addMode;
  document.getElementById('btnAddMode').textContent = "Add Mode: " + (addMode ? "ON" : "OFF");
}

function showToast(msg="Saved!") {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.display = "block";
  setTimeout(() => t.style.display = "none", 1600);
}

function mapClickHandler(e) {
  lastClickLL = e.latlng;

  const map = findMap();
  if (moveIndex !== null) {
    const idx = moveIndex;
    moveIndex = null;
    if (DATA[idx]) {
      DATA[idx].lat = Number(e.latlng.lat.toFixed(6));
      DATA[idx].lon = Number(e.latlng.lng.toFixed(6));
      const m = markers[idx] && markers[idx].marker;
      if (m) m.setLatLng(e.latlng);
      applyFilters(map);
      showToast("Moved.");
    }
    return;
  }

  if (!addMode) return;
  openAddForm(lastClickLL.lat, lastClickLL.lng, null, "", "");
}

async function doGeocodeToForm(form) {
  const q = form.querySelector('#f_address').value || '';
  const statusSpan = form.querySelector('#f_geo_status');
  statusSpan.textContent = "Looking up…";
  const ll = await geocodeClient(q);
  if (ll) {
    form.querySelector('#f_lat').textContent = ll.lat.toFixed(6);
    form.querySelector('#f_lon').textContent = ll.lon.toFixed(6);
    statusSpan.textContent = "Found ✓";
  } else {
    statusSpan.textContent = "Not found (using map/cursor)";
  }
}

function openAddForm(lat, lon, geocodedLL, usedAddress, usedGroup) {
  const form = document.createElement('div');
  form.style.cssText = "position:fixed; top:50%; left:50%; transform:translate(-50%,-50%); background:white; padding:12px; border:1px solid #ccc; border-radius:10px; z-index:10000; width:360px; box-shadow:0 2px 8px rgba(0,0,0,0.2); font-family:system-ui, sans-serif; font-size:13px;";
  form.innerHTML = `
    <div style="font-weight:700; margin-bottom:8px;">Add School/Location</div>
    <label>School<br><input id="f_school" style="width:100%" placeholder="Name"></label><br><br>
    <label>Address<br><input id="f_address" style="width:100%" placeholder="Street, Town, NS" value="${usedAddress || ''}"></label>
    <div style="display:flex; gap:6px; margin:6px 0 8px 0;">
      <button id="f_geocode">Geocode</button>
      <span id="f_geo_status" style="font-size:12px; color:#666;"></span>
    </div>
    <label>Group<br><input id="f_group" style="width:100%" placeholder="e.g., HRCE / AVRCE / CSAP / Private" value="${usedGroup || ''}"></label><br><br>
    <label>Status<br>
      <select id="f_status" style="width:100%">
        <option value="none">None</option>
        <option value="recent">Recent</option>
        <option value="current">Current</option>
      </select>
    </label><br><br>
    <div>Lat: <span id="f_lat">${Number(lat).toFixed(6)}</span> &nbsp; Lon: <span id="f_lon">${Number(lon).toFixed(6)}</span></div>
    <div style="display:flex; gap:8px; margin-top:10px; justify-content:flex-end;">
      <button id="f_cancel">Cancel</button>
      <button id="f_save">Save</button>
    </div>
  `;
  document.body.appendChild(form);

  form.querySelector('#f_geocode').onclick = () => { doGeocodeToForm(form); };
  form.querySelector('#f_cancel').onclick = () => form.remove();
  form.querySelector('#f_save').onclick = () => {
    const rec = {
      School: form.querySelector('#f_school').value || 'New Location',
      Address: form.querySelector('#f_address').value || '',
      Group: form.querySelector('#f_group').value || '',
      lat: Number(form.querySelector('#f_lat').textContent),
      lon: Number(form.querySelector('#f_lon').textContent),
      Status: normalizeStatus(form.querySelector('#f_status').value || 'none')
    };
    const map = findMap();
    DATA.push(rec);
    addCircleFor(rec, DATA.length - 1, map);
    if (rec.Group) {
      // Update group selector if it's a fresh custom value
      const sel = document.getElementById('groupSel');
      const exists = Array.from(sel.options).some(o => o.value === rec.Group);
      if (!exists) {
        const opt = document.createElement('option');
        opt.value = rec.Group; opt.textContent = rec.Group; opt.selected = false;
        sel.appendChild(opt);
      }
    }
    applyFilters(map);
    form.remove();
    showToast("Added!");
  };
}

function undoDelete() {
  if (!UNDO_STACK.length) { showToast("Nothing to undo."); return; }
  const action = UNDO_STACK.pop();
  if (action.type === 'delete') {
    const pos = Math.min(action.idx, DATA.length);
    DATA.splice(pos, 0, action.rec);
    const map = findMap();
    rebuildMarkers(map);
    showToast("Restored.");
  }
}

// ---------- Init & wiring ----------
function init() {
  const map = findMap();
  if (!map) { setTimeout(init, 300); return; }

  groupSelInit();

  DATA.forEach((rec, idx) => {
    rec.Status = normalizeStatus(rec.Status);
    if (Number.isFinite(rec.lat) && Number.isFinite(rec.lon)) {
      addCircleFor(rec, idx, map);
    }
  });

  document.getElementById('groupClear').onclick = () => {
    const sel = document.getElementById('groupSel');
    Array.from(sel.options).forEach(o => o.selected = false);
    applyFilters(map);
  };
  document.getElementById('groupSelectAll').onclick = () => {
    const sel = document.getElementById('groupSel');
    Array.from(sel.options).forEach(o => o.selected = true);
    applyFilters(map);
  };

  Array.from(document.querySelectorAll('.statusChk')).forEach(cb =>
    cb.addEventListener('change', () => applyFilters(map))
  );
  document.getElementById('groupSel').addEventListener('change', () => applyFilters(map));
  document.getElementById('btnSearch').onclick = () => applyFilters(map);
  document.getElementById('btnClearSearch').onclick = () => { document.getElementById('searchBox').value=''; applyFilters(map); };
  document.getElementById('btnZoomFirst').onclick = () => zoomFirst();
  document.getElementById('btnDownload').onclick = () => downloadCSV();
  document.getElementById('btnUndoDelete').onclick = () => undoDelete();

  document.getElementById('btnAddMode').onclick = () => toggleAddMode();
  map.on('click', mapClickHandler);

  document.getElementById('btnGeocode').onclick = async () => {
    if (!addMode) { toggleAddMode(); }
    const addr = (document.getElementById('addAddress').value || '').trim();
    if (!addr) { document.getElementById('addAddress').focus(); return; }
    const ll = await geocodeClient(addr);
    if (ll) {
      openAddForm(ll.lat, ll.lon, ll, addr, '');
      map.setView([ll.lat, ll.lon], 14);
    } else {
      const base = lastClickLL ? {lat: lastClickLL.lat, lon: lastClickLL.lng} : {lat: map.getCenter().lat, lon: map.getCenter().lng};
      openAddForm(base.lat, base.lon, null, addr, '');
    }
  };
  document.getElementById('btnUseClick').onclick = () => {
    if (!addMode) { toggleAddMode(); }
    const base = lastClickLL ? {lat: lastClickLL.lat, lon: lastClickLL.lng} : {lat: map.getCenter().lat, lon: map.getCenter().lng};
    const addr = (document.getElementById('addAddress').value || '').trim();
    openAddForm(base.lat, base.lon, null, addr, '');
  };

  document.getElementById('btnToggleList').onclick = () => toggleList();

  applyFilters(map); // also builds the list if ON
}

function showToast(msg="Saved!") {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.display = "block";
  setTimeout(() => t.style.display = "none", 1600);
}

setTimeout(init, 400);
</script>
"""

    from branca.element import Element
    m.get_root().html.add_child(Element(controls_html))
    m.get_root().html.add_child(Element(js_head + js_body))
    m.save(str(output_html))

# ---------- Main ----------
def main():
    parser = argparse.ArgumentParser(description="Editable NS schools map (filters, search, list view, add, move, delete+undo).")
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

# Load data: auto-detect CSV vs Excel
if input_path.suffix.lower() == ".csv":
    df = pd.read_csv(input_path)
else:
    df = load_workbook(input_path, args.district_col)


    # Load data: auto-detect CSV vs Excel
if input_path.suffix.lower() == ".csv":
    df = pd.read_csv(input_path)
else:
    df = load_workbook(input_path, args.district_col)


    for col in [args.address_col, args.school_col]:
        if col not in df.columns:
            raise KeyError(f"Required column '{col}' not found.")

    df["__full_addr__"] = df[args.address_col].astype(str).map(normalize_address)

    cache = {} if args.no_cache else load_cache(cache_path)
    geolocator = Nominatim(user_agent="ns_schools_mapper_v9")
    geocode_fn = RateLimiter(geolocator.geocode, min_delay_seconds=args.min_delay_seconds, swallow_exceptions=False)

    if args.max > 0:
        df = df.head(args.max)

    # Honor existing lat/lon; otherwise cache/geocode
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

    # Normalize status and save CSV snapshot
    df[STATUS_COL] = df.apply(derive_status, axis=1)
    out_csv = output_html.with_suffix(".csv")
    df.to_csv(out_csv, index=False)

    # Build map
    mapped = df.dropna(subset=["lat","lon"]).copy()
    build_map(mapped, output_html)

    print(f"Done. Map: {output_html}")
    if failed:
        print(f"Geocoding failures: {output_html.with_suffix('.failed_geocodes.csv')}")
    print(f"Reference data (CSV): {out_csv}")

if __name__ == "__main__":
    main()

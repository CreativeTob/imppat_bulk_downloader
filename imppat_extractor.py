import streamlit as st
import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import time
import zipfile
import io
import string
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

if "selected_plants" not in st.session_state:
    st.session_state.selected_plants = set()

# ── CONFIG ───────────────────────────────────────────────────────────────────
BASE_URL          = "https://cb.imsc.res.in/imppat"
SDF_URL_PATTERN   = "{base}/images/2D/SDF/{imphy_id}.sdf"
CACHE_DIR         = ".imppat_cache"
CACHE_EXPIRY_DAYS = 7
MAX_WORKERS       = 16  # parallel requests for both scan and download

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

PLANT_PARTS = [
    "aerial part", "bark", "bark and stem", "bulb", "calyx", "cone",
    "endosperm", "essential oil", "exudate", "flower", "fruit", "gall",
    "gum", "heartwood", "hull", "kernel", "latex", "leaf", "node",
    "peel", "pericarp", "plant", "pod", "pollen", "pulp", "resin",
    "rhizome", "root", "root bark", "seed", "shoot", "stamen", "stem",
    "stem bark", "stigma", "twig", "whole plant", "wood",
]
# ─────────────────────────────────────────────────────────────────────────────

os.makedirs(CACHE_DIR, exist_ok=True)


# ── CACHE HELPERS ─────────────────────────────────────────────────────────────
def cache_path(key):
    return os.path.join(CACHE_DIR, f"{key}.json")


def load_cache(key):
    path = cache_path(key)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            data = json.load(f)
        cached_at = datetime.fromisoformat(data["cached_at"])
        if datetime.now() - cached_at > timedelta(days=CACHE_EXPIRY_DAYS):
            return None
        return data["results"]
    except Exception:
        return None


def save_cache(key, results):
    path = cache_path(key)
    try:
        with open(path, "w") as f:
            json.dump({"cached_at": datetime.now().isoformat(), "results": results}, f)
    except Exception:
        pass


def cache_age(key):
    path = cache_path(key)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            data = json.load(f)
        cached_at = datetime.fromisoformat(data["cached_at"])
        delta = datetime.now() - cached_at
        hours = int(delta.total_seconds() // 3600)
        if hours < 1:
            return "less than 1 hour ago"
        elif hours < 24:
            return f"{hours} hour{'s' if hours > 1 else ''} ago"
        else:
            days = hours // 24
            return f"{days} day{'s' if days > 1 else ''} ago"
    except Exception:
        return None


# ── CORE FUNCTIONS ────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def fetch_plant_list():
    resp = requests.get(f"{BASE_URL}/", headers=HEADERS, timeout=20)
    soup = BeautifulSoup(resp.text, "html.parser")

    select = None
    for s in soup.find_all("select"):
        opts = s.find_all("option")
        if any("/imppat/phytochemical/" in (o.get("value") or "") for o in opts):
            select = s
            break

    if not select:
        return []

    plants = []
    for option in select.find_all("option"):
        name  = option.get_text(strip=True)
        value = option.get("value", "")
        if not name or not value or "choose" in name.lower():
            continue
        url = "https://cb.imsc.res.in" + value.strip("'").replace(" ", "%20")
        plants.append({"name": name, "url": url})

    return plants


def fetch_compounds(plant_url):
    resp = requests.get(plant_url, headers=HEADERS, timeout=20)
    if resp.status_code != 200:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    tbody = table.find("tbody")
    if not tbody:
        return []

    thead = table.find("thead")
    headers = [th.get_text(strip=True) for th in thead.find_all("th")] if thead else []

    id_col   = next((i for i, h in enumerate(headers) if "identifier" in h.lower() or "imphy" in h.lower() or "id" in h.lower()), 2)
    name_col = next((i for i, h in enumerate(headers) if "name" in h.lower()), 3)
    part_col = next((i for i, h in enumerate(headers) if "part" in h.lower()), 1)

    compounds = []
    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue

        def cell_text(idx):
            return cells[idx].get_text(strip=True) if idx < len(cells) else ""

        imphy_id = cell_text(id_col)
        name     = cell_text(name_col)
        part     = cell_text(part_col)

        for link in tr.find_all("a"):
            href = link.get("href", "")
            text = link.get_text(strip=True)
            if "IMPHY" in href:
                imphy_id = href.split("/")[-1]
                break
            elif "IMPHY" in text:
                imphy_id = text
                break

        if imphy_id and imphy_id.startswith("IMPHY"):
            compounds.append({
                "imphy_id": imphy_id,
                "name": name,
                "plant_part": part,
            })

    return compounds


@st.cache_data(show_spinner=False)
def fetch_single_sdf(imphy_id):
    """Fetch a single SDF file — cached so reruns don't re-download."""
    url  = SDF_URL_PATTERN.format(base=BASE_URL, imphy_id=imphy_id)
    resp = requests.get(url, headers=HEADERS, timeout=15)
    if resp.status_code == 200 and len(resp.content) > 10:
        return resp.content
    return None


def download_sdfs_to_zip(compounds_by_plant):
    """
    Download all SDF files in parallel and pack into a ZIP.
    No sleep delay — parallelized with ThreadPoolExecutor.
    """
    zip_buf  = io.BytesIO()
    total    = sum(len(c) for c in compounds_by_plant.values())
    progress = st.progress(0, text="Starting downloads...")
    done     = 0
    results  = {}  # imphy_id -> (filepath, content)

    # Build flat list of all downloads
    tasks = []
    for plant_name, compounds in compounds_by_plant.items():
        safe_plant = "".join(c if c.isalnum() or c in "-_" else "_" for c in plant_name)
        for compound in compounds:
            safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in compound["name"])
            filepath  = f"{safe_plant}/{safe_name}.sdf"
            tasks.append((compound["imphy_id"], compound["name"], filepath))

    def fetch_sdf(task):
        imphy_id, name, filepath = task
        url = SDF_URL_PATTERN.format(base=BASE_URL, imphy_id=imphy_id)
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200 and len(resp.content) > 10:
                return filepath, resp.content, name
        except Exception:
            pass
        return filepath, None, name

    # Download in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_sdf, task): task for task in tasks}
        for future in as_completed(futures):
            filepath, content, name = future.result()
            if content:
                results[filepath] = content
            done += 1
            progress.progress(done / total, text=f"Downloading {done}/{total}: {name[:40]}")

    # Write all to ZIP
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for filepath, content in results.items():
            zf.writestr(filepath, content)

    progress.empty()
    zip_buf.flush()
    zip_buf.seek(0)
    return zip_buf


# ── PARALLEL SCAN ─────────────────────────────────────────────────────────────
def scan_plants_parallel(plants, progress_bar):
    """Fetch compound lists for all plants in parallel. No sleep delay."""
    results = {}
    total   = len(plants)
    done    = 0

    def fetch_one(plant):
        compounds = fetch_compounds(plant["url"])
        return plant["name"], plant["url"], compounds

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_one, plant): plant for plant in plants}
        for future in as_completed(futures):
            name, url, compounds = future.result()
            results[name] = {"compounds": compounds, "url": url}
            done += 1
            progress_bar.progress(
                done / total,
                text=f"Scanning {done}/{total}: {name[:45]}"
            )

    return results


# ── PAGE SETUP ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="IMPPAT Explorer", page_icon="🌿", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
h1, h2, h3 { font-family: 'DM Serif Display', serif !important; }

/* Background */
.stApp { background: #0f1117; }
.block-container { padding-top: 2rem; max-width: 1100px; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: #161b27;
    border-right: 1px solid #2a2f3e;
}
[data-testid="stSidebar"] * { color: #c9d1d9 !important; }
[data-testid="stSidebar"] h2 { color: #58a6ff !important; font-size: 1rem !important; }

/* Metrics */
[data-testid="stMetric"] {
    background: #161b27;
    border: 1px solid #2a2f3e;
    border-radius: 12px;
    padding: 1rem 1.2rem;
}
[data-testid="stMetricValue"] { color: #58a6ff !important; font-size: 1.8rem !important; }
[data-testid="stMetricLabel"] { color: #8b949e !important; }

/* Plant cards */
.plant-card {
    background: #161b27;
    border: 1px solid #2a2f3e;
    border-radius: 14px;
    padding: 0.9rem 1.2rem;
    margin-bottom: 0.5rem;
    display: flex;
    justify-content: space-between;
    align-items: center;
    transition: border-color 0.2s;
}
.plant-card:hover { border-color: #58a6ff; }
.plant-name { font-weight: 500; color: #e6edf3; font-size: 0.95rem; }
.plant-latin { font-style: italic; color: #8b949e; font-size: 0.82rem; }
.compound-badge {
    background: linear-gradient(135deg, #1f6feb, #388bfd);
    color: white;
    border-radius: 20px;
    padding: 3px 14px;
    font-size: 0.78rem;
    font-weight: 600;
    white-space: nowrap;
}

/* Dataframe overrides */
[data-testid="stDataFrame"] {
    border: 1px solid #2a2f3e !important;
    border-radius: 12px !important;
    overflow: hidden;
}
[data-testid="stDataFrame"] thead tr th {
    background: #161b27 !important;
    color: #58a6ff !important;
    font-weight: 600 !important;
    font-size: 0.85rem !important;
    letter-spacing: 0.05em !important;
    text-transform: uppercase !important;
    border-bottom: 1px solid #2a2f3e !important;
    padding: 0.75rem 1rem !important;
}
[data-testid="stDataFrame"] tbody tr td {
    background: #0f1117 !important;
    color: #e6edf3 !important;
    font-size: 0.9rem !important;
    padding: 0.65rem 1rem !important;
    border-bottom: 1px solid #1e2433 !important;
}
[data-testid="stDataFrame"] tbody tr:hover td {
    background: #161b27 !important;
}
[data-testid="stDataFrame"] tbody tr[aria-selected="true"] td {
    background: #1f3a5f !important;
    color: #58a6ff !important;
}

/* Expander */
[data-testid="stExpander"] {
    background: #161b27 !important;
    border: 1px solid #2a2f3e !important;
    border-radius: 12px !important;
    margin-bottom: 0.5rem;
}
[data-testid="stExpander"] summary {
    color: #e6edf3 !important;
    font-weight: 500 !important;
    font-size: 0.92rem !important;
}

/* Buttons */
.stButton > button {
    border-radius: 8px !important;
    font-weight: 500 !important;
    transition: all 0.2s !important;
}
.stDownloadButton > button {
    border-radius: 8px !important;
    font-weight: 600 !important;
}

/* Divider */
hr { border-color: #2a2f3e !important; }

/* Info / warning boxes */
[data-testid="stAlert"] {
    border-radius: 10px !important;
    border: 1px solid #2a2f3e !important;
}

/* Section headers */
.section-label {
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #58a6ff;
    margin-bottom: 0.5rem;
    margin-top: 1.2rem;
}

/* Hint text */
.hint-text {
    font-size: 0.8rem;
    color: #8b949e;
    margin-top: -0.3rem;
    margin-bottom: 0.8rem;
}
</style>
""", unsafe_allow_html=True)


# ── HEADER ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="padding: 1.5rem 0 0.5rem 0;">
    <div style="font-size:0.78rem; font-weight:600; letter-spacing:0.12em; color:#58a6ff; text-transform:uppercase; margin-bottom:0.4rem;">
        Phytochemical Database
    </div>
    <h1 style="font-family:'DM Serif Display',serif; font-size:2.4rem; color:#e6edf3; margin:0; line-height:1.1;">
        🌿 IMPPAT Explorer
    </h1>
    <p style="color:#8b949e; font-size:0.95rem; margin-top:0.6rem; max-width:600px;">
        Browse, filter, and bulk-download 2D SDF files from India's medicinal plant phytochemistry database.
        Select plants from the table, then download their structures as a ZIP.
    </p>
</div>
""", unsafe_allow_html=True)
st.divider()


# ── LOAD PLANT LIST ───────────────────────────────────────────────────────────
with st.spinner("Loading plant database..."):
    all_plants = fetch_plant_list()

if not all_plants:
    st.error("Could not load plant list from IMPPAT. Check your internet connection.")
    st.stop()


# ── SIDEBAR FILTERS ───────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🔬 Filters")

    browse_mode = st.radio("Browse mode", ["All plants", "By letter"], index=0)

    selected_letter = None
    if browse_mode == "By letter":
        selected_letter = st.selectbox("Select letter", list(string.ascii_uppercase))

    st.markdown("---")
    st.markdown("#### Compound count filter")
    filter_by_count = st.checkbox("Filter by compound count")
    min_compounds   = 0
    max_compounds   = 9999
    if filter_by_count:
        min_compounds, max_compounds = st.slider(
            "Compound range",
            min_value=0, max_value=1000,
            value=(100, 500), step=10,
        )

    st.markdown("---")
    st.markdown("#### Plant part filter")
    filter_by_part = st.checkbox("Filter by plant part")
    selected_parts = []
    if filter_by_part:
        selected_parts = st.multiselect("Select plant parts", options=PLANT_PARTS, default=[])

    st.markdown("---")
    st.markdown("#### Download")
    download_mode   = st.radio("What to download", ["Selected plants only", "All filtered plants"])
    limit_downloads = st.checkbox("Limit number of plants to download")
    download_limit  = None
    if limit_downloads:
        download_limit = st.number_input("Max plants to download", min_value=1, max_value=500, value=10, step=1)


# ── PRE-FILTERING ─────────────────────────────────────────────────────────────
if browse_mode == "By letter" and selected_letter:
    base_plants = [p for p in all_plants if p["name"].upper().startswith(selected_letter)]
else:
    base_plants = all_plants

# ── SCAN STATE ────────────────────────────────────────────────────────────────
visible_plants_display = []
is_scanned = st.session_state.get("scanned", False)
scan_key   = selected_letter or "ALL"

# Auto-load from disk cache into session state if not already loaded
if not is_scanned:
    cached = load_cache(scan_key)
    if cached:
        st.session_state.scan_results = cached
        st.session_state.scanned      = True
        st.session_state.scanned_key  = scan_key
        is_scanned = True

if is_scanned:
    if st.sidebar.button("🔄 Clear Cache & Rescan"):
        if os.path.exists(cache_path(scan_key)):
            os.remove(cache_path(scan_key))
        st.session_state.scanned = False
        st.rerun()

    scan = st.session_state.scan_results
    for plant in base_plants:
        result = scan.get(plant["name"])
        if not result:
            continue

        compounds = result["compounds"]

        # Step 1: plant part filter (trims compound list first)
        if filter_by_part and selected_parts:
            compounds = [c for c in compounds if c["plant_part"].lower() in [p.lower() for p in selected_parts]]

        current_count = len(compounds)

        # Step 2: compound range filter on trimmed count
        if filter_by_count:
            if not (min_compounds <= current_count <= max_compounds):
                continue

        if current_count > 0 or not filter_by_count:
            visible_plants_display.append({**plant, "compounds": compounds, "count": current_count})

    # Step 3: download limit — sort by count descending, take top N
    if limit_downloads and download_limit:
        visible_plants_display = sorted(
            visible_plants_display, key=lambda p: p["count"], reverse=True
        )[:int(download_limit)]

else:
    st.info("🔍 Filters (Range/Part) are inactive. Click 'Scan compounds' to enable them.")
    if st.button("🔍 Scan compounds now", type="primary"):
        prog    = st.progress(0)
        results = scan_plants_parallel(base_plants, prog)
        save_cache(scan_key, results)
        st.session_state.scan_results = results
        st.session_state.scanned      = True
        st.session_state.scanned_key  = scan_key
        st.rerun()

    visible_plants_display = [
        {"name": p["name"], "url": p["url"], "count": "?", "compounds": []}
        for p in base_plants
    ]


# ── STATS ─────────────────────────────────────────────────────────────────────
total_cpds = sum(p["count"] for p in visible_plants_display if isinstance(p["count"], int))

c1, c2, c3 = st.columns(3)
with c1: st.metric("Plants in Database", len(all_plants))
with c2: st.metric("Matching Plants", len(visible_plants_display))
with c3: st.metric("Total Compounds", total_cpds)


# ── PLANT LIST ────────────────────────────────────────────────────────────────
if visible_plants_display:
    st.markdown(
        '<p class="hint-text">💡 Click a row to select it. Hold Shift or Ctrl to select multiple plants.</p>',
        unsafe_allow_html=True
    )

    df_display = pd.DataFrame([
        {"🌿  Plant Name": p["name"], "Compounds": p["count"]}
        for p in visible_plants_display
    ])

    event = st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
        key="plant_table",
        column_config={
            "🌿  Plant Name": st.column_config.TextColumn(width="large"),
            "Compounds": st.column_config.NumberColumn(
                width="small",
                format="%d",
            ),
        },
    )

    selected_rows = event.selection.rows if event.selection else []
    st.session_state.selected_plants = {
        visible_plants_display[i]["name"] for i in selected_rows
    }

    if selected_rows:
        st.markdown(
            f'<p class="hint-text">✓ {len(selected_rows)} plant(s) selected</p>',
            unsafe_allow_html=True
        )

    # Per-compound expander — shown below table for selected plants only
    if st.session_state.get("scanned") and selected_rows:
        st.markdown('<div class="section-label">Compounds in selected plants</div>', unsafe_allow_html=True)
        for i in selected_rows:
            plant = visible_plants_display[i]
            if not plant.get("compounds"):
                continue
            with st.expander(f"🌱 {plant['name']}  ·  {len(plant['compounds'])} compounds"):
                # Group by plant part for cleaner display
                parts = {}
                for compound in plant["compounds"]:
                    parts.setdefault(compound["plant_part"], []).append(compound)

                for part, part_compounds in parts.items():
                    st.markdown(
                        f'<div style="font-size:0.72rem;font-weight:600;letter-spacing:0.08em;'
                        f'text-transform:uppercase;color:#58a6ff;margin:0.8rem 0 0.4rem 0;">'
                        f'📍 {part or "Unknown part"}</div>',
                        unsafe_allow_html=True
                    )
                    for c_idx, compound in enumerate(part_compounds):
                        global_idx = plant["compounds"].index(compound)
                        c1, c2 = st.columns([0.82, 0.18])
                        c1.markdown(
                            f'<div style="color:#e6edf3;font-size:0.88rem;padding:0.25rem 0;">'
                            f'<span style="font-weight:500;">{compound["name"]}</span> '
                            f'<span style="color:#8b949e;font-size:0.78rem;">{compound["imphy_id"]}</span>'
                            f'</div>',
                            unsafe_allow_html=True
                        )
                        btn_key = f"btn_{i}_{global_idx}"
                        if f"data_{btn_key}" not in st.session_state:
                            if c2.button("⬇ SDF", key=f"load_{btn_key}"):
                                with st.spinner(""):
                                    data = fetch_single_sdf(compound["imphy_id"])
                                    if data:
                                        st.session_state[f"data_{btn_key}"] = data
                                        st.rerun()
                        else:
                            c2.download_button(
                                label="📥 Save",
                                data=st.session_state[f"data_{btn_key}"],
                                file_name=f"{compound['name']}.sdf",
                                key=btn_key,
                                type="primary",
                            )
else:
    st.markdown("""
    <div style="text-align:center; padding:3rem 1rem; color:#8b949e;">
        <div style="font-size:2.5rem; margin-bottom:0.5rem;">🔍</div>
        <div style="font-size:1rem; font-weight:500; color:#e6edf3;">No plants match your filters</div>
        <div style="font-size:0.85rem; margin-top:0.3rem;">Try adjusting the compound range or removing the plant part filter.</div>
    </div>
    """, unsafe_allow_html=True)


# ── DOWNLOAD SECTION ──────────────────────────────────────────────────────────
st.divider()
st.markdown("""
<div style="margin-bottom:0.8rem;">
    <div class="section-label">Bulk Download</div>
    <div style="font-size:1.3rem; font-weight:700; color:#e6edf3; font-family:'DM Serif Display',serif;">
        📦 Download SDF Files
    </div>
    <div style="color:#8b949e; font-size:0.85rem; margin-top:0.2rem;">
        Downloads are packaged as a ZIP, organized by plant name.
    </div>
</div>
""", unsafe_allow_html=True)

if download_mode == "Selected plants only":
    plants_to_download = [p for p in visible_plants_display if p["name"] in st.session_state.selected_plants]
    if not plants_to_download:
        st.warning("No plants selected. Click rows in the table above to select, or switch to 'All filtered plants'.")
else:
    plants_to_download = visible_plants_display

st.markdown(f"**{len(plants_to_download)} plant(s)** queued for download.")

if st.button("⬇️ Start Download", type="primary", disabled=len(plants_to_download) == 0):
    compounds_by_plant = {}
    needs_fetch = [p for p in plants_to_download if not p.get("compounds")]

    if needs_fetch:
        prog = st.progress(0, text="Fetching compound lists...")
        fetched = {}

        def fetch_plant_compounds(plant):
            compounds = fetch_compounds(plant["url"])
            if filter_by_part and selected_parts:
                compounds = [c for c in compounds if c["plant_part"].lower() in [p.lower() for p in selected_parts]]
            return plant["name"], compounds

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_plant_compounds, p): p for p in needs_fetch}
            done = 0
            for future in as_completed(futures):
                name, compounds = future.result()
                fetched[name] = compounds
                done += 1
                prog.progress(done / len(needs_fetch), text=f"Fetching {done}/{len(needs_fetch)}: {name[:40]}")

        prog.empty()
        compounds_by_plant.update(fetched)

    for plant in plants_to_download:
        if plant["name"] not in compounds_by_plant:
            compounds_by_plant[plant["name"]] = plant.get("compounds", [])

    total_sdfs = sum(len(c) for c in compounds_by_plant.values())
    st.markdown(f"Downloading **{total_sdfs} SDF files** across {len(compounds_by_plant)} plant(s)...")

    zip_buf = download_sdfs_to_zip(compounds_by_plant)
    st.session_state["zip_bytes"] = zip_buf.getvalue()
    st.session_state["zip_ready"] = True

# Render download button outside click block so it persists across reruns
if st.session_state.get("zip_ready"):
    st.success("✓ Done! ZIP is ready.")
    st.download_button(
        label="📥 Download ZIP",
        data=st.session_state["zip_bytes"],
        file_name="imppat_sdfs.zip",
        mime="application/zip",
        type="primary",
        key="final_zip_download",
    )

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
MAX_WORKERS       = 8   # parallel requests during scan

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


# ── CORE FUNCTIONS (unchanged from working code) ──────────────────────────────
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


def download_sdfs_to_zip(compounds_by_plant):
    session  = requests.Session()
    session.headers.update(HEADERS)
    zip_buf  = io.BytesIO()
    total    = sum(len(c) for c in compounds_by_plant.values())
    progress = st.progress(0, text="Starting downloads...")
    done     = 0

    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for plant_name, compounds in compounds_by_plant.items():
            safe_plant = "".join(c if c.isalnum() or c in "-_" else "_" for c in plant_name)
            for compound in compounds:
                imphy_id  = compound["imphy_id"]
                name      = compound["name"]
                url       = SDF_URL_PATTERN.format(base=BASE_URL, imphy_id=imphy_id)
                safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in name)
                filepath  = f"{safe_plant}/{safe_name}.sdf"

                try:
                    resp = session.get(url, timeout=15)
                    if resp.status_code == 200 and len(resp.content) > 10:
                        zf.writestr(filepath, resp.content)
                except Exception:
                    pass

                done += 1
                progress.progress(done / total, text=f"Downloading {done}/{total}: {name[:40]}")
                time.sleep(0.3)

    progress.empty()
    zip_buf.flush()
    zip_buf.seek(0)
    return zip_buf


@st.cache_data(show_spinner=False)
def fetch_single_sdf(imphy_id):
    """Fetch a single SDF file and cache it so reruns don't re-download."""
    url  = SDF_URL_PATTERN.format(base=BASE_URL, imphy_id=imphy_id)
    resp = requests.get(url, headers=HEADERS, timeout=15)
    if resp.status_code == 200 and len(resp.content) > 10:
        return resp.content
    return None


# ── PARALLEL SCAN ─────────────────────────────────────────────────────────────
def scan_plants_parallel(plants, progress_bar):
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
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
h1, h2, h3 { font-family: 'DM Serif Display', serif !important; }
.stApp { background: black; }
.block-container { padding-top: 2rem; }
.plant-card {
    background: white; border-radius: 50px; padding: 1rem 1.2rem;
    margin-bottom: 0.6rem;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    display: flex; justify-content: space-between; align-items: center;
}
.plant-name { font-weight: 500; color: #1a1a1a; }
.compound-badge {
    background: #2d6a4f; color: white; border-radius: 20px;
    padding: 2px 12px; font-size: 0.8rem; font-weight: 500;
}
.stat-box {
    background: white; border-radius: 12px; padding: 1.2rem;
    text-align: center; box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
.stat-number { font-size: 2rem; font-weight: 700; color: #2d6a4f; }
.stat-label  { font-size: 0.85rem; color: #666; margin-top: 0.2rem; }
.cache-info  { font-size: 0.78rem; color: #888; font-style: italic; }
</style>
""", unsafe_allow_html=True)


# ── HEADER ────────────────────────────────────────────────────────────────────
st.markdown("# 🌿 IMPPAT Explorer")
st.markdown("Browse, filter, and bulk-download 2D SDF files from the Indian Medicinal Plants, Phytochemistry and Therapeutics database.")
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
    download_mode = st.radio("What to download", ["Selected plants only", "All filtered plants"])
    limit_downloads = st.checkbox("Limit number of plants to download")
    download_limit  = None
    if limit_downloads:
        download_limit = st.number_input("Max plants to download", min_value=1, max_value=500, value=10, step=1)


# ── FILTER BY LETTER ──────────────────────────────────────────────────────────
# 1. PRE-FILTERING (Establish the base list)
if browse_mode == "By letter" and selected_letter:
    base_plants = [p for p in all_plants if p["name"].upper().startswith(selected_letter)]
else:
    base_plants = all_plants

# 2. PROCESSING & STABLE FILTERING
visible_plants_display = []

# Determine if we are in "Scan Mode"
is_scanned = st.session_state.get("scanned", False)
scan_key = selected_letter or "ALL"

# Auto-load cache if it exists but session is empty
if not is_scanned:
    cached = load_cache(scan_key)
    if cached:
        st.session_state.scan_results = cached
        st.session_state.scanned = True
        st.session_state.scanned_key = scan_key
        is_scanned = True

if is_scanned:
    # --- RESCAN BUTTON ---
    if st.sidebar.button("🔄 Clear Cache & Rescan"):
        if os.path.exists(cache_path(scan_key)):
            os.remove(cache_path(scan_key))
        st.session_state.scanned = False
        st.rerun()

    scan = st.session_state.scan_results
    for plant in base_plants:
        result = scan.get(plant["name"])
        if not result: continue
        
        compounds = result["compounds"]
        
        # 1. Apply PART filter first (Crucial: this changes the count!)
        if filter_by_part and selected_parts:
            compounds = [c for c in compounds if c["plant_part"].lower() in [p.lower() for p in selected_parts]]
        
        current_count = len(compounds)
        
        # 2. Apply RANGE filter based on the NEW count
        if filter_by_count:
            if not (min_compounds <= current_count <= max_compounds):
                continue
        
        # 3. Add to display list
        if current_count > 0 or not filter_by_count:
            visible_plants_display.append({**plant, "compounds": compounds, "count": current_count})

    # 4. Apply DOWNLOAD LIMIT last
    if limit_downloads and download_limit:
        visible_plants_display = sorted(visible_plants_display, key=lambda p: p["count"], reverse=True)[:int(download_limit)]

else:
    # NOT SCANNED: Show base plants but ignore the range/part filters so they don't disappear
    st.info("🔍 Filters (Range/Part) are inactive. Click 'Scan compounds' to enable them.")
    if st.button("🔍 Scan compounds now", type="primary"):
        prog = st.progress(0)
        results = scan_plants_parallel(base_plants, prog)
        save_cache(scan_key, results)
        st.session_state.scan_results = results
        st.session_state.scanned = True
        st.session_state.scanned_key = scan_key
        st.rerun()
        
    visible_plants_display = [{"name": p["name"], "url": p["url"], "count": "?", "compounds": []} for p in base_plants]

# 3. STATS & DISPLAY
st.markdown(f"### Results ({len(visible_plants_display)} plants)")
c1, c2, c3 = st.columns(3)
with c1: st.metric("Database Total", len(all_plants))
with c2: st.metric("Filtered Plants", len(visible_plants_display))
with c3: 
    total_cpds = sum(p["count"] for p in visible_plants_display if isinstance(p["count"], int))
    st.metric("Total Compounds", total_cpds)

# 4. STABLE INTERACTIVE LIST
select_all = st.checkbox("Select all visible", key="master_select")

for p_idx, plant in enumerate(visible_plants_display[:200]):
    check_key = f"chk_{plant['name']}_{p_idx}"
    col_check, col_info = st.columns([0.05, 0.95])
    
    with col_check:
        is_selected = st.checkbox("", key=check_key, value=select_all)
        if is_selected:
            st.session_state.selected_plants.add(plant["name"])
        else:
            st.session_state.selected_plants.discard(plant["name"])
            
    with col_info:
        badge = plant["count"]
        st.markdown(f'''
            <div class="plant-card">
                <span class="plant-name">{plant["name"]}</span>
                <span class="compound-badge">{badge} compounds</span>
            </div>
        ''', unsafe_allow_html=True)
        
        # Only show expander if we have scanned data
        if st.session_state.get("scanned") and plant.get("compounds"):
            with st.expander(f"View {len(plant['compounds'])} compounds"):
                for c_idx, compound in enumerate(plant["compounds"]):
                    c1, c2 = st.columns([0.8, 0.2])
                    c1.write(f"**{compound['name']}** ({compound['plant_part']})")
                    
                    
                    # Generate a unique key
                    btn_key = f"btn_{p_idx}_{c_idx}"
                    
                    
                    if f"data_{btn_key}" not in st.session_state:
                        # Place a placeholder button to "Load & Download"
                        if c2.button("Get SDF", key=f"load_{btn_key}"):
                            with st.spinner("..."):
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
                            type="primary"
                        )


if len(visible_plants_display) > 200:
    st.info(f"Showing first 200 of {len(visible_plants_display)} plants.")

# ── DOWNLOAD SECTION ──────────────────────────────────────────────────────────
st.divider()
st.markdown("### 📦 Download SDFs")

# Use the session state set directly for the filter
if download_mode == "Selected plants only":
    plants_to_download = [p for p in visible_plants_display if p["name"] in st.session_state.selected_plants]
else:
    plants_to_download = visible_plants_display

st.markdown(f"**{len(plants_to_download)} plant(s)** queued for download.")

if st.button("⬇️ Start Download", type="primary", disabled=len(plants_to_download) == 0):
    compounds_by_plant = {}
    needs_fetch = [p for p in plants_to_download if not p.get("compounds")]

    if needs_fetch:
        prog = st.progress(0, text="Fetching compound lists...")
        for i, plant in enumerate(needs_fetch):
            compounds = fetch_compounds(plant["url"])
            if filter_by_part and selected_parts:
                compounds = [c for c in compounds if c["plant_part"].lower() in [p.lower() for p in selected_parts]]
            compounds_by_plant[plant["name"]] = compounds
            prog.progress((i + 1) / len(needs_fetch), text=f"Fetching {i+1}/{len(needs_fetch)}: {plant['name'][:40]}")
        prog.empty()

    for plant in plants_to_download:
        if plant["name"] not in compounds_by_plant:
            compounds_by_plant[plant["name"]] = plant.get("compounds", [])

    total_sdfs = sum(len(c) for c in compounds_by_plant.values())
    st.markdown(f"Downloading **{total_sdfs} SDF files** across {len(compounds_by_plant)} plant(s)...")

    # zip_buf = download_sdfs_to_zip(compounds_by_plant)
    # zip_bytes = zip_buf.getvalue()  # read into bytes before Streamlit reruns

    zip_buf = download_sdfs_to_zip(compounds_by_plant)
    # Store bytes in session_state so they survive Streamlit reruns
    st.session_state["zip_bytes"] = zip_buf.getvalue()
    st.session_state["zip_ready"] = True

    st.success(f"✓ Done! {total_sdfs} SDF files packed into ZIP.")
    st.download_button(
        label="📥 Download ZIP",
        data=zip_bytes,
        file_name="imppat_sdfs.zip",
        mime="application/zip",
        type="primary",
    )

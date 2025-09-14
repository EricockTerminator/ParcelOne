#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: app_streamlit_simple.py

Jednoduchá appka len s WFS + GML.
- E/C register, KU, voliteľné parcely (CSV)
- Vždy ťahá GML cez WFS 2.0 (FES filter), stránkuje po 1000
- Výstupy: gml-zip (bez GDAL), geojson/shp/dxf/gpkg cez GDAL (ogr2ogr)

Spustenie:  streamlit run app_streamlit_simple.py
"""
from __future__ import annotations
import time  # <-- pre profiler timed()
import glob
import io
import json
import os
import re
import sys
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple
from streamlit.components.v1 import html as st_html
from streamlit_folium import st_folium
from functools import lru_cache
from urllib.parse import urlencode
import folium
import pydeck as pdk  # nechávam – môžeš mať aj pydeck fallback
import requests
import streamlit as st
import unicodedata
result = None          # type: ignore  # FetchResult | None
map_res = None         # type: ignore  # FetchResult | None


# ----------------------------- Konštanty ------------------------------------

CP_WFS_BASE = "https://inspirews.skgeodesy.sk/geoserver/cp/ows"        # C register
CP_UO_WFS_BASE = "https://inspirews.skgeodesy.sk/geoserver/cp_uo/ows"   # E register
TYPE_C = "cp:CP.CadastralParcel"
TYPE_E = "cp_uo:CP.CadastralParcelUO"
HEADERS_XML = {
    "User-Agent": "ParcelOne/WFS-GML 1.0",
    "Accept": "application/xml,*/*;q=0.5",
    "Connection": "close",
}
TIMEOUT = (15, 90)  # (connect, read)
PAGE_SIZE = 1000
# --- voľby CRS pre WFS srsName (None = default servera)
WFS_CRS_CHOICES = {
    "auto (server default)": None,
    "EPSG:5514 (S-JTSK / Krovák EN)": "EPSG:5514",
    "EPSG:4258 (ETRS89)": "EPSG:4258",
    "EPSG:4326 (WGS84)": "EPSG:4326",
}
DEBUG_UI = False  # keď True, ukáže URL, diagnostiku, počty stránok
WMS_URL_C = "https://inspirews.skgeodesy.sk/geoserver/cp/ows"
WMS_URL_E = "https://inspirews.skgeodesy.sk/geoserver/cp_uo/ows"
LAYER_C = "cp:CP.CadastralParcel"
LAYER_E = "cp_uo:CP.CadastralParcelUO"
ZONE_C = "cp:CP.CadastralZoning"
ZONE_E = "cp_uo:CP.CadastralZoningUO"
PREVIEW_MAX_FEATURES = 500 # koľko GeoJSON prvkov max vykreslíme
PREVIEW_PAGE_SIZE = 500 # koľko prvkov žiadame na stránku
DEBUG_PROFILE = False # zapni pre zobrazenie časov
# --- Jednoduché profilovanie ---
_step_times: dict[str, float] = {}
class timed:
    def __init__(self, name: str):
        self.name = name
        self.t0 = 0.0
    def __enter__(self):
        self.t0 = time.perf_counter(); return self
    def __exit__(self, *exc):
        _step_times[self.name] = _step_times.get(self.name, 0.0) + (time.perf_counter() - self.t0)

# --------------------------- Pomocné funkcie -------------------------------

def _zoning_candidates(register: str) -> list[str]:
    if (register or "").upper() == "E":
        return ["cp_uo:CP.CadastralZoningUO", "cp_uo:CP.CadastralZoning"]
    else:
        return ["cp:CP.CadastralZoning", "cp:CP.CadastralZoningUO"]

TAB_SPLIT = re.compile(r"\t+")

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_zone_bbox(register: str, ku_code: str):
    """
    Vráti (minx, miny, maxx, maxy) pre hranicu KU.
    1) WFS Zoning → GeoJSON + CQL
    2) WFS Zoning → GeoJSON + FES
    3) POISTKA: WFS Parcely → vezmi 1 feature (GeoJSON) a sprav bbox
    """
    reg = (register or "").upper().strip()
    k = (ku_code or "").strip()
    if not k:
        return None

    base  = CP_UO_WFS_BASE if reg == "E" else CP_WFS_BASE
    layer = ZONE_E         if reg == "E" else ZONE_C

    # 1) Zoning – GeoJSON + CQL
    try:
        params = {
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": layer, "count": "1", "startIndex": "0",
            "outputFormat": "application/json",
            "CQL_FILTER": f"nationalCadastralReference='{k}'",
        }
        url = f"{base}?{urlencode(params)}"
        jb = http_get_bytes(url)
        feats = json.loads(jb.decode("utf-8","ignore")).get("features",[])
        if feats:
            return bbox_from_geojson({"type":"FeatureCollection","features":feats})
    except Exception:
        pass

    # 2) Zoning – GeoJSON + FES
    try:
        fes = ('<Filter xmlns="http://www.opengis.net/fes/2.0">'
               '<PropertyIsEqualTo><ValueReference>nationalCadastralReference</ValueReference>'
               f'<Literal>{xml_escape(k)}</Literal></PropertyIsEqualTo></Filter>')
        params = {
            "service":"WFS","version":"2.0.0","request":"GetFeature",
            "typeNames":layer,"count":"1","startIndex":"0",
            "outputFormat":"application/json","filter":fes,
        }
        url = f"{base}?{urlencode(params)}"
        jb = http_get_bytes(url)
        feats = json.loads(jb.decode("utf-8","ignore")).get("features",[])
        if feats:
            return bbox_from_geojson({"type":"FeatureCollection","features":feats})
    except Exception:
        pass

    # 3) POISTKA – parcelový layer, 1 feature (GeoJSON), srsName 4326/auto
    try:
        res = p_to_dxfk(reg, k, "", page_size=1)
        if res.ok and res.pages:
            obj = json.loads(res.pages[0].decode("utf-8","ignore"))
            return bbox_from_geojson(obj)
    except Exception:
        pass

    return None

# ---------- Robust resource resolver (works in EXE too) ----------
# keep exactly one copy of this helper in your file

def _resource_path(rel: str) -> str:
    base = getattr(sys, "_MEIPASS", None)  # when bundled by PyInstaller
    if base:
        p = os.path.join(base, rel)
        if os.path.exists(p):
            return p
    if getattr(sys, "frozen", False):  # onefile EXE – use dir of executable
        p = os.path.join(os.path.dirname(sys.executable), rel)
        if os.path.exists(p):
            return p
    here = os.path.dirname(__file__)
    p = os.path.join(here, rel)
    return p if os.path.exists(p) else rel

# ---------- KodKU.txt loader (format: "<name>" <code>) ----------
# keep exactly one copy of these helpers in your file

_KU_QUOTED_RE = re.compile(r'^\s*"(?P<name>.+?)"\s+(?P<code>\d{6,})\s*$')

def _parse_ku_line(line: str):
    s = (line or "").strip()
    m = _KU_QUOTED_RE.match(s)
    if not m:
        return None, None
    return m.group("code"), m.group("name").strip()

@st.cache_data(ttl=86400, show_spinner=False)
def load_ku_table(file_bytes: bytes | None = None, default_path: str = "KodKU.txt") -> list[dict]:
    text = ""
    if file_bytes is not None:
        try:
            text = file_bytes.decode("utf-8", "ignore")
        except Exception:
            text = file_bytes.decode("cp1250", "ignore")
    else:
        path = _resource_path(default_path)
        try:
            with open(path, "rb") as f:
                raw = f.read()
            try:
                text = raw.decode("utf-8", "ignore")
            except Exception:
                text = raw.decode("cp1250", "ignore")
        except FileNotFoundError:
            return []

    items, seen = [], set()
    for line in text.splitlines():
        code, name = _parse_ku_line(line)
        if not code or code in seen:
            continue
        seen.add(code)
        nm = name or code
        items.append({"code": code, "name": nm, "norm": nm.lower()})
    return items

# (helpery nižšie môžu zostať – niektoré sa môžu hodiť pri ďalších úpravách)

def _parse_zoning_xml_for_items(xml: bytes) -> list[dict]:
    txt = xml.decode("utf-8", "ignore")
    items = []
    code_re = re.compile(r"<([^>\s:]*:)?(?:[^>]*?(?:national)[^>]*?(?:cadastral)[^>]*?(?:ref|reference)|localId)[^>]*>([^<]+)</", re.I)
    name_re = re.compile(r"<([^>\s:]*:)?(?:[^>]*?(?:name|text|local))[^>]*>([^<]+)</", re.I)
    codes = [m.group(2).strip() for m in code_re.finditer(txt)]
    names = [m.group(2).strip() for m in name_re.finditer(txt)]
    best_name = ""
    if names:
        best_name = sorted(set(names), key=len, reverse=True)[0]
    for c in codes:
        cflat = "".join(c.split())
        if cflat.isdigit():
            nm = best_name or cflat
            items.append({"code": cflat, "name": nm, "norm": _strip_accents(nm)})
    return items

def _dedup_by_code(items: list[dict]) -> list[dict]:
    seen, out = set(), []
    for it in items:
        if it["code"] in seen:
            continue
        seen.add(it["code"])
        out.append(it)
    return out

def _as_text(v) -> str:
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        return str(v)
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, dict):
        if "text" in v and isinstance(v["text"], str):
            return v["text"].strip()
        sp = v.get("spelling")
        if isinstance(sp, list) and sp:
            t = _as_text(sp[0])
            if t:
                return t
        for vv in v.values():
            t = _as_text(vv)
            if t:
                return t
        return ""
    if isinstance(v, list):
        for item in v:
            t = _as_text(item)
            if t:
                return t
    return ""

# ---------- KU lookup z lokálneho súboru KodKU.txt ----------

def _strip_accents(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower()
    s = re.sub(r"[\-–—]+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = " ".join(s.split())
    return s

def lookup_ku_code(ku_table: list[dict], query: str) -> tuple[str | None, list[dict]]:
    q = (query or "").strip()
    if not q:
        return None, []
    if q.isdigit():
        return q, []
    nq = _strip_accents(q)
    for it in ku_table:
        if it["norm"] == nq:
            return it["code"], [it]
    hits = [it for it in ku_table if nq in it["norm"] or it["norm"].startswith(nq)]
    hits.sort(key=lambda x: (len(x["norm"]), x["norm"]))
    return (hits[0]["code"], hits[:10]) if hits else (None, [])

def _zoning_layer(register: str) -> str:
    return "cp_uo:CP.CadastralZoningUO" if (register or "").upper() == "E" else "cp:CP.CadastralZoning"

@st.cache_data(ttl=90, show_spinner=False)
def _preview_geojson(reg: str, ku: str, parcels: str):
    return fetch_geojson_pages(reg, ku, parcels, wfs_srs="EPSG:4326")

# ---------- WMS preview CQL builder ----------

def _build_cql_for_preview(ku: str, parcels_csv: str) -> str:
    parts = []
    ku = (ku or "").strip()
    if ku:
        parts.append(f"nationalCadastralReference LIKE '{ku}%'")
    pcs = [p.strip() for p in (parcels_csv or '').replace(';', ',').split(',') if p.strip()]
    if pcs and ku:
        ors = " OR ".join(["label='" + p.replace("'", "''") + "'" for p in pcs])
        parts.append(f"({ors})")
    return " AND ".join(parts)


def _cql_for_zone(ku: str) -> str:
    ku = (ku or "").strip()
    return f"nationalCadastralReference='{ku}'" if ku else ""


def show_map_preview(
    reg: str,
    fc_geojson: dict,
    bbox: tuple[float, float, float, float],
    *,
    ku: str = "",
    parcels: str = "",
):
    minx, miny, maxx, maxy = bbox
    cx, cy = (minx + maxx) / 2, (miny + maxy) / 2

    m = folium.Map(location=[cy, cx], zoom_start=14, tiles=None, control_scale=True)
    folium.TileLayer("OpenStreetMap", control=False).add_to(m)

    is_E = (reg or '').upper() == 'E'
    url   = WMS_URL_E if is_E else WMS_URL_C
    layer = LAYER_E    if is_E else LAYER_C
    zone  = ZONE_E     if is_E else ZONE_C

    # Parcely ako podklad (WMS), obmedzené len na KU (žiadne GeoJSON masy)
    cql_parc = _build_cql_for_preview(ku, "")
    p = dict(layers=layer, fmt="image/png", transparent=True, overlay=True, control=False, version="1.3.0",
             attr="© GKÚ SR / INSPIRE")
    if cql_parc:
        p["CQL_FILTER"] = cql_parc
    folium.raster_layers.WmsTileLayer(url=url, name="Parcely (WMS)", **p).add_to(m)

    # Hranica KU (WMS) – zvýraznenie
    cql_zone = _cql_for_zone(ku)
    zp = dict(layers=zone, fmt="image/png", transparent=True, overlay=True, control=False, version="1.3.0",
              attr="© GKÚ SR / INSPIRE", opacity=0.8)
    if cql_zone:
        zp["CQL_FILTER"] = cql_zone
    folium.raster_layers.WmsTileLayer(url=url, name="Hranica KU", **zp).add_to(m)

    # Ak používateľ zadá parcely → zvýrazni len tie (WFS → GeoJSON),
    # ale iba ak máme validný GeoJSON s aspoň jednou geometriou.
    if (parcels or '').strip() and isinstance(fc_geojson, dict):
        t = fc_geojson.get("type")
        has_feats = (t == "FeatureCollection" and bool(fc_geojson.get("features"))) or (t == "Feature")
        if has_feats:
            folium.GeoJson(
                fc_geojson,
                name="Vybrané parcely (WFS)",
                style_function=lambda _: {"color": "#0b5ed7", "weight": 3, "fill": False},
            ).add_to(m)

    st_folium(m, height=540, returned_objects=[])


# ----------------------------- HTTP & WFS ----------------------------------

def http_get_bytes(url: str, tries: int = 3) -> bytes:
    last_err: Exception | None = None
    for i in range(tries):
        try:
            r = requests.get(url, headers=HEADERS_XML, timeout=TIMEOUT)
            r.raise_for_status()
            return r.content
        except Exception as e:
            last_err = e
            import time
            time.sleep(0.6 * (i + 1))
    assert last_err is not None
    raise last_err


def xml_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def build_fes_filter(ku: str, parcels: List[str]) -> str:
    ku_part = (
        f'<PropertyIsLike wildCard="*" singleChar="." escape="!" matchCase="false">'
        f"<ValueReference>nationalCadastralReference</ValueReference><Literal>{xml_escape(ku)}*</Literal>"
        f"</PropertyIsLike>"
        if ku
        else ""
    )
    if parcels:
        inner_ors = []
        for p in parcels:
            p_xml = xml_escape(p)
            and_parts = [
                f"<PropertyIsEqualTo><ValueReference>label</ValueReference><Literal>{p_xml}</Literal></PropertyIsEqualTo>",
            ]
            if ku_part:
                and_parts.append(ku_part)
            inner_ors.append("<And>" + "".join(and_parts) + "</And>")
        return f'<Filter xmlns="http://www.opengis.net/fes/2.0"><Or>{"".join(inner_ors)}</Or></Filter>'
    if ku_part:
        return f'<Filter xmlns="http://www.opengis.net/fes/2.0">{ku_part}</Filter>'
    return ""


def build_cql_filter(ku: str, parcels: List[str]) -> str:
    parts = []
    if parcels:
        quoted = ",".join(["'" + p.replace("'", "''") + "'" for p in parcels if p])
        if quoted:
            parts.append(f"label IN ({quoted})")
    if ku:
        parts.append(f"nationalCadastralReference LIKE '{ku}%'")
    return " AND ".join(parts)


def has_any_feature(xml_bytes: bytes) -> bool:
    b = xml_bytes
    return (b.find(b"featureMember") != -1) or (b.find(b":member") != -1) or (b.find(b"<wfs:member") != -1)


@dataclass
class FetchResult:
    ok: bool
    note: str
    pages: List[bytes]
    first_url: str
    detected_epsg: Optional[str] = None


def gml_number_returned(xmlb: bytes) -> Optional[int]:
    m = re.search(rb'numberReturned="(\d+)"', xmlb)
    return int(m.group(1)) if m else None


def fetch_gml_pages(register: str, ku: str, parcels_csv: str, wfs_srs: Optional[str] = None) -> FetchResult:
    reg = (register or "").upper().strip()
    ku = (ku or "").strip()
    if not ku and not (parcels_csv or "").strip():
        return FetchResult(False, "Zadaj aspoň KU alebo parcelné čísla.", [], "")

    parcels = [p.strip() for p in re.split(r"[,;\s]+", parcels_csv or "") if p.strip()]
    base = CP_UO_WFS_BASE if reg == "E" else CP_WFS_BASE
    typename = TYPE_E if reg == "E" else TYPE_C

    from urllib.parse import urlencode

    fes = build_fes_filter(ku, parcels)
    if not fes:
        return FetchResult(False, "Neplatný filter (chýba KU aj parcely).", [], "")

    pages: List[bytes] = []
    start = 0
    first_url = ""
    dropped_srs = False

    while True:
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": typename,
            "count": str(PAGE_SIZE),
            "startIndex": str(start),
            "filter": fes,
        }
        if wfs_srs and not dropped_srs:
            params["srsName"] = wfs_srs

        url = f"{base}?{urlencode(params)}"
        if not first_url:
            first_url = url

        try:
            xmlb = http_get_bytes(url)
        except requests.HTTPError as e:
            sc = getattr(e.response, "status_code", None)

            if pages and sc == 400:
                break

            if (sc == 400 or sc is None) and wfs_srs and not dropped_srs:
                dropped_srs = True
                continue

            if sc == 400 and parcels:
                single_pages: List[bytes] = []
                for pval in parcels:
                    sp = {
                        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
                        "typeNames": typename, "count": "1000", "startIndex": "0",
                        "filter": build_fes_filter(ku, [pval]),
                    }
                    if not dropped_srs and wfs_srs:
                        sp["srsName"] = wfs_srs
                    surl = f"{base}?{urlencode(sp)}"
                    try:
                        sb = http_get_bytes(surl)
                        if has_any_feature(sb):
                            single_pages.append(sb)
                    except Exception:
                        pass
                if single_pages:
                    return FetchResult(True, f"Počet stránok: {len(single_pages)} (split-by-one)", single_pages, first_url)

            cql = build_cql_filter(ku, parcels)
            if cql:
                cql_params = {
                    "service": "WFS",
                    "version": "2.0.0",
                    "request": "GetFeature",
                    "typeNames": typename,
                    "count": str(PAGE_SIZE),
                    "startIndex": str(start),
                    "CQL_FILTER": cql,
                }
                if not dropped_srs and wfs_srs:
                    cql_params["srsName"] = wfs_srs
                cql_url = f"{base}?{urlencode(cql_params)}"
                if not first_url:
                    first_url = cql_url
                try:
                    xmlb = http_get_bytes(cql_url)
                except Exception as ee:
                    return FetchResult(False, f"HTTP chyba: {e}\nCQL fallback zlyhal: {ee}", [], first_url or url)
            else:
                return FetchResult(False, f"HTTP chyba: {e}", [], first_url or url)

        except Exception as e:
            return FetchResult(False, f"Chyba: {e}", [], first_url or url)

        nr = gml_number_returned(xmlb)
        if (nr is not None and nr == 0) or not has_any_feature(xmlb):
            break

        pages.append(xmlb)

        if nr is not None:
            if nr < PAGE_SIZE:
                break
            start += nr
        else:
            if len(xmlb) < 10000:
                break
            start += PAGE_SIZE

        if start > 500_000:
            break

    if not pages:
        return FetchResult(False, "Server vrátil 0 prvkov pre daný filter.", [], first_url)

    return FetchResult(True, f"Počet stránok: {len(pages)}", pages, first_url)

@st.cache_data(ttl=120, show_spinner=False)
def fetch_geojson_pages(
    register: str,
    resolved_ku: str,
    parcels_csv: str,
    wfs_srs: Optional[str] = None,
    *,
    page_size: int = PREVIEW_PAGE_SIZE,   # <<— nové
) -> "FetchResult":
    """Rýchlejšie stránkovanie pre náhľad (limit `page_size`)."""
    reg = (register or "").upper().strip()
    resolved_ku = (resolved_ku or "").strip()
    parcels = [p.strip() for p in re.split(r"[,;\s]+", parcels_csv or "") if p.strip()]
    base = CP_UO_WFS_BASE if reg == "E" else CP_WFS_BASE
    typename = TYPE_E if reg == "E" else TYPE_C

    filt_xml = build_fes_filter(resolved_ku, parcels)
    if not filt_xml:
        return FetchResult(False, "Neplatný filter (chýba KU aj parcely)", [], "")

    pages: List[bytes] = []
    start = 0
    first_url = ""

    with timed("wfs_geojson"):
        while True:
            params = {
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeNames": typename,
                "count": str(page_size),          # <<— menšie stránky
                "startIndex": str(start),
                "filter": filt_xml,
                "outputFormat": "application/json",
            }
            if wfs_srs:
                params["srsName"] = wfs_srs
            url = f"{base}?{urlencode(params)}"
            if not first_url:
                first_url = url
            try:
                jb = http_get_bytes(url)
            except requests.HTTPError as e:
                if pages and getattr(e.response, "status_code", None) == 400:
                    break
                return FetchResult(False, f"HTTP chyba: {e}", [], first_url or url)
            except Exception as e:
                return FetchResult(False, f"Chyba: {e}", [], first_url or url)
            try:
                obj = json.loads(jb.decode("utf-8", "ignore"))
                feats = obj.get("features", [])
            except Exception:
                feats = []
            if not feats:
                break
            pages.append(jb)
            # pre náhľad stačí prvá stránka (alebo málo stránok),
            # aby sme vedeli bbox a pár geometrií
            if len(pages) >= 2:
                break
            if len(feats) < page_size:
                break
            start += page_size
    if not pages:
        return FetchResult(False, "Server vrátil 0 prvkov pre daný filter.", [], first_url)
    return FetchResult(True, f"Počet stránok (náhľad): {len(pages)}", pages, first_url)

@st.cache_data(ttl=60, show_spinner=False)
def preview_geojson_autofallback(
    reg: str,
    ku: str,
    parcels: str,
    *,
    page_size: int = PREVIEW_PAGE_SIZE,
) -> "FetchResult":
    """Skús GeoJSON s rôznymi srsName; rýchlejšie, limitované stránky."""
    for srs in ("EPSG:4326", None, "EPSG:5514"):
        res = fetch_geojson_pages(reg, ku, parcels, wfs_srs=srs, page_size=page_size)
        if res.ok and res.pages:
            return res
    return FetchResult(False, "Prázdny výstup pre všetky srsName (4326/auto/5514).", [], "")

# --------- GeoJSON helpers pre mapový náhľad / bbox ---------

def merge_geojson_pages(pages: List[bytes], max_features: int = 8000):
    features: List[dict] = []
    total = 0
    for jb in pages:
        try:
            obj = json.loads(jb.decode("utf-8", "ignore"))
            feats = obj.get("features", [])
        except Exception:
            feats = []
        total += len(feats)
        if len(features) < max_features:
            room = max_features - len(features)
            features.extend(feats[:room])
        if len(features) >= max_features:
            break
    fc = {"type": "FeatureCollection", "features": features}
    return fc, total, len(features)

def _walk_coords(geom: dict, agg: List[float]):
    if not geom:
        return
    coords = geom.get("coordinates")
    def _rec(c):
        if isinstance(c, (list, tuple)):
            if c and isinstance(c[0], (int, float)) and isinstance(c[1], (int, float)):
                x, y = float(c[0]), float(c[1])
                agg[0] = min(agg[0], x); agg[1] = min(agg[1], y)
                agg[2] = max(agg[2], x); agg[3] = max(agg[3], y)
            else:
                for cc in c: _rec(cc)
    _rec(coords)

def bbox_from_geojson(obj: dict) -> Optional[Tuple[float, float, float, float]]:
    if not obj: return None
    agg = [float("inf"), float("inf"), float("-inf"), float("-inf")]
    if obj.get("type") == "FeatureCollection":
        for f in obj.get("features", []):
            _walk_coords((f or {}).get("geometry") or {}, agg)
    elif obj.get("type") == "Feature":
        _walk_coords((obj or {}).get("geometry") or {}, agg)
    else:
        _walk_coords(obj, agg)
    if agg[0] == float("inf"): return None
    return tuple(agg)

def view_from_bbox(bbox: Tuple[float, float, float, float]) -> Tuple[float, float, float]:
    minx, miny, maxx, maxy = bbox
    cx, cy = (minx + maxx)/2.0, (miny + maxy)/2.0
    span = max(maxx-minx, maxy-miny, 0.0005)
    import math
    zoom = max(2.0, min(18.0, math.log2(360.0/span)+1.0))
    return cy, cx, zoom


def wms_endpoint_and_layer(register: str) -> Tuple[str, str]:
    if (register or "").upper() == "E":
        return ("https://inspirews.skgeodesy.sk/geoserver/cp_uo/ows", "cp_uo:CP.CadastralParcelUO")
    else:
        return ("https://inspirews.skgeodesy.sk/geoserver/cp/ows", "cp:CP.CadastralParcel")


# ----------------------------- GDAL helpers --------------------------------

def _unique(seq: List[str]) -> List[str]:
    out: List[str] = []
    for x in seq:
        if x and x not in out:
            out.append(x)
    return out


def gdal_data_candidates() -> List[str]:
    cands: List[str] = []
    gd_env = os.environ.get("GDAL_DATA")
    if gd_env:
        cands.append(gd_env)

    ogr = shutil.which("ogr2ogr.exe") or shutil.which("ogr2ogr")
    if ogr:
        p = Path(ogr).resolve()
        root = p.parent.parent
        cands += [
            str(root / "share" / "gdal"),
            str(root / "apps" / "gdal" / "share" / "gdal"),
        ]

    for q in glob.glob(r"C:\\Program Files\\QGIS *"):
        base = Path(q)
        cands += [
            str(base / "share" / "gdal"),
            str(base / "apps" / "gdal" / "share" / "gdal"),
        ]

    for base in [r"C:\\OSGeo4W", r"C:\\OSGeo4W64"]:
        cands.append(str(Path(base) / "share" / "gdal"))

    try:
        import osgeo  # type: ignore
        cands.append(str(Path(osgeo.__file__).resolve().parent / "data"))
    except Exception:
        pass

    for pat in [
        "/usr/share/gdal",
        "/usr/local/share/gdal",
        "/opt/homebrew/share/gdal",
    ]:
        cands += glob.glob(pat)

    return _unique(cands)


def find_gdal_data_dir() -> Optional[str]:
    cands = gdal_data_candidates()
    for c in cands:
        if Path(c, "header.dxf").exists() and Path(c, "gml_registry.xml").exists():
            return c
    for c in cands:
        if Path(c).exists():
            return c
    return None


def ensure_gdal_data_env() -> Optional[str]:
    gd = find_gdal_data_dir()
    if gd:
        os.environ["GDAL_DATA"] = gd
        try:
            from osgeo import gdal  # type: ignore
            gdal.SetConfigOption("GDAL_DATA", gd)
        except Exception:
            pass
    return gd


def find_ogr2ogr_windows() -> Optional[str]:
    candidates = [
        r"C:\\Program Files\\QGIS*\\bin\\ogr2ogr.exe",
        r"C:\\Program Files\\QGIS*\\apps\\gdal*\\bin\\ogr2ogr.exe",
        r"C:\\OSGeo4W64\\bin\\ogr2ogr.exe",
        r"C:\\OSGeo4W\\bin\\ogr2ogr.exe",
        r"C:\\Program Files\\GDAL\\ogr2ogr.exe",
    ]
    for pat in candidates:
        hits = sorted(glob.glob(pat), reverse=True)
        if hits:
            return hits[0]
    return None


def ensure_gdal() -> Tuple[str, object]:
    try:
        from osgeo import gdal as _gdal  # type: ignore
        return ("python-gdal", _gdal)
    except Exception:
        pass
    ogr = shutil.which("ogr2ogr") or shutil.which("ogr2ogr.exe") or find_ogr2ogr_windows()
    if ogr:
        return ("ogr2ogr-cli", ogr)
    raise RuntimeError(
        "GDAL nie je k dispozícii. Nainštaluj Python balík 'GDAL' (pip install GDAL) "
        "alebo QGIS/OSGeo4W a pridaj 'ogr2ogr' do PATH, prípadne zvoľ výstup 'gml-zip'."
    )


def run_ogr(ogr_path: str, args: List[str]) -> None:
    env = os.environ.copy()
    if not env.get("GDAL_DATA"):
        gd = ensure_gdal_data_env()
        if gd:
            env["GDAL_DATA"] = gd
    proc = subprocess.run([ogr_path] + args, capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"ogr2ogr failed: {args}")


GDAL_DATA_DIR = ensure_gdal_data_env()


# ----------------------------- Konverzie (GDAL) ----------------------------

def convert_pages_with_gdal(pages: List[bytes], driver: str, out_ext: str) -> Tuple[bytes, str, str]:
    mode, handle = ensure_gdal()
    with tempfile.TemporaryDirectory() as td:
        gml_paths: List[str] = []
        for i, b in enumerate(pages, 1):
            p = os.path.join(td, f"page_{i:03d}.gml")
            with open(p, "wb") as f:
                f.write(b)
            gml_paths.append(p)

        if driver == "ESRI Shapefile":
            gpkg_path = os.path.join(td, "merge_for_shp.gpkg")
            layer_name = "parcely"

            if mode == "python-gdal":
                gdal = handle  # type: ignore
                gdal.UseExceptions()
                opts = gdal.VectorTranslateOptions(
                    format="GPKG",
                    layerName=layer_name,
                    geometryType="MULTIPOLYGON",
                    explodeCollections=True,
                )
                gdal.VectorTranslate(destNameOrDestDS=gpkg_path, srcDS=gml_paths[0], options=opts)
                for p in gml_paths[1:]:
                    opts_app = gdal.VectorTranslateOptions(
                        format="GPKG",
                        layerName=layer_name,
                        accessMode="append",
                        geometryType="MULTIPOLYGON",
                        explodeCollections=True,
                    )
                    gdal.VectorTranslate(destNameOrDestDS=gpkg_path, srcDS=p, options=opts_app)
                shp_path = os.path.join(td, "parcely.shp")
                opts_shp = gdal.VectorTranslateOptions(format="ESRI Shapefile", layerName=layer_name)
                gdal.VectorTranslate(destNameOrDestDS=shp_path, srcDS=gpkg_path, options=opts_shp)

            else:
                ogr = handle
                run_ogr(ogr, ["-f", "GPKG", gpkg_path, gml_paths[0],
                              "-nln", layer_name, "-nlt", "MULTIPOLYGON", "-explodecollections"])
                for p in gml_paths[1:]:
                    run_ogr(ogr, ["-f", "GPKG", gpkg_path, p,
                                  "-nln", layer_name, "-update", "-append",
                                  "-nlt", "MULTIPOLYGON", "-explodecollections"])
                shp_path = os.path.join(td, "parcely.shp")
                run_ogr(ogr, ["-f", "ESRI Shapefile", shp_path, gpkg_path, "-nln", layer_name])

            mem = io.BytesIO()
            import zipfile
            with zipfile.ZipFile(mem, mode="w") as z:
                base = os.path.splitext(shp_path)[0]
                for ext in (".shp", ".shx", ".dbf", ".prj", ".cpg"):
                    fp = base + ext
                    if os.path.exists(fp):
                        z.write(fp, arcname=os.path.basename(fp))
            return mem.getvalue(), "application/zip", mode

        if driver == "DXF":
            gpkg_path = os.path.join(td, "merge.gpkg")
            layer_name = "parcely"
            out_path = os.path.join(td, "parcely.dxf")

            if mode == "python-gdal":
                gdal = handle  # type: ignore
                gdal.UseExceptions()
                opts = gdal.VectorTranslateOptions(format="GPKG", layerName=layer_name)
                gdal.VectorTranslate(destNameOrDestDS=gpkg_path, srcDS=gml_paths[0], options=opts)
                for p in gml_paths[1:]:
                    opts_app = gdal.VectorTranslateOptions(format="GPKG", layerName=layer_name, accessMode="append")
                    gdal.VectorTranslate(destNameOrDestDS=gpkg_path, srcDS=p, options=opts_app)
                opts_dxf = gdal.VectorTranslateOptions(format="DXF", layerName=layer_name)
                gdal.VectorTranslate(destNameOrDestDS=out_path, srcDS=gpkg_path, options=opts_dxf)
            else:
                ogr = handle
                run_ogr(ogr, ["-f", "GPKG", gpkg_path, gml_paths[0], "-nln", layer_name])
                for p in gml_paths[1:]:
                    run_ogr(ogr, ["-f", "GPKG", gpkg_path, p, "-nln", layer_name, "-update", "-append"])
                run_ogr(ogr, ["-f", "DXF", out_path, gpkg_path, "-nln", layer_name])

            return open(out_path, "rb").read(), "application/dxf", mode

        out_path = os.path.join(td, f"parcely{out_ext}")
        if mode == "python-gdal":
            gdal = handle  # type: ignore
            gdal.UseExceptions()
            opts = gdal.VectorTranslateOptions(format=driver, layerName="parcely")
            gdal.VectorTranslate(destNameOrDestDS=out_path, srcDS=gml_paths[0], options=opts)
            for p in gml_paths[1:]:
                opts_app = gdal.VectorTranslateOptions(format=driver, layerName="parcely", accessMode="append")
                try:
                    gdal.VectorTranslate(destNameOrDestDS=out_path, srcDS=p, options=opts_app)
                except Exception:
                    pass
        else:
            ogr = handle
            run_ogr(ogr, ["-f", driver, out_path, gml_paths[0], "-nln", "parcely"])
            for p in gml_paths[1:]:
                try:
                    run_ogr(ogr, ["-f", driver, out_path, p, "-nln", "parcely", "-update", "-append"])
                except Exception:
                    pass

        mime = {
            ".geojson": "application/geo+json",
            ".gpkg": "application/geopackage+sqlite3",
        }.get(out_ext, "application/octet-stream")
        return open(out_path, "rb").read(), mime, mode


def geojson_pages_to_dxf(json_pages: List[bytes]) -> Tuple[bytes, str]:
    import ezdxf
    import tempfile
    import json
    import os

    doc = ezdxf.new(setup=True)
    msp = doc.modelspace()
    LAYER = "PARCELY"
    if LAYER not in doc.layers:
        doc.layers.new(name=LAYER)

    def add_polygon(coords):
        for ring in coords:
            pts = [(float(x), float(y)) for x, y in ring]
            if not pts:
                continue
            if pts[0] != pts[-1]:
                pts.append(pts[0])
            msp.add_lwpolyline(pts, format="xy", dxfattribs={"layer": LAYER, "closed": True})

    for jb in json_pages:
        obj = json.loads(jb.decode("utf-8", "ignore"))
        for f in obj.get("features", []):
            g = (f or {}).get("geometry") or {}
            t = g.get("type")
            if t == "Polygon":
                add_polygon(g.get("coordinates", []))
            elif t == "MultiPolygon":
                for poly in g.get("coordinates", []):
                    add_polygon(poly)

    with tempfile.TemporaryDirectory() as td:
        out_path = os.path.join(td, "parcely.dxf")
        try:
            doc.saveas(out_path)
        except AttributeError:
            doc.save()
        with open(out_path, "rb") as f:
            data = f.read()
    return data, "application/dxf"
def gml_pages_to_dxf(gml_pages: List[bytes]) -> Tuple[bytes, str]:
    """
    Minimalistický fallback: vyparsuje gml:posList/gml:pos z GML a vykreslí
    uzavreté polyline do DXF. Nevyžaduje GDAL ani nové sieťové volania.
    """
    import xml.etree.ElementTree as ET
    import ezdxf

    ns = {"gml": "http://www.opengis.net/gml"}
    doc = ezdxf.new(setup=True)
    msp = doc.modelspace()
    LAYER = "PARCELY"
    if LAYER not in doc.layers:
        doc.layers.new(name=LAYER)

    def parse_poslist(txt: str) -> list[tuple[float, float]]:
        vals = [float(x) for x in re.split(r"[ ,\s]+", (txt or "").strip()) if x]
        pts = [(vals[i], vals[i+1]) for i in range(0, len(vals) - 1, 2)]
        if pts and pts[0] != pts[-1]:
            pts.append(pts[0])
        return pts

    def add_ring(pts: list[tuple[float, float]]):
        if len(pts) >= 4:
            msp.add_lwpolyline(pts, format="xy", dxfattribs={"layer": LAYER, "closed": True})

    for b in gml_pages:
        try:
            root = ET.fromstring(b)
        except Exception:
            continue

        # Polygon/MultiSurface s posList (najčastejší prípad)
        for poslist in root.findall(".//gml:posList", ns):
            add_ring(parse_poslist(poslist.text or ""))

        # Rezerva: ak by boli len jednotlivé gml:pos (menej časté)
        rings = []
        cur = []
        for pos in root.findall(".//gml:pos", ns):
            parts = [float(x) for x in (pos.text or "").replace(",", " ").split() if x]
            if len(parts) >= 2:
                cur.append((parts[0], parts[1]))
            # uzavretie prstenca heuristikou
            if len(cur) >= 4 and cur[0] == cur[-1]:
                rings.append(cur); cur = []
        if cur:
            rings.append(cur)
        for r in rings:
            if r and r[0] != r[-1]:
                r = r + [r[0]]
            add_ring(r)

    with tempfile.TemporaryDirectory() as td:
        out_path = os.path.join(td, "parcely.dxf")
        try:
            doc.saveas(out_path)
        except AttributeError:
            doc.save()
        with open(out_path, "rb") as f:
            data = f.read()
    return data, "application/dxf"

# ----------------------------- Streamlit UI --------------------------------

st.set_page_config(page_title="ParcelOne – WFS GML", layout="wide")
st.title("ParcelOne – Sťahuj geometrie KN vo vybranom formáte")

with st.sidebar:
    reg = st.selectbox("Register", ["E", "C"], index=0)
    col_ku1, col_ku2 = st.columns(2)
    with col_ku1:
        ku_code = st.text_input("Katastrálne územie – kód", placeholder="napr. 808156")
    with col_ku2:
        ku_name = st.text_input("...alebo názov", placeholder="napr. Bratislava-Staré Mesto")
    parcels = st.text_area("Parcelné čísla (voliteľné)", placeholder="napr. 1234/1, 1234/2")
    fmt = st.selectbox("Výstupový formát", ["gml-zip", "geojson", "shp", "dxf", "gpkg"], index=0)
    btn = st.button("Stiahnuť parcely", type="primary")
    crs_label = st.selectbox("CRS (WFS srsName)", list(WFS_CRS_CHOICES.keys()), index=1)
    wfs_srs = WFS_CRS_CHOICES[crs_label]
    st.markdown(
    """
    **Kontakt**
    📞 [+421 948 955 128](tel:+421948955128)
    ✉️ [svitokerik02@gmail.com](mailto:svitokerik02@gmail.com)
    """
    )

col1, col2 = st.columns([2, 1])

ku_table = load_ku_table()

resolved_ku = (ku_code or "").strip()
ku_suggestions: list[dict] = []
if not resolved_ku:
    resolved_ku, ku_suggestions = lookup_ku_code(ku_table, ku_name or "")

# soft-pick do náhľadu: ak presná zhoda nie je, ber prvý návrh
soft_pick = None
if not resolved_ku and ku_suggestions:
    soft_pick = ku_suggestions[0]

# hlavička s info + klikateľné návrhy (max 5)
if ku_name and not resolved_ku:
    st.info("Nenašiel som presnú zhodu.")
if ku_suggestions:
    cols = st.columns(min(5, len(ku_suggestions)))
    for i, it in enumerate(ku_suggestions[:5]):
        label = f"{it['name']} ({it['code']})"
        if cols[i].button(label, key=f"pick_ku_{it['code']}"):
            resolved_ku = it['code']
            ku_name = it['name']
            soft_pick = it


if ku_name and resolved_ku:
    st.caption(f"Vybrané KU: {ku_name} → kód **{resolved_ku}**")

# ─────────────────────────────────────────────────────────────
# NÁHĽAD MAPY (UI HOOK – PREVIEW)  ⟵ vlož NAD tlačidlo "Stiahnuť parcely"
# ─────────────────────────────────────────────────────────────
do_preview_ku = resolved_ku or (soft_pick['code'] if isinstance(soft_pick, dict) and soft_pick.get('code') else "")
if do_preview_ku or (parcels or '').strip():
    col1, = st.columns(1)
    with col1:
        bbox = None
        fc = {}

        # 1) Bez zadaných parciel → rýchly náhľad len KU (WMS + hranica KU), bez WFS
        if not (parcels or '').strip():
            with timed('zone_bbox'):
                bbox = fetch_zone_bbox(reg, do_preview_ku)
            if not bbox:
                bbox = (17.0, 48.0, 17.01, 48.01)
            show_map_preview(reg, {}, bbox, ku=do_preview_ku, parcels="")

        # 2) Parcely sú zadané → malá WFS vzorka, zvýrazníme len tie
        else:
            with st.spinner("Pripravujem výber parciel…"):
                with timed('wfs_preview'):
                    gj = preview_geojson_autofallback(reg, do_preview_ku, parcels, page_size=PREVIEW_PAGE_SIZE)
            if gj.ok and gj.pages:
                fc, total, used = merge_geojson_pages(gj.pages, max_features=PREVIEW_MAX_FEATURES)
                bbox = bbox_from_geojson(fc)
            # ak nič neprišlo, nezvýrazňuj parcely – len KU
            parcels_for_map = parcels if (gj.ok and gj.pages and fc and fc.get("features")) else ""
            if not bbox:
                with timed('zone_bbox_fallback'):
                    bbox = fetch_zone_bbox(reg, do_preview_ku) or (17.0, 48.0, 17.01, 48.01)
            show_map_preview(reg, fc if parcels_for_map else {}, bbox, ku=do_preview_ku, parcels=parcels_for_map)


    # Voliteľné: ukáž profilovanie krokov
    if DEBUG_PROFILE and _step_times:
        with st.expander("Profilovanie náhľadu"):
            for k, v in _step_times.items():
                st.write(f"{k}: {v*1000:.0f} ms")

    if not (resolved_ku or parcels.strip()):
        st.error("Zadaj KU (kód alebo názov) alebo aspoň jedno parcelné číslo.")
        st.stop()

    with st.spinner("Naťahujem GML stránky z WFS…"):
        result = fetch_gml_pages(reg, resolved_ku or "", parcels, wfs_srs=wfs_srs)


    with col1:
        st.success("Parcely pripravené.")

        import zipfile
        mem_zip = io.BytesIO()
        with zipfile.ZipFile(mem_zip, mode="w") as zf:
            for i, b in enumerate(result.pages, 1):
                zf.writestr(f"parcely_{i:03d}.gml", b)
        gml_zip = mem_zip.getvalue()

        if fmt == "gml-zip":
            st.download_button(
                "Stiahnuť GML (ZIP)",
                data=gml_zip,
                file_name=f"parcely_{reg}_{resolved_ku or 'filter'}.zip",
                mime="application/zip",
            )
        else:
            try:
                if fmt == "geojson":
                    data, mime, conv_src = convert_pages_with_gdal(result.pages, "GeoJSON", ".geojson")
                    st.download_button("Stiahnuť GeoJSON", data=data,
                                       file_name=f"parcely_{reg}_{resolved_ku or 'filter'}.geojson", mime=mime)
                    st.caption(f"Konverzia: {conv_src}")

                elif fmt == "shp":
                    data, mime, conv_src = convert_pages_with_gdal(result.pages, "ESRI Shapefile", ".shp")
                    st.download_button("Stiahnuť SHP (ZIP)", data=data,
                                       file_name=f"parcely_{reg}_{resolved_ku or 'filter'}.zip", mime=mime)
                    st.caption(f"Konverzia: {conv_src}")

                elif fmt == "dxf":
                    try:
                        # 1) GDAL → DXF (ak je k dispozícii)
                        data, mime, conv_src = convert_pages_with_gdal(result.pages, "DXF", ".dxf")
                        st.download_button("Stiahnuť DXF", data=data,
                                           file_name=f"parcely_{reg}_{resolved_ku or 'filter'}.dxf", mime=mime)
                        st.caption(f"Konverzia: {conv_src}")
                    except Exception:
                        # 2) Bez siete: rovno z GML stránok → DXF
                        try:
                            st.info("GDAL nie je dostupný – konvertujem priamo z GML (bez ďalších requestov).")
                            dxf_bytes, mime = gml_pages_to_dxf(result.pages)
                            st.download_button("Stiahnuť DXF", data=dxf_bytes,
                                               file_name=f"parcely_{reg}_{resolved_ku or 'filter'}.dxf", mime=mime)
                            st.caption("Konverzia: čistý Python (GML → DXF)")
                        except Exception:
                            # 3) Posledný pokus: GeoJSON fallback (môže trvať dlhšie / timeout)
                            st.info("Skúšam ešte GeoJSON fallback…")
                            try:
                                gj_res = fetch_geojson_pages(reg, resolved_ku or "", parcels, wfs_srs=wfs_srs)
                                if not gj_res.ok:
                                    raise RuntimeError(gj_res.note or "GeoJSON nevrátil dáta.")
                                dxf_bytes, mime = geojson_pages_to_dxf(gj_res.pages)
                                st.download_button("Stiahnuť DXF", data=dxf_bytes,
                                                   file_name=f"parcely_{reg}_{resolved_ku or 'filter'}.dxf", mime=mime)
                                st.caption("Konverzia: čistý Python (GeoJSON → DXF)")
                            except Exception as e:
                                st.error(f"DXF sa nepodarilo pripraviť: {e}")
                                st.download_button("Stiahnuť GML (ZIP)", data=gml_zip,
                                                   file_name=f"parcely_{reg}_{resolved_ku or 'filter'}.zip",
                                                   mime="application/zip")


                elif fmt == "gpkg":
                    data, mime, conv_src = convert_pages_with_gdal(result.pages, "GPKG", ".gpkg")
                    st.download_button("Stiahnuť GPKG", data=data,
                                       file_name=f"parcely_{reg}_{resolved_ku or 'filter'}.gpkg", mime=mime)
                    st.caption(f"Konverzia: {conv_src}")

            except Exception as e:
                st.error(str(e))

# --- voliteľná diagnostika (default vypnutá) -------------------------------
if DEBUG_UI and "result" in locals():
    with col2:
        st.subheader("Diagnostika")
        pages_val = len(getattr(result, "pages", []) or [])
        dbg_lines = [
            f"register={reg}",
            f"resolved_ku={resolved_ku}",
            f"parcely={parcels}",
            f"wfs_srs={wfs_srs or 'auto'}",
            f"pages={pages_val}",
            f"GDAL_DATA={os.environ.get('GDAL_DATA') or GDAL_DATA_DIR or '-'}",
        ]
        st.code("\n".join(dbg_lines), language="text")






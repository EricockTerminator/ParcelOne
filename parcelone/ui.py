from __future__ import annotations
from .ku_index import code_for
import io
import os
import re
import sys
import zipfile
from typing import List, Optional, Tuple

import folium
from streamlit_folium import st_folium
import streamlit as st
import unicodedata

from .wfs import (
    fetch_zone_bbox,
    preview_geojson_autofallback,
    merge_geojson_pages,
    bbox_from_geojson,
    fetch_gml_pages,
    fetch_geojson_pages,
    WFS_CRS_CHOICES,
    _step_times,
    DEBUG_PROFILE,
    PREVIEW_PAGE_SIZE,
    PREVIEW_MAX_FEATURES,
)
from .converters import (
    convert_pages_with_gdal,
    gml_pages_to_dxf,
    geojson_pages_to_dxf,
    GDAL_DATA_DIR,
)

# ----------------------------- Konštanty ------------------------------------
WMS_URL_C = "https://inspirews.skgeodesy.sk/geoserver/cp/ows"
WMS_URL_E = "https://inspirews.skgeodesy.sk/geoserver/cp_uo/ows"
LAYER_C = "cp:CP.CadastralParcel"
LAYER_E = "cp_uo:CP.CadastralParcelUO"
ZONE_C = "cp:CP.CadastralZoning"
ZONE_E = "cp_uo:CP.CadastralZoningUO"
DEBUG_UI = False

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

from .ku_index import code_for  # resolves KU code from name or returns candidates


def _ku_code_input(label: str = "Katastrálne územie – kód alebo názov") -> str:
    """Streamlit input that accepts 6-digit KU code or KU name.
    Returns a resolved 6-digit code or stops the app with an error.
    """
    import streamlit as st  # local import to avoid circular issues at import time

    ku_input = st.text_input(label, value="", placeholder="napr. 801062 alebo Banská Bystrica").strip()
    if not ku_input:
        st.info("Zadaj kód KU (6 číslic) alebo názov katastrálneho územia.")
        st.stop()

    ku_code, candidates = code_for(ku_input)

    if candidates:
        # ambiguous name -> let user choose
        options = {f"{c.name} ({c.code})": c.code for c in candidates}
        choice = st.selectbox("Našli sme viac KU – vyber jedno:", list(options.keys()))
        ku_code = options[choice]

    if not ku_code:
        st.error("Neplatný kód alebo názov KU. Skús znova.")
        st.stop()

    return ku_code


ku_code = _ku_code_input()
bbox = fetch_zone_bbox(register, ku_code, retries=RETRIES)

ku_code, candidates = code_for(ku_input)


if candidates: # ambiguous -> let user choose
    label = "Našli sme viac KU s rovnakým názvom — vyber jedno"
    options = {f"{c.name} ({c.code})": c.code for c in candidates}
    choice = st.selectbox(label, list(options))
    ku_code = options[choice]

if not ku_code:
    st.error("Zadaj 6‑miestny kód KU alebo platný názov.")
    st.stop()
    
# keep exactly one copy of these helpers in your file

def _parse_ku_line(line: str):
    m = re.search(r'"(.+?)"\s+(\d+)', line)
    if m:
        return m.group(2), m.group(1)
    return None, None


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


def _build_cql_for_preview(ku: str, parcels_csv: str) -> str:
    parts = []
    ku = (ku or "").strip()
    if ku:
        parts.append(f"nationalCadastralReference LIKE '{ku}%'")
    parcels = [p.strip() for p in re.split(r"[,;\s]+", parcels_csv or "") if p.strip()]
    if parcels:
        quoted = ",".join(["'" + p.replace("'", "''") + "'" for p in parcels if p])
        if quoted:
            parts.append(f"label IN ({quoted})")
    return " AND ".join(parts)


def _cql_for_zone(ku: str) -> str:
    if not ku:
        return ""
    return f"nationalCadastralReference='{ku}'"


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

    cql_parc = _build_cql_for_preview(ku, "")
    p = dict(layers=layer, fmt="image/png", transparent=True, overlay=True, control=False, version="1.3.0",
             attr="© GKÚ SR / INSPIRE")
    if cql_parc:
        p["CQL_FILTER"] = cql_parc
    folium.raster_layers.WmsTileLayer(url=url, name="Parcely (WMS)", **p).add_to(m)

    cql_zone = _cql_for_zone(ku)
    zp = dict(layers=zone, fmt="image/png", transparent=True, overlay=True, control=False, version="1.3.0",
              attr="© GKÚ SR / INSPIRE", opacity=0.8)
    if cql_zone:
        zp["CQL_FILTER"] = cql_zone
    folium.raster_layers.WmsTileLayer(url=url, name="Hranica KU", **zp).add_to(m)

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

# ----------------------------- Streamlit UI ---------------------------------

def main():
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

    soft_pick = None
    if not resolved_ku and ku_suggestions:
        soft_pick = ku_suggestions[0]

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

    do_preview_ku = resolved_ku or (soft_pick['code'] if isinstance(soft_pick, dict) and soft_pick.get('code') else "")
    if do_preview_ku or (parcels or '').strip():
        col1, = st.columns(1)
        with col1:
            bbox = None
            fc = {}

            if not (parcels or '').strip():
                bbox = fetch_zone_bbox(reg, do_preview_ku)
                if not bbox:
                    bbox = (17.0, 48.0, 17.01, 48.01)
                show_map_preview(reg, {}, bbox, ku=do_preview_ku, parcels="")
            else:
                with st.spinner("Pripravujem výber parciel…"):
                    gj = preview_geojson_autofallback(reg, do_preview_ku, parcels, page_size=PREVIEW_PAGE_SIZE)
                if gj.ok and gj.pages:
                    fc, total, used = merge_geojson_pages(gj.pages, max_features=PREVIEW_MAX_FEATURES)
                    bbox = bbox_from_geojson(fc)
                parcels_for_map = parcels if (gj.ok and gj.pages and fc and fc.get("features")) else ""
                if not bbox:
                    bbox = fetch_zone_bbox(reg, do_preview_ku) or (17.0, 48.0, 17.01, 48.01)
                show_map_preview(reg, fc if parcels_for_map else {}, bbox, ku=do_preview_ku, parcels=parcels_for_map)

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
                            data, mime, conv_src = convert_pages_with_gdal(result.pages, "DXF", ".dxf")
                            st.download_button("Stiahnuť DXF", data=data,
                                               file_name=f"parcely_{reg}_{resolved_ku or 'filter'}.dxf", mime=mime)
                            st.caption(f"Konverzia: {conv_src}")
                        except Exception:
                            try:
                                st.info("GDAL nie je dostupný – konvertujem priamo z GML (bez ďalších requestov).")
                                dxf_bytes, mime = gml_pages_to_dxf(result.pages)
                                st.download_button("Stiahnuť DXF", data=dxf_bytes,
                                                   file_name=f"parcely_{reg}_{resolved_ku or 'filter'}.dxf", mime=mime)
                                st.caption("Konverzia: čistý Python (GML → DXF)")
                            except Exception:
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

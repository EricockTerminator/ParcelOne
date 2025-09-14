# file: parcelone/parcelone/ui.py
"""Streamlit UI (no forms). Simpler flow, working submit + KU lookup + DXF export."""
import json
from io import BytesIO
from zipfile import ZipFile
from typing import Dict, Iterable, List, Tuple

import streamlit as st

from .ku_index import code_for
from .wfs import (
    fetch_zone_bbox,
    preview_geojson_autofallback,
    merge_geojson_pages,
    fetch_geojson_pages,
    fetch_gml_pages,
)
from .config import WFS_CRS_CHOICES

RETRIES = 3


# --------------------------- helpers ----------------------------------------

def _normalize_crs_choices(choices) -> Tuple[List[str], Dict[str, str]]:
    """Return (options, label_map) for selectbox from flexible choices input.
    Accepts dict {key: label} or iterable of (key, label[, ...]) or simple list of keys.
    """
    options: List[str] = []
    label_map: Dict[str, str] = {}
    if isinstance(choices, dict):
        options = list(choices.keys())
        label_map = {k: str(v) for k, v in choices.items()}
        return options, label_map
    for item in choices:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            key = str(item[0])
            label = str(item[1])
        else:
            key = str(item)
            label = key
        options.append(key)
        label_map[key] = label
    return options, label_map


def _resolve_ku(ku_input: str) -> str | None:
    ku_input = (ku_input or "").strip()
    if not ku_input:
        return None
    code, cands = code_for(ku_input)
    if cands:
        options = {f"{c.name} ({c.code})": c.code for c in cands}
        choice = st.selectbox("Našli sme viac KU – vyber jedno:", list(options), key="ku_choice")
        code = options.get(choice)
    return code


def _download_geojson(pages: list[bytes], ku_code: str) -> None:
    gj, total, used = merge_geojson_pages(pages)
    payload = json.dumps(gj, ensure_ascii=False).encode("utf-8")
    st.success(f"GeoJSON pripravený – vybrané: {used} / spolu: {total}")
    st.download_button(
        "Stiahnuť GeoJSON",
        payload,
        file_name=f"parcely_{ku_code}.geojson",
        mime="application/geo+json",
    )


def _download_gml_zip(pages: list[bytes], ku_code: str) -> None:
    buf = BytesIO()
    with ZipFile(buf, "w") as zf:
        for i, page in enumerate(pages, start=1):
            zf.writestr(f"parcely_{i}.gml", page)
    buf.seek(0)
    st.success(f"GML ZIP pripravený – stránok: {len(pages)}")
    st.download_button(
        "Stiahnuť GML (.zip)",
        buf.read(),
        file_name=f"parcely_{ku_code}.zip",
        mime="application/zip",
    )


def _download_dxf_from_geojson(pages: list[bytes], ku_code: str) -> None:
    try:
        import ezdxf  # type: ignore
    except Exception:
        st.error("DXF export vyžaduje balík `ezdxf`. Pridaj `ezdxf>=1.1` do requirements.txt a redeploy.")
        return

    gj, total, used = merge_geojson_pages(pages)
    feats = gj.get("features", []) if isinstance(gj, dict) else []

    doc = ezdxf.new()
    msp = doc.modelspace()

    def add_polyline(coords: Iterable[Iterable[float]], *, layer: str) -> None:
        pts = [(float(x), float(y)) for x, y in coords]
        if len(pts) >= 2:
            msp.add_lwpolyline(pts, format="xy", close=True, dxfattribs={"layer": layer})

    def add_linestring(coords: Iterable[Iterable[float]], *, layer: str) -> None:
        pts = [(float(x), float(y)) for x, y in coords]
        if len(pts) >= 2:
            msp.add_lwpolyline(pts, format="xy", dxfattribs={"layer": layer})

    def add_point(coord: Iterable[float], *, layer: str) -> None:
        x, y = coord
        msp.add_point((float(x), float(y)), dxfattribs={"layer": layer})

    for f in feats:
        if not isinstance(f, dict):
            continue
        g = f.get("geometry") or {}
        gtype = g.get("type")
        coords = g.get("coordinates")
        if gtype == "Polygon":
            if coords:
                rings = coords  # [exterior, holes...]
                add_polyline(rings[0], layer="PARCEL_POLY")
                for ring in rings[1:]:
                    add_polyline(ring, layer="PARCEL_HOLE")
        elif gtype == "MultiPolygon":
            for poly in coords or []:
                if poly:
                    add_polyline(poly[0], layer="PARCEL_POLY")
                    for ring in poly[1:]:
                        add_polyline(ring, layer="PARCEL_HOLE")
        elif gtype == "LineString":
            add_linestring(coords or [], layer="PARCEL_LINE")
        elif gtype == "MultiLineString":
            for ls in coords or []:
                add_linestring(ls, layer="PARCEL_LINE")
        elif gtype == "Point":
            add_point(coords or (0, 0), layer="PARCEL_POINT")
        elif gtype == "MultiPoint":
            for pt in coords or []:
                add_point(pt, layer="PARCEL_POINT")

    buf = BytesIO()
    doc.write(buf)
    buf.seek(0)
    st.success(f"DXF pripravený – prvkov: {used}")
    st.download_button(
        "Stiahnuť DXF",
        buf.getvalue(),
        file_name=f"parcely_{ku_code}.dxf",
        mime="image/vnd.dxf",
    )


# ----------------------------- main -----------------------------------------

def main() -> None:
    st.set_page_config(page_title="ParcelOne", layout="wide")
    st.title("ParcelOne – Sťahuj geometrie KN vo vybranom formáte")

    col1, col2 = st.columns([1, 1])
    with col1:
        register = st.selectbox("Register", ["E", "C"], index=0)
        ku_raw = st.text_input("Katastrálne územie – kód alebo názov", placeholder="napr. 801062 alebo Banská Bystrica")
        parcels_csv = st.text_area("Parcelné čísla (voliteľné)", placeholder="napr. 1234/1, 456/2")
    with col2:
        # Robust CRS select from various shapes of WFS_CRS_CHOICES
        options, label_map = _normalize_crs_choices(WFS_CRS_CHOICES)
        wfs_crs = st.selectbox(
            "CRS (WFS srsName)",
            options=options,
            format_func=lambda k: label_map.get(k, k),
        )
        do_preview_ku = st.checkbox("Zobraziť náhľad KU (bbox)", value=False)
        output_format = st.selectbox("Výstupový formát", ["gml-zip", "geojson", "dxf"], index=0)

    ku_code = _resolve_ku(ku_raw)

    if do_preview_ku and ku_code:
        bbox = fetch_zone_bbox(register, ku_code, retries=RETRIES)
        if bbox:
            st.caption("BBOX (minx, miny, maxx, maxy)")
            st.code(repr(bbox))
        else:
            st.warning("Nepodarilo sa načítať BBOX pre zadané KU.")

    clicked = st.button("Stiahnuť parcely", type="primary")
    if not clicked:
        return

    if not ku_code:
        st.error("Zadaj kód KU alebo platný názov a prípadne vyber zo zoznamu.")
        return

    if output_format == "geojson":
        res = fetch_geojson_pages(register, ku_code, parcels_csv, wfs_srs=wfs_crs, retries=RETRIES)
        if not res.ok:
            st.error(res.note)
            prev = preview_geojson_autofallback(register, ku_code, parcels_csv, wfs_srs=wfs_crs, retries=RETRIES)
            if prev.ok:
                st.info("Náhľad úspešný s fallbackom – server pre GeoJSON môže byť náladový.")
                _download_geojson(prev.pages, ku_code)
            return
        _download_geojson(res.pages, ku_code)
    elif output_format == "gml-zip":
        res = fetch_gml_pages(register, ku_code, parcels_csv, wfs_srs=wfs_crs, retries=RETRIES)
        if not res.ok:
            st.error(res.note)
            return
        _download_gml_zip(res.pages, ku_code)
    else:  # dxf
        # Fetch as GeoJSON first, then convert to DXF locally
        res = fetch_geojson_pages(register, ku_code, parcels_csv, wfs_srs=wfs_crs, retries=RETRIES)
        if not res.ok:
            st.error(res.note)
            return
        _download_dxf_from_geojson(res.pages, ku_code)


if __name__ == "__main__":
    main()

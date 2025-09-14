# file: parcelone/parcelone/ui.py
"""Streamlit UI (no forms). KU lookup + robust CRS select + WMS preview + exports.

This version adds a *compatibility wrapper* around `fetch_zone_bbox` so the UI
works with both the old signature `(register, ku, *, retries=...)` and the new
one `(register, ku, *, wfs_srs=..., retries=...)`.
"""
import os
import re
import json
from io import BytesIO
from zipfile import ZipFile
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Dict, Iterable, List, Tuple, Optional

import streamlit as st

from .ku_index import code_for
from .wfs import (
    fetch_zone_bbox,
    preview_geojson_autofallback,
    merge_geojson_pages,
    fetch_geojson_pages,
    fetch_gml_pages,
    ZONE_C,
    ZONE_E,
)
from .config import WFS_CRS_CHOICES, CP_WFS_BASE, CP_UO_WFS_BASE

RETRIES = 3  # used only in bbox helper

# --------------------------- helpers ----------------------------------------

def _normalize_crs_choices(choices) -> Tuple[List[str], Dict[str, str]]:
    options: List[str] = []
    label_map: Dict[str, str] = {}
    if isinstance(choices, dict):
        for k, v in choices.items():
            k = str(k); options.append(k); label_map[k] = str(v)
        return options, label_map
    for item in choices:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            key = str(item[0]); label = str(item[1])
        else:
            key = str(item); label = key
        options.append(key); label_map[key] = label
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
        st.error("DXF export vyžaduje balík `ezdxf`. Pridaj `ezdxf>=1.1` do requirements.txt.")
        return

    gj, total, used = merge_geojson_pages(pages)
    feats = gj.get("features", []) if isinstance(gj, dict) else []
    doc = ezdxf.new(); msp = doc.modelspace()

    def as_xy(seq: Iterable) -> List[Tuple[float, float]]:
        out = []
        for p in seq:
            try:
                out.append((float(p[0]), float(p[1])))
            except Exception:
                continue
        return out

    for f in feats:
        g = (f or {}).get("geometry") or {}
        t = g.get("type"); c = g.get("coordinates")
        if t == "Polygon" and c:
            msp.add_lwpolyline(as_xy(c[0]), format="xy", close=True, dxfattribs={"layer": "PARCEL_POLY"})
        elif t == "MultiPolygon" and c:
            for poly in c:
                if poly:
                    msp.add_lwpolyline(as_xy(poly[0]), format="xy", close=True, dxfattribs={"layer": "PARCEL_POLY"})

    buf = BytesIO(); doc.write(buf); buf.seek(0)
    st.success(f"DXF pripravený – prvkov: {used}")
    st.download_button("Stiahnuť DXF", buf.getvalue(), file_name=f"parcely_{ku_code}.dxf", mime="image/vnd.dxf")


def _download_shp_zip_from_geojson(pages: list[bytes], ku_code: str) -> None:
    try:
        import shapefile  # pyshp
    except Exception:
        st.error("SHP export vyžaduje balík `pyshp` (package `shapefile`). Pridaj `pyshp>=2.3` do requirements.txt.")
        return

    gj, total, used = merge_geojson_pages(pages)
    feats = gj.get("features", []) if isinstance(gj, dict) else []

    def exterior_coords(geom) -> List[List[Tuple[float, float]]]:
        out: List[List[Tuple[float, float]]] = []
        t = (geom or {}).get("type"); c = (geom or {}).get("coordinates")
        def as_xy(seq: Iterable) -> List[Tuple[float, float]]:
            pts = []
            for p in seq:
                try:
                    pts.append((float(p[0]), float(p[1])))
                except Exception:
                    continue
            return pts
        if t == "Polygon" and c:
            out.append(as_xy(c[0]))  # exterior only
        elif t == "MultiPolygon" and c:
            for poly in c:
                if poly:
                    out.append(as_xy(poly[0]))
        return out

    with TemporaryDirectory() as td:
        shp_path = os.path.join(td, "parcely")
        w = shapefile.Writer(shp_path, shapeType=shapefile.POLYGON)
        w.autoBalance = 1
        w.field("fid", "N")
        cnt = 0
        for i, f in enumerate(feats):
            parts = exterior_coords((f or {}).get("geometry"))
            if not parts:
                continue
            w.poly(parts=parts)
            w.record(int(i))
            cnt += 1
        w.close()
        with open(shp_path + ".cpg", "w", encoding="ascii") as cpg:
            cpg.write("UTF-8")
        buf = BytesIO()
        from zipfile import ZipFile
        with ZipFile(buf, "w") as zf:
            for ext in (".shp", ".shx", ".dbf", ".cpg"):
                p = shp_path + ext
                if os.path.exists(p):
                    zf.write(p, arcname=os.path.basename(p))
        buf.seek(0)
    st.success(f"SHP ZIP pripravený – prvkov: {cnt}")
    st.download_button("Stiahnuť SHP (.zip)", buf.getvalue(), file_name=f"parcely_{ku_code}.zip", mime="application/zip")


def _download_gpkg_from_geojson(pages: list[bytes], ku_code: str) -> None:
    try:
        import fiona
    except Exception:
        st.error("GPKG export vyžaduje `fiona` (GDAL) alebo `geopandas`.")
        return

    gj, total, used = merge_geojson_pages(pages)
    feats = gj.get("features", []) if isinstance(gj, dict) else []

    with NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        schema = {"geometry": "Polygon", "properties": {"fid": "int"}}
        with fiona.open(tmp_path, mode="w", driver="GPKG", schema=schema, layer="parcely") as dst:
            for i, f in enumerate(feats):
                geom = (f or {}).get("geometry")
                if not isinstance(geom, dict):
                    continue
                if geom.get("type") not in ("Polygon", "MultiPolygon"):
                    continue
                dst.write({"geometry": geom, "properties": {"fid": int(i)}})
        data = open(tmp_path, "rb").read()
    finally:
        try: os.remove(tmp_path)
        except Exception: pass

    st.success(f"GPKG pripravený – prvkov: {used}")
    st.download_button("Stiahnuť GPKG", data, file_name=f"parcely_{ku_code}.gpkg", mime="application/geopackage+sqlite3")

# ---- WMS preview ------------------------------------------------------------

def _derive_wms_base(wfs_base: str) -> str:
    return re.sub(r"/wfs(\b|$)", "/wms", wfs_base, flags=re.IGNORECASE)


def _safe_fetch_zone_bbox(register: str, ku_code: str) -> Tuple[Optional[Tuple[float, float, float, float]], str]:
    """Try multiple `fetch_zone_bbox` signatures + srs strategies.
    Returns (bbox, crs) where crs is "EPSG:4326" if bbox is in 4326, otherwise "EPSG:5514".
    """
    # 1) Prefer bbox in EPSG:4326 (better for WMS 1.3.0)
    try:
        bbox = fetch_zone_bbox(register, ku_code, wfs_srs="EPSG:4326", retries=RETRIES)  # type: ignore[arg-type]
        if bbox:
            return bbox, "EPSG:4326"
    except TypeError:
        # Older signature without wfs_srs param
        try:
            bbox = fetch_zone_bbox(register, ku_code, retries=RETRIES)  # type: ignore[misc]
            if bbox:
                return bbox, "EPSG:5514"  # assume server default
        except TypeError:
            bbox = fetch_zone_bbox(register, ku_code)  # last resort
            if bbox:
                return bbox, "EPSG:5514"
    # 2) Fallback: call without args even if above failed
    try:
        bbox = fetch_zone_bbox(register, ku_code)
        if bbox:
            return bbox, "EPSG:5514"
    except Exception:
        pass
    return None, ""


def _show_wms_preview(register: str, ku_code: str) -> None:
    bbox, crs = _safe_fetch_zone_bbox(register, ku_code)
    if not bbox:
        st.warning("Náhľad WMS: BBOX sa nepodarilo získať.")
        return
    minx, miny, maxx, maxy = bbox

    base = CP_UO_WFS_BASE if (register or "").upper() == "E" else CP_WFS_BASE
    wms = _derive_wms_base(base)
    layer = ZONE_E if (register or "").upper() == "E" else ZONE_C

    if crs == "EPSG:4326":
        # WMS 1.3.0 + EPSG:4326 expects lat,lon order
        bbox_param = f"{miny},{minx},{maxy},{maxx}"
        crs_param = "EPSG:4326"
    else:
        # Assume projected native (S-JTSK). Axis order standard x,y
        bbox_param = f"{minx},{miny},{maxx},{maxy}"
        crs_param = "EPSG:5514"

    params = {
        "service": "WMS",
        "version": "1.3.0",
        "request": "GetMap",
        "layers": layer,
        "styles": "",
        "crs": crs_param,
        "bbox": bbox_param,
        "width": "900",
        "height": "650",
        "format": "image/png",
        "transparent": "false",
        "bgcolor": "0xFFFFFF",
    }
    from urllib.parse import urlencode
    url = f"{wms}?{urlencode(params)}"
    st.image(url, caption="WMS náhľad KU (zóna)", use_column_width=True)

# ----------------------------- main -----------------------------------------

def main() -> None:
    st.set_page_config(page_title="ParcelOne", layout="wide")
    st.title("ParcelOne – Sťahuj geometrie KN vo vybranom formáte")

    col1, col2 = st.columns([1, 1])
    with col1:
        register = st.selectbox("Register", ["E", "C"], index=0)
        ku_raw = st.text_input("Katastrálne územie – kód alebo názov",
                               placeholder="napr. 801062 alebo Banská Bystrica")
        parcels_csv = st.text_area("Parcelné čísla (voliteľné)", placeholder="napr. 1234/1, 456/2")
    with col2:
        options, label_map = _normalize_crs_choices(WFS_CRS_CHOICES)
        wfs_crs = st.selectbox("CRS (WFS srsName)", options=options, format_func=lambda k: label_map.get(k, k))
        do_preview_ku = st.checkbox("Zobraziť náhľad KU (WMS)", value=False)
        output_format = st.selectbox("Výstupový formát", ["gml-zip", "geojson", "shp-zip", "gpkg", "dxf"], index=0)

    ku_code = _resolve_ku(ku_raw)

    if do_preview_ku and ku_code:
        _show_wms_preview(register, ku_code)

    clicked = st.button("Stiahnuť parcely", type="primary")
    if not clicked:
        return

    if not ku_code:
        st.error("Zadaj kód KU alebo platný názov a prípadne vyber zo zoznamu.")
        return

    if output_format == "geojson":
        res = fetch_geojson_pages(register, ku_code, parcels_csv, wfs_srs=wfs_crs)
        if not res.ok:
            st.error(res.note)
            prev = preview_geojson_autofallback(register, ku_code, parcels_csv, wfs_srs=wfs_crs)
            if prev.ok:
                st.info("Náhľad úspešný s fallbackom – server pre GeoJSON môže byť náladový.")
                _download_geojson(prev.pages, ku_code)
            return
        _download_geojson(res.pages, ku_code)

    elif output_format == "gml-zip":
        res = fetch_gml_pages(register, ku_code, parcels_csv, wfs_srs=wfs_crs)
        if not res.ok:
            st.error(res.note); return
        _download_gml_zip(res.pages, ku_code)

    elif output_format == "shp-zip":
        res = fetch_geojson_pages(register, ku_code, parcels_csv, wfs_srs=wfs_crs)
        if not res.ok:
            st.error(res.note); return
        _download_shp_zip_from_geojson(res.pages, ku_code)

    elif output_format == "gpkg":
        res = fetch_geojson_pages(register, ku_code, parcels_csv, wfs_srs=wfs_crs)
        if not res.ok:
            st.error(res.note); return
        _download_gpkg_from_geojson(res.pages, ku_code)

    else:  # dxf
        res = fetch_geojson_pages(register, ku_code, parcels_csv, wfs_srs=wfs_crs)
        if not res.ok:
            st.error(res.note); return
        _download_dxf_from_geojson(res.pages, ku_code)


if __name__ == "__main__":
    main()

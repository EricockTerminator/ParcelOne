from __future__ import annotations
import io, zipfile
import streamlit as st
import folium
from streamlit_folium import st_folium
from typing import Optional, Tuple

from .wfs import (
    fetch_gml_pages, fetch_geojson_pages, merge_geojson_pages,
    bbox_from_geojson, WMS_URL_C, WMS_URL_E, LAYER_C, LAYER_E, ZONE_C, ZONE_E,
    fetch_zone_bbox,  # nový bbox helper pre zoom
)
from .convert import convert_pages_with_gdal
from .ku import load_ku_table, lookup_ku_code

WFS_CRS_CHOICES = {
    "auto (server default)": None,
    "EPSG:5514 (S-JTSK / Krovák EN)": "EPSG:5514",
    "EPSG:4258 (ETRS89)": "EPSG:4258",
    "EPSG:4326 (WGS84)": "EPSG:4326",
}

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

def show_map_preview(reg: str, fc_geojson: Optional[dict], bbox: Optional[Tuple[float,float,float,float]], *, ku: str = "", parcels: str = ""):
    # Prečo: keď nie je bbox, aspoň SR centrum
    default_center = (48.7, 19.7); default_zoom = 8
    if bbox:
        minx, miny, maxx, maxy = bbox
        cx, cy = (minx + maxx) / 2, (miny + maxy) / 2
        center = (cy, cx); zoom = 14
    else:
        center = default_center; zoom = default_zoom

    m = folium.Map(location=list(center), zoom_start=zoom, tiles=None, control_scale=True)
    folium.TileLayer("OpenStreetMap", control=False).add_to(m)

    is_E = (reg or '').upper() == 'E'
    url   = WMS_URL_E if is_E else WMS_URL_C
    layer = LAYER_E    if is_E else LAYER_C
    zone  = ZONE_E     if is_E else ZONE_C

    cql_parc = _build_cql_for_preview(ku, parcels or "")
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

    if (parcels or '').strip() and fc_geojson:
        folium.GeoJson(fc_geojson, name="Vybrané parcely (WFS)", style_function=lambda _: {"weight": 3, "fill": False}).add_to(m)

    st_folium(m, height=540, returned_objects=[])

def main():
    # MUSÍ byť prvé volanie Streamlitu
    st.set_page_config(page_title="ParcelOne – WFS GML", layout="wide")
    st.title("ParcelOne – Sťahuj geometrie KN vo vybranom formáte")

    # Sidebar vo vnútri main()
    with st.sidebar:
        reg = st.selectbox("Register", ["E", "C"], index=0)
        col_ku1, col_ku2 = st.columns(2)
        with col_ku1:
            ku_code = st.text_input("Katastrálne územie – kód", placeholder="napr. 808156")
        with col_ku2:
            ku_name = st.text_input("...alebo názov", placeholder="napr. Bratislava-Staré Mesto")
        parcels = st.text_area("Parcelné čísla (voliteľné)", placeholder="napr. 1234/1, 1234/2")
        fmt = st.selectbox("Výstupový formát", ["gml-zip", "geojson", "shp", "dxf", "gpkg"], index=0)
        crs_label = st.selectbox("CRS (WFS srsName)", list(WFS_CRS_CHOICES.keys()), index=0)
        wfs_srs = WFS_CRS_CHOICES[crs_label]
        st.caption("**Kontakt**  •  📞 +421 948 955 128  •  ✉️ svitokerik02@gmail.com")

    col1, col2 = st.columns([2, 1])

    # KU lookup
    ku_table = load_ku_table()
    resolved_ku = (ku_code or "").strip()
    ku_suggestions: list[dict] = []
    if not resolved_ku:
        resolved_ku, ku_suggestions = lookup_ku_code(ku_table, ku_name or "")
    soft_pick = ku_suggestions[0] if (not resolved_ku and ku_suggestions) else None

    if ku_name and not resolved_ku:
        st.info("Nenašiel som presnú zhodu.")
    if ku_suggestions:
        cols = st.columns(min(5, len(ku_suggestions)))
        for i, it in enumerate(ku_suggestions[:5]):
            label = f"{it['name']} ({it['code']})"
            if cols[i].button(label, key=f"pick_ku_{it['code']}"):
                resolved_ku = it['code']; ku_name = it['name']; soft_pick = it
    if ku_name and resolved_ku:
        st.caption(f"Vybrané KU: {ku_name} → kód **{resolved_ku}**")

    # Náhľad (Zoning bbox → rýchly zoom)
    __ku_for_preview = resolved_ku or (soft_pick['code'] if soft_pick else "")
    with col1:
        with st.spinner("Pripravujem mapový náhľad…"):
            zone_bbox = fetch_zone_bbox(reg, __ku_for_preview) if __ku_for_preview else None
            if (parcels or '').strip():
                gj = fetch_geojson_pages(reg, __ku_for_preview, parcels, wfs_srs="EPSG:4326")
                if gj.ok and gj.pages:
                    fc, total, used = merge_geojson_pages(gj.pages, max_features=4000)
                    bb = bbox_from_geojson(fc) or zone_bbox
                    show_map_preview(reg, fc, bb, ku=__ku_for_preview, parcels=parcels)
                    if used < total:
                        st.caption(f"Náhľad skrátený: {used} z {total} prvkov.")
                else:
                    show_map_preview(reg, None, zone_bbox, ku=__ku_for_preview, parcels=parcels)
                    st.caption("WMS náhľad – WFS pre parcely nevrátil dáta.")
            else:
                show_map_preview(reg, None, zone_bbox, ku=__ku_for_preview, parcels="")

    # Download (ťažká časť až tu)
    if not (resolved_ku or (parcels or '').strip()):
        st.error("Zadaj KU (kód alebo názov) alebo aspoň jedno parcelné číslo.")
        return

    with st.spinner("Naťahujem GML stránky z WFS…"):
        result = fetch_gml_pages(reg, resolved_ku or "", parcels, wfs_srs=wfs_srs)

    with col1:
        if not result.ok or not result.pages:
            st.error(f"WFS nevrátil dáta: {result.note}\nURL: {result.first_url or '-'}")
            return
        st.success(f"Parcely pripravené. Stránok: {len(result.pages)}")

        mem_zip = io.BytesIO()
        with zipfile.ZipFile(mem_zip, mode="w") as zf:
            for i, b in enumerate(result.pages, 1):
                zf.writestr(f"parcely_{i:03d}.gml", b)
        gml_zip = mem_zip.getvalue()

        if fmt == "gml-zip":
            st.download_button(
                "Stiahnuť GML (ZIP)", data=gml_zip,
                file_name=f"parcely_{reg}_{resolved_ku or 'filter'}.zip", mime="application/zip",
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
                    data, mime, conv_src = convert_pages_with_gdal(result.pages, "DXF", ".dxf")
                    st.download_button("Stiahnuť DXF", data=data,
                                       file_name=f"parcely_{reg}_{resolved_ku or 'filter'}.dxf", mime=mime)
                    st.caption(f"Konverzia: {conv_src}")
                elif fmt == "gpkg":
                    data, mime, conv_src = convert_pages_with_gdal(result.pages, "GPKG", ".gpkg")
                    st.download_button("Stiahnuť GPKG", data=data,
                                       file_name=f"parcely_{reg}_{resolved_ku or 'filter'}.gpkg", mime=mime)
                    st.caption(f"Konverzia: {conv_src}")
            except Exception as e:
                st.error(f"Konverzia zlyhala: {e}")

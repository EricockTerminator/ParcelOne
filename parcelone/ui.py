# file: parcelone/parcelone/ui.py
"""Streamlit UI (no forms). Simpler flow, working submit + KU lookup."""
import json
from io import BytesIO
from zipfile import ZipFile

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


def main() -> None:
    st.set_page_config(page_title="ParcelOne", layout="wide")
    st.title("ParcelOne – Sťahuj geometrie KN vo vybranom formáte")

    col1, col2 = st.columns([1, 1])
    with col1:
        register = st.selectbox("Register", ["E", "C"], index=0)
        ku_raw = st.text_input("Katastrálne územie – kód alebo názov", placeholder="napr. 801062 alebo Banská Bystrica")
        parcels_csv = st.text_area("Parcelné čísla (voliteľné)", placeholder="napr. 1234/1, 456/2")
    with col2:
        output_format = st.selectbox("Výstupový formát", ["gml-zip", "geojson"], index=0)
        wfs_crs = st.selectbox(
            "CRS (WFS srsName)",
            options=[c[0] for c in WFS_CRS_CHOICES],
            format_func=lambda k: next((label for key, label in WFS_CRS_CHOICES if key == k), k),
        )
        do_preview_ku = st.checkbox("Zobraziť náhľad KU (bbox)", value=False)

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
    else:
        res = fetch_gml_pages(register, ku_code, parcels_csv, wfs_srs=wfs_crs, retries=RETRIES)
        if not res.ok:
            st.error(res.note)
            return
        _download_gml_zip(res.pages, ku_code)


if __name__ == "__main__":
    main()

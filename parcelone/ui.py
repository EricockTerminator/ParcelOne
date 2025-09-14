# file: parcelone/parcelone/ui.py
"""Streamlit UI for ParcelOne (refactor: no top-level execution).

- All logic runs inside `main()`; nothing executes at import-time.
- KU input accepts 6-digit code or name (uses `ku_index.code_for`).
- Disambiguation when name maps to multiple KUs.
- Supports preview (BBox) and download in `geojson` or `gml-zip`.
"""
from __future__ import annotations

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

# UI defaults
RETRIES = 3


# --------------------------- helpers ----------------------------------------

def _ku_code_input(label: str = "Katastrálne územie – kód alebo názov") -> str:
    """Return a resolved 6-digit KU code from user input (code or name).
    Stops the app with an error message on invalid input.
    """
    ku_input = st.text_input(
        label,
        value="",
        placeholder="napr. 801062 alebo Banská Bystrica",
    ).strip()

    if not ku_input:
        st.stop()  # wait for user to type

    ku_code, candidates = code_for(ku_input)

    if candidates:
        options = {f"{c.name} ({c.code})": c.code for c in candidates}
        choice = st.selectbox("Našli sme viac KU – vyber jedno:", list(options.keys()))
        ku_code = options[choice]

    if not ku_code:
        st.error("Neplatný kód alebo názov KU. Skús znova.")
        st.stop()

    return ku_code


def _download_geojson(pages: list[bytes], ku_code: str) -> None:
    gj, total, used = merge_geojson_pages(pages)
    payload = json.dumps(gj, ensure_ascii=False).encode("utf-8")
    st.success(f"Pripravený GeoJSON – vybrané prvky: {used} / spolu: {total}")
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
    st.success(f"Pripravený GML ZIP – počet stránok: {len(pages)}")
    st.download_button(
        "Stiahnuť GML (.zip)",
        buf.read(),
        file_name=f"parcely_{ku_code}.zip",
        mime="application/zip",
    )


# ----------------------------- main -----------------------------------------

def main() -> None:
    st.set_page_config(page_title="ParcelOne", layout="wide")
    st.title("ParcelOne – Sťahuj geometrie KN vo vybranom formáte")

    with st.form("parcel_form"):
        register = st.selectbox("Register", ["E", "C"], index=0)
        ku_code = _ku_code_input()
        parcels_csv = st.text_area("Parcelné čísla (voliteľné)", placeholder="napr. 1234/1, 456/2")
        output_format = st.selectbox("Výstupový formát", ["gml-zip", "geojson"], index=0)
        wfs_crs = st.selectbox(
            "CRS (WFS srsName)",
            options=[c[0] for c in WFS_CRS_CHOICES],
            format_func=lambda k: next((label for key, label in WFS_CRS_CHOICES if key == k), k),
        )
        do_preview_ku = st.checkbox("Zobraziť náhľad KU (bbox)", value=False)
        submitted = st.form_submit_button("Stiahnuť parcely")

    if do_preview_ku:
        bbox = fetch_zone_bbox(register, ku_code, retries=RETRIES)
        if bbox:
            st.caption("BBOX (minx, miny, maxx, maxy)")
            st.code(repr(bbox))
        else:
            st.warning("Nepodarilo sa načítať BBOX pre zadané KU.")

    if not submitted:
        return

    # Download flow
    if output_format == "geojson":
        res = fetch_geojson_pages(register, ku_code, parcels_csv, wfs_srs=wfs_crs, retries=RETRIES)
        if not res.ok:
            st.error(res.note)
            # try a fallback preview without srsName to help the user
            prev = preview_geojson_autofallback(register, ku_code, parcels_csv, wfs_srs=wfs_crs, retries=RETRIES)
            if prev.ok:
                st.info("Náhľad úspešný s fallbackom – sťahovanie nižšie môže zlyhať podľa servera.")
                _download_geojson(prev.pages, ku_code)
            return
        _download_geojson(res.pages, ku_code)
    else:  # gml-zip
        res = fetch_gml_pages(register, ku_code, parcels_csv, wfs_srs=wfs_crs, retries=RETRIES)
        if not res.ok:
            st.error(res.note)
            return
        _download_gml_zip(res.pages, ku_code)


if __name__ == "__main__":  # manual run
    main()


# --------------------------- wfs.py note -------------------------------------
# Ensure your `wfs.py` defines parcel type names used by GetFeature:
# Add near the top (next to ZONE_C/ZONE_E) if missing:
# TYPE_C = "cp:CP.CadastralParcel"
# TYPE_E = "cp_uo:CP.CadastralParcelUO"

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urlencode

import requests
import streamlit as st

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
PREVIEW_MAX_FEATURES = 500  # koľko GeoJSON prvkov max vykreslíme
PREVIEW_PAGE_SIZE = 500      # koľko prvkov žiadame na stránku
DEBUG_PROFILE = False        # zapni pre zobrazenie časov

# --- Jednoduché profilovanie ---
_step_times: dict[str, float] = {}

class timed:
    def __init__(self, name: str):
        self.name = name
        self.t0 = 0.0

    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *exc):
        _step_times[self.name] = _step_times.get(self.name, 0.0) + (time.perf_counter() - self.t0)

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
    page_size: int = PREVIEW_PAGE_SIZE,
) -> FetchResult:
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
                "count": str(page_size),
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
) -> FetchResult:
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
                for cc in c:
                    _rec(cc)
    _rec(coords)


def bbox_from_geojson(obj: dict) -> Optional[Tuple[float, float, float, float]]:
    if not obj:
        return None
    agg = [float("inf"), float("inf"), float("-inf"), float("-inf")]
    if obj.get("type") == "FeatureCollection":
        for f in obj.get("features", []):
            _walk_coords((f or {}).get("geometry") or {}, agg)
    elif obj.get("type") == "Feature":
        _walk_coords((obj or {}).get("geometry") or {}, agg)
    else:
        _walk_coords(obj, agg)
    if agg[0] == float("inf"):
        return None
    return tuple(agg)


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

    base = CP_UO_WFS_BASE if reg == "E" else CP_WFS_BASE
    layer = "cp_uo:CP.CadastralZoningUO" if reg == "E" else "cp:CP.CadastralZoning"

    try:
        params = {
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": layer, "count": "1", "startIndex": "0",
            "outputFormat": "application/json",
            "CQL_FILTER": f"nationalCadastralReference='{k}'",
        }
        url = f"{base}?{urlencode(params)}"
        jb = http_get_bytes(url)
        feats = json.loads(jb.decode("utf-8", "ignore")).get("features", [])
        if feats:
            return bbox_from_geojson({"type": "FeatureCollection", "features": feats})
    except Exception:
        pass

    try:
        fes = ('<Filter xmlns="http://www.opengis.net/fes/2.0">'
               '<PropertyIsEqualTo><ValueReference>nationalCadastralReference</ValueReference>'
               f'<Literal>{xml_escape(k)}</Literal></PropertyIsEqualTo></Filter>')
        params = {
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": layer, "count": "1", "startIndex": "0",
            "outputFormat": "application/json", "filter": fes,
        }
        url = f"{base}?{urlencode(params)}"
        jb = http_get_bytes(url)
        feats = json.loads(jb.decode("utf-8", "ignore")).get("features", [])
        if feats:
            return bbox_from_geojson({"type": "FeatureCollection", "features": feats})
    except Exception:
        pass

    try:
        res = fetch_geojson_pages(reg, k, "", page_size=1)
        if res.ok and res.pages:
            obj = json.loads(res.pages[0].decode("utf-8", "ignore"))
            return bbox_from_geojson(obj)
    except Exception:
        pass

    return None

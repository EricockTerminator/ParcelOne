# file: parcelone/parcelone/wfs.py
"""Fast, sync WFS helpers using `requests` (no asyncio), with robust fallbacks.

- FES filter by KU + optional parcel labels
- PAGE_SIZE=1000 default, loop until empty
- Fallbacks: drop `srsName` on HTTP 400; CQL fallback; split-by-one for labels
- Separate GeoJSON and GML paths
- Lightweight bbox fetch for KU via zoning layer (GeoJSON), supports `wfs_srs`
"""
from dataclasses import dataclass
from typing import List, Optional, Tuple
import json
import re

import requests

try:  # Allow running as a standalone script or part of the package
    from .config import CP_WFS_BASE, CP_UO_WFS_BASE, PAGE_SIZE, WFS_CRS_CHOICES
except ImportError:  # pragma: no cover
    from config import CP_WFS_BASE, CP_UO_WFS_BASE, PAGE_SIZE, WFS_CRS_CHOICES  # type: ignore

# ---- layers / types ---------------------------------------------------------
TYPE_C = "cp:CP.CadastralParcel"
TYPE_E = "cp_uo:CP.CadastralParcelUO"
ZONE_C = "cp:CP.CadastralZoning"
ZONE_E = "cp_uo:CP.CadastralZoningUO"

# ---- HTTP defaults ----------------------------------------------------------
HEADERS_XML = {
    "User-Agent": "ParcelOne/WFS 1.0",
    "Accept": "application/xml,*/*;q=0.5",
    "Connection": "close",
}
TIMEOUT = (10, 60)  # (connect, read)

# ---- common helpers ---------------------------------------------------------
def http_get_bytes(url: str, tries: int = 3) -> bytes:
    last: Exception | None = None
    for i in range(tries):
        try:
            r = requests.get(url, headers=HEADERS_XML, timeout=TIMEOUT)
            r.raise_for_status()
            return r.content
        except Exception as e:
            last = e
            import time as _t
            _t.sleep(0.6 * (i + 1))
    assert last is not None
    raise last


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
                f"<PropertyIsEqualTo><ValueReference>label</ValueReference>"
                f"<Literal>{p_xml}</Literal></PropertyIsEqualTo>",
            ]
            if ku_part:
                and_parts.append(ku_part)
            inner_ors.append("<And>" + "".join(and_parts) + "</And>")
        return f'<Filter xmlns="http://www.opengis.net/fes/2.0"><Or>{"".join(inner_ors)}</Or></Filter>'
    if ku_part:
        return f'<Filter xmlns="http://www.opengis.net/fes/2.0">{ku_part}</Filter>'
    return ""


def build_cql_filter(ku: str, parcels: List[str]) -> str:
    parts: List[str] = []
    if parcels:
        quoted = ",".join(["'" + p.replace("'", "''") + "'" for p in parcels if p])
        if quoted:
            parts.append(f"label IN ({quoted})")
    if ku:
        parts.append(f"nationalCadastralReference LIKE '{ku}%' ")
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

# ---- GML (fast path) --------------------------------------------------------
def gml_number_returned(xmlb: bytes) -> Optional[int]:
    m = re.search(rb'numberReturned="(\d+)"', xmlb)
    return int(m.group(1)) if m else None


def fetch_gml_pages(
    register: str,
    ku: str,
    parcels_csv: str,
    wfs_srs: Optional[str] = None,
    *,
    page_size: int = PAGE_SIZE,
) -> FetchResult:
    reg = (register or "").upper().strip()
    ku = (ku or "").strip()
    if not ku and not (parcels_csv or "").strip():
        return FetchResult(False, "Zadaj aspoň KU alebo parcelné čísla.", [], "")

    from urllib.parse import urlencode

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
            "count": str(page_size),
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
                # split-by-one fallback for OR filters
                singles: List[bytes] = []
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
                            singles.append(sb)
                    except Exception:
                        pass
                if singles:
                    return FetchResult(True, f"Počet stránok: {len(singles)} (split-by-one)", singles, first_url)
            # final CQL fallback
            cql = build_cql_filter(ku, parcels)
            if cql:
                cql_params = {
                    "service": "WFS", "version": "2.0.0", "request": "GetFeature",
                    "typeNames": typename, "count": str(page_size), "startIndex": str(start),
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
            if nr < page_size:
                break
            start += nr
        else:
            if len(xmlb) < 10000:
                break
            start += page_size

        if start > 500_000:
            break

    if not pages:
        return FetchResult(False, "Server vrátil 0 prvkov pre daný filter.", [], first_url)

    return FetchResult(True, f"Počet stránok: {len(pages)}", pages, first_url)

# ---- GeoJSON ---------------------------------------------------------------
def fetch_geojson_pages(
    register: str,
    ku: str,
    parcels_csv: str,
    wfs_srs: Optional[str] = None,
    *,
    page_size: int = PAGE_SIZE,
) -> FetchResult:
    reg = (register or "").upper().strip()
    ku = (ku or "").strip()
    parcels = [p.strip() for p in re.split(r"[,;\s]+", parcels_csv or "") if p.strip()]
    base = CP_UO_WFS_BASE if reg == "E" else CP_WFS_BASE
    typename = TYPE_E if reg == "E" else TYPE_C

    from urllib.parse import urlencode

    filt_xml = build_fes_filter(ku, parcels)
    if not filt_xml:
        return FetchResult(False, "Neplatný filter (chýba KU aj parcely)", [], "")

    pages: List[bytes] = []
    start = 0
    first_url = ""

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

        if len(feats) < page_size:
            break
        start += page_size
        if start > 500_000:
            break

    if not pages:
        return FetchResult(False, "Server vrátil 0 prvkov pre daný filter.", [], first_url)

    return FetchResult(True, f"Počet stránok: {len(pages)}", pages, first_url)

# ---- GeoJSON helpers -------------------------------------------------------
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
                agg[0] = min(agg[0], x)
                agg[1] = min(agg[1], y)
                agg[2] = max(agg[2], x)
                agg[3] = max(agg[3], y)
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
    return tuple(agg)  # minx, miny, maxx, maxy

# ---- KU bbox ---------------------------------------------------------------
def _zoning_layer(register: str) -> str:
    return ZONE_E if (register or "").upper() == "E" else ZONE_C


def fetch_zone_bbox(
    register: str,
    ku: str,
    *,
    wfs_srs: Optional[str] = "EPSG:4326",
    retries: int = 3,
) -> Optional[Tuple[float, float, float, float]]:
    """GeoJSON request to zoning layer filtered by KU -> bbox.
    If `wfs_srs` is provided, the WFS call asks the server to reproject.
    """
    base = CP_UO_WFS_BASE if (register or "").upper() == "E" else CP_WFS_BASE
    layer = _zoning_layer(register)
    from urllib.parse import urlencode

    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": layer,
        "outputFormat": "application/json",
        "count": "1",
        "CQL_FILTER": f"nationalCadastralReference='{ku}'",
    }
    if wfs_srs:
        params["srsName"] = wfs_srs

    url = f"{base}?{urlencode(params)}"
    try:
        jb = http_get_bytes(url, tries=retries)
        obj = json.loads(jb.decode("utf-8", "ignore"))
    except Exception:
        return None
    return bbox_from_geojson(obj)

# ---- convenience -----------------------------------------------------------
def preview_geojson_autofallback(
    register: str,
    ku: str,
    parcels_csv: str,
    wfs_srs: Optional[str] = None,
    *,
    page_size: int = PAGE_SIZE,
) -> FetchResult:
    res = fetch_geojson_pages(register, ku, parcels_csv, wfs_srs=wfs_srs, page_size=page_size)
    if res.ok or not wfs_srs:
        return res
    return fetch_geojson_pages(register, ku, parcels_csv, wfs_srs=None, page_size=page_size)


__all__ = [
    "TYPE_C",
    "TYPE_E",
    "ZONE_C",
    "ZONE_E",
    "HEADERS_XML",
    "TIMEOUT",
    "FetchResult",
    "build_fes_filter",
    "merge_geojson_pages",
    "fetch_gml_pages",
    "fetch_geojson_pages",
    "preview_geojson_autofallback",
    "fetch_zone_bbox",
    "bbox_from_geojson",
]

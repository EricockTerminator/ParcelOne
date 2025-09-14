import asyncio
import aiohttp
import json
import re
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urlencode

from .config import WFS_CRS_CHOICES


try:  # Allow running as a standalone script or as part of the package
    from .config import CP_WFS_BASE, CP_UO_WFS_BASE, PAGE_SIZE
except ImportError:  # pragma: no cover
    from config import CP_WFS_BASE, CP_UO_WFS_BASE, PAGE_SIZE  # type: ignore
ZONE_C = "cp:CP.CadastralZoning"
ZONE_E = "cp_uo:CP.CadastralZoningUO"
PREVIEW_PAGE_SIZE = 100
PREVIEW_MAX_FEATURES = 500
DEBUG_PROFILE = False
_step_times: dict[str, float] = {}
DEFAULT_TIMEOUT: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
    total=60, # total request budget (s)
    sock_connect=15,
    sock_read=30,
)

HEADERS_XML = {
    # Needed by WFS GetFeature endpoints that prefer XML
    "Accept": "application/xml, text/xml;q=0.9,*/*;q=0.8",
    # A UA helps some gateways; keep it benign
    "User-Agent": "ParcelOne/0.1 (+https://parcelone.streamlit.app)",
}

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


def has_any_feature(xml_bytes: bytes) -> bool:
    b = xml_bytes
    return (
        (b.find(b"featureMember") != -1)
        or (b.find(b":member") != -1)
        or (b.find(b"<wfs:member") != -1)
    )


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


def gml_number_matched(xmlb: bytes) -> Optional[int]:
    m = re.search(rb'numberMatched="(\d+)"', xmlb)
    return int(m.group(1)) if m else None

def has_any_geojson_feature(json_bytes: bytes) -> bool:
    try:
        data = json.loads(json_bytes.decode("utf-8", "ignore"))
    except Exception:
        return False
    if isinstance(data, dict):
        t = data.get("type")
        if t == "FeatureCollection":
            return bool(data.get("features"))
        if t == "Feature":
            return True
    return False


def json_number_returned(jsonb: bytes) -> Optional[int]:
    try:
        data = json.loads(jsonb.decode("utf-8", "ignore"))
    except Exception:
        return None
    if isinstance(data, dict):
        if "numberReturned" in data:
            try:
                return int(data["numberReturned"])
            except Exception:
                return None
        if data.get("type") == "FeatureCollection":
            feats = data.get("features") or []
            if isinstance(feats, list):
                return len(feats)
        if data.get("type") == "Feature":
            return 1
    return None


def json_number_matched(jsonb: bytes) -> Optional[int]:
    try:
        data = json.loads(jsonb.decode("utf-8", "ignore"))
    except Exception:
        return None
    if isinstance(data, dict) and "numberMatched" in data:
        try:
            return int(data["numberMatched"])
        except Exception:
            return None
    return None


def _bbox_from_coords(coords: List) -> List[Tuple[float, float]]:
    points: List[Tuple[float, float]] = []
    if not isinstance(coords, list):
        return points
    if coords and isinstance(coords[0], (int, float)):
        if len(coords) >= 2:
            points.append((coords[0], coords[1]))
        return points
    for item in coords:
        points.extend(_bbox_from_coords(item))
    return points


def bbox_from_geojson(gj: dict) -> Optional[Tuple[float, float, float, float]]:
    if not isinstance(gj, dict):
        return None
    if "bbox" in gj and isinstance(gj["bbox"], (list, tuple)) and len(gj["bbox"]) >= 4:
        b = gj["bbox"]
        return (float(b[0]), float(b[1]), float(b[2]), float(b[3]))
    points: List[Tuple[float, float]] = []
    t = gj.get("type")
    if t == "FeatureCollection":
        for feat in gj.get("features", []) or []:
            geom = feat.get("geometry") if isinstance(feat, dict) else None
            if geom:
                points.extend(_bbox_from_coords(geom.get("coordinates")))
    elif t == "Feature":
        geom = gj.get("geometry")
        if geom:
            points.extend(_bbox_from_coords(geom.get("coordinates")))
    else:
        # geometry object
        points.extend(_bbox_from_coords(gj.get("coordinates")))
    if not points:
        return None
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return (min(xs), min(ys), max(xs), max(ys))


def merge_geojson_pages(pages: List[bytes], max_features: Optional[int] = None) -> Tuple[dict, int, int]:
    features = []
    total = 0
    for page in pages:
        try:
            data = json.loads(page.decode("utf-8", "ignore"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        t = data.get("type")
        if t == "FeatureCollection":
            feats = data.get("features") or []
            if isinstance(feats, list):
                total += len(feats)
                for f in feats:
                    if max_features is None or len(features) < max_features:
                        features.append(f)
        elif t == "Feature":
            total += 1
            if max_features is None or len(features) < max_features:
                features.append(data)
    used = len(features)
    return {"type": "FeatureCollection", "features": features}, total, used



async def _fetch(session: aiohttp.ClientSession, url: str, *, retries: int = 3,
                timeout: aiohttp.ClientTimeout | None = None) -> bytes:
    timeout = timeout or DEFAULT_TIMEOUT

    Why: default arguments are evaluated at definition time. If a module-level
    constant is declared below, `DEFAULT_TIMEOUT` is not yet defined, causing an
    import-time failure. Using `None` here and resolving inside avoids that.

    Raises the final exception if all retries fail.
    if retries < 1:
        raise ValueError("retries must be >= 1")

    effective_timeout = timeout or DEFAULT_TIMEOUT

    backoff = 0.5
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, timeout=effective_timeout) as resp:
                resp.raise_for_status()
                return await resp.read()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt >= retries:
                raise
            await asyncio.sleep(backoff)
            backoff *= 2


__all__ = [
    "DEFAULT_TIMEOUT",
    "_fetch",
]



async def fetch_gml_pages_async(
    register: str,
    ku: str,
    parcels_csv: str,
    wfs_srs: Optional[str] = None,
    *,
    page_size: int = PAGE_SIZE,
    retries: int = 3,
    timeout: aiohttp.ClientTimeout = DEFAULT_TIMEOUT,
) -> FetchResult:
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

    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": typename,
        "count": str(page_size),
        "startIndex": "0",
        "filter": fes,
    }
    if wfs_srs:
        params["srsName"] = wfs_srs
    first_url = f"{base}?{urlencode(params)}"

    async with aiohttp.ClientSession(headers=HEADERS_XML) as session:
        try:
            first_page = await _fetch(session, first_url, retries=retries, timeout=timeout)
        except Exception as e:
            return FetchResult(False, f"HTTP chyba: {e}", [], first_url)

        if not has_any_feature(first_page):
            return FetchResult(False, "Server vrátil 0 prvkov pre daný filter.", [], first_url)

        pages = [first_page]
        total = gml_number_matched(first_page) or gml_number_returned(first_page) or 0
        if total <= page_size or total == 0:
            return FetchResult(True, "Počet stránok: 1", pages, first_url)

        start_indices = list(range(page_size, total, page_size))
        urls = []
        for start in start_indices:
            p = params.copy()
            p["startIndex"] = str(start)
            urls.append(f"{base}?{urlencode(p)}")

        tasks = [
            _fetch(session, url, retries=retries, timeout=timeout)
            for url in urls
        ]
        try:
            results = await asyncio.gather(*tasks)
        except Exception as e:
            return FetchResult(False, f"HTTP chyba: {e}", pages, first_url)
        pages.extend(results)
        return FetchResult(True, f"Počet stránok: {len(pages)}", pages, first_url)


def fetch_gml_pages(
    register: str,
    ku: str,
    parcels_csv: str,
    wfs_srs: Optional[str] = None,
    **kwargs,
) -> FetchResult:
    """Synchronous wrapper for Streamlit."""
    return asyncio.run(
        fetch_gml_pages_async(register, ku, parcels_csv, wfs_srs=wfs_srs, **kwargs)
    )

# ---------------------- GeoJSON helpers ---------------------------------

async def fetch_geojson_pages_async(
    register: str,
    ku: str,
    parcels_csv: str,
    wfs_srs: Optional[str] = None,
    *,
    page_size: int = PAGE_SIZE,
    retries: int = 3,
    timeout: aiohttp.ClientTimeout = DEFAULT_TIMEOUT,
) -> FetchResult:
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

    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": typename,
        "count": str(page_size),
        "startIndex": "0",
        "filter": fes,
        "outputFormat": "application/json",
    }
    if wfs_srs:
        params["srsName"] = wfs_srs
    first_url = f"{base}?{urlencode(params)}"

    async with aiohttp.ClientSession(headers=HEADERS_XML) as session:
        try:
            first_page = await _fetch(session, first_url, retries=retries, timeout=timeout)
        except Exception as e:
            return FetchResult(False, f"HTTP chyba: {e}", [], first_url)

        if not has_any_geojson_feature(first_page):
            return FetchResult(False, "Server vrátil 0 prvkov pre daný filter.", [], first_url)

        pages = [first_page]
        total = json_number_matched(first_page) or json_number_returned(first_page) or 0
        if total <= page_size or total == 0:
            return FetchResult(True, "Počet stránok: 1", pages, first_url)

        start_indices = list(range(page_size, total, page_size))
        urls = []
        for start in start_indices:
            p = params.copy()
            p["startIndex"] = str(start)
            urls.append(f"{base}?{urlencode(p)}")

        tasks = [
            _fetch(session, url, retries=retries, timeout=timeout)
            for url in urls
        ]
        try:
            results = await asyncio.gather(*tasks)
        except Exception as e:
            return FetchResult(False, f"HTTP chyba: {e}", pages, first_url)
        pages.extend(results)
        return FetchResult(True, f"Počet stránok: {len(pages)}", pages, first_url)


def fetch_geojson_pages(
    register: str,
    ku: str,
    parcels_csv: str,
    wfs_srs: Optional[str] = None,
    **kwargs,
) -> FetchResult:
    return asyncio.run(
        fetch_geojson_pages_async(register, ku, parcels_csv, wfs_srs=wfs_srs, **kwargs)
    )


def preview_geojson_autofallback(
    register: str,
    ku: str,
    parcels_csv: str,
    wfs_srs: Optional[str] = None,
    **kwargs,
) -> FetchResult:
    res = fetch_geojson_pages(register, ku, parcels_csv, wfs_srs=wfs_srs, **kwargs)
    if res.ok or not wfs_srs:
        return res
    return fetch_geojson_pages(register, ku, parcels_csv, wfs_srs=None, **kwargs)


async def _fetch_zone_bbox_async(
    register: str,
    ku: str,
    *,
    retries: int = 3,
    timeout: aiohttp.ClientTimeout = DEFAULT_TIMEOUT,
) -> Optional[Tuple[float, float, float, float]]:
    reg = (register or "").upper().strip()
    ku = (ku or "").strip()
    if not ku:
        return None
    base = CP_UO_WFS_BASE if reg == "E" else CP_WFS_BASE
    layer = ZONE_E if reg == "E" else ZONE_C
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": layer,
        "outputFormat": "application/json",
        "count": "1",
        "cql_filter": f"nationalCadastralReference='{ku}'",
    }
    url = f"{base}?{urlencode(params)}"
    async with aiohttp.ClientSession(headers=HEADERS_XML) as session:
        try:
            data = await _fetch(session, url, retries=retries, timeout=timeout)
        except Exception:
            return None
    try:
        gj = json.loads(data.decode("utf-8", "ignore"))
    except Exception:
        return None
    return bbox_from_geojson(gj)


def fetch_zone_bbox(
    register: str,
    ku: str,
    *,
    retries: int = 3,
    timeout: aiohttp.ClientTimeout = DEFAULT_TIMEOUT,
) -> Optional[Tuple[float, float, float, float]]:
    """Fetch bounding box for cadastral zone."""
    return asyncio.run(
        _fetch_zone_bbox_async(register, ku, retries=retries, timeout=timeout)
    )

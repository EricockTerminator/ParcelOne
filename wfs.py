import asyncio
import aiohttp
import re
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urlencode

CP_WFS_BASE = "https://inspirews.skgeodesy.sk/geoserver/cp/ows"
CP_UO_WFS_BASE = "https://inspirews.skgeodesy.sk/geoserver/cp_uo/ows"
TYPE_C = "cp:CP.CadastralParcel"
TYPE_E = "cp_uo:CP.CadastralParcelUO"
HEADERS_XML = {
    "User-Agent": "ParcelOne/WFS-GML 1.0",
    "Accept": "application/xml,*/*;q=0.5",
    "Connection": "close",
}
PAGE_SIZE = 1000
DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=90)


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


async def _fetch(session: aiohttp.ClientSession, url: str, *, retries: int = 3, timeout: aiohttp.ClientTimeout = DEFAULT_TIMEOUT) -> bytes:
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=timeout) as resp:
                resp.raise_for_status()
                return await resp.read()
        except Exception:
            if attempt + 1 == retries:
                raise
            await asyncio.sleep(0.5)
    raise RuntimeError("unreachable")


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

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Tuple
import json, os, re
import requests

# --- Endpoints & constants ---
CP_WFS_BASE = "https://inspirews.skgeodesy.sk/geoserver/cp/ows"        # C register
CP_UO_WFS_BASE = "https://inspirews.skgeodesy.sk/geoserver/cp_uo/ows"   # E register
TYPE_C = "cp:CP.CadastralParcel"
TYPE_E = "cp_uo:CP.CadastralParcelUO"
HEADERS_XML = {"User-Agent": "ParcelOne/WFS-GML 1.0", "Accept": "application/xml,*/*;q=0.5", "Connection": "close"}
TIMEOUT = (10, 60)
PAGE_SIZE = 1000
WMS_URL_C = "https://inspirews.skgeodesy.sk/geoserver/cp/ows"
WMS_URL_E = "https://inspirews.skgeodesy.sk/geoserver/cp_uo/ows"
LAYER_C = "cp:CP.CadastralParcel"
LAYER_E = "cp_uo:CP.CadastralParcelUO"
ZONE_C  = "cp:CP.CadastralZoning"
ZONE_E  = "cp_uo:CP.CadastralZoningUO"

# --- HTTP helper ---
def http_get_bytes(url: str, tries: int = 3) -> bytes:
    last: Exception | None = None
    for i in range(tries):
        try:
            r = requests.get(url, headers=HEADERS_XML, timeout=TIMEOUT)
            r.raise_for_status()
            return r.content
        except Exception as e:  # why: WFS býva „krehký“, retry zvyšuje úspešnosť
            last = e
            import time; time.sleep(0.6 * (i + 1))
    assert last is not None
    raise last

# --- FES/CQL builders ---
def xml_escape(text: str) -> str:
    return (text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&apos;"))

def build_fes_filter(ku: str, parcels: List[str]) -> str:
    ku_part = (
        f'<PropertyIsLike wildCard="*" singleChar="." escape="!" matchCase="false">'
        f"<ValueReference>nationalCadastralReference</ValueReference><Literal>{xml_escape(ku)}*</Literal>"
        f"</PropertyIsLike>" if ku else ""
    )
    if parcels:
        inner = []
        for p in parcels:
            p_xml = xml_escape(p)
            ands = [f"<PropertyIsEqualTo><ValueReference>label</ValueReference><Literal>{p_xml}</Literal></PropertyIsEqualTo>"]
            if ku_part: ands.append(ku_part)
            inner.append("<And>" + "".join(ands) + "</And>")
        return f'<Filter xmlns="http://www.opengis.net/fes/2.0"><Or>{"".join(inner)}</Or></Filter>'
    return f'<Filter xmlns="http://www.opengis.net/fes/2.0">{ku_part}</Filter>' if ku_part else ""

def build_cql_filter(ku: str, parcels: List[str]) -> str:
    parts = []
    if parcels:
        q = ",".join(["'" + p.replace("'", "''") + "'" for p in parcels if p])
        if q: parts.append(f"label IN ({q})")
    if ku: parts.append(f"nationalCadastralReference LIKE '{ku}%'")
    return " AND ".join(parts)

# --- Result model ---
@dataclass
class FetchResult:
    ok: bool
    note: str
    pages: List[bytes]
    first_url: str
    detected_epsg: Optional[str] = None

# --- Helpers ---
_gml_has_features = lambda b: (b.find(b"featureMember")!=-1) or (b.find(b":member")!=-1) or (b.find(b"<wfs:member")!=-1)

def _gml_number_returned(xmlb: bytes) -> Optional[int]:
    m = re.search(rb'numberReturned="(\d+)"', xmlb)
    return int(m.group(1)) if m else None

# --- WFS: GML paging ---
def fetch_gml_pages(register: str, ku: str, parcels_csv: str, wfs_srs: Optional[str] = None) -> FetchResult:
    reg = (register or "").upper().strip()
    ku = (ku or "").strip()
    if not ku and not (parcels_csv or "").strip():
        return FetchResult(False, "Zadaj aspoň KU alebo parcelné čísla.", [], "")
    parcels = [p.strip() for p in re.split(r"[,;\s]+", parcels_csv or "") if p.strip()]
    base = CP_UO_WFS_BASE if reg == "E" else CP_WFS_BASE
    typename = TYPE_E if reg == "E" else TYPE_C
    from urllib.parse import urlencode
    fes = build_fes_filter(ku, parcels)
    if not fes: return FetchResult(False, "Neplatný filter (chýba KU aj parcely).", [], "")

    pages: List[bytes] = []
    start = 0
    first_url = ""
    dropped_srs = False
    while True:
        params = {"service": "WFS", "version": "2.0.0", "request": "GetFeature", "typeNames": typename,
                  "count": str(PAGE_SIZE), "startIndex": str(start), "filter": fes}
        if wfs_srs and not dropped_srs: params["srsName"] = wfs_srs
        url = f"{base}?{urlencode(params)}"; first_url = first_url or url
        try:
            xmlb = http_get_bytes(url)
        except requests.HTTPError as e:
            sc = getattr(e.response, "status_code", None)
            # why: niektoré servery vracajú 400 pri prekročení limitu – stopni stránkovanie
            if pages and sc == 400: break
            if (sc == 400 or sc is None) and wfs_srs and not dropped_srs:
                dropped_srs = True; continue
            # fallback: split-by-one alebo CQL
            if sc == 400 and parcels:
                singles: List[bytes] = []
                for pval in parcels:
                    sp = {"service":"WFS","version":"2.0.0","request":"GetFeature","typeNames":typename,
                          "count":"1000","startIndex":"0","filter": build_fes_filter(ku,[pval])}
                    if not dropped_srs and wfs_srs: sp["srsName"] = wfs_srs
                    surl = f"{base}?{urlencode(sp)}"
                    try:
                        sb = http_get_bytes(surl)
                        if _gml_has_features(sb): singles.append(sb)
                    except Exception: pass
                if singles:
                    return FetchResult(True, f"Počet stránok: {len(singles)} (split-by-one)", singles, first_url)
            cql = build_cql_filter(ku, parcels)
            if cql:
                cql_params = {"service":"WFS","version":"2.0.0","request":"GetFeature","typeNames":typename,
                              "count":str(PAGE_SIZE),"startIndex":str(start),"CQL_FILTER": cql}
                if not dropped_srs and wfs_srs: cql_params["srsName"] = wfs_srs
                cql_url = f"{base}?{urlencode(cql_params)}"; first_url = first_url or cql_url
                try:
                    xmlb = http_get_bytes(cql_url)
                except Exception as ee:
                    return FetchResult(False, f"HTTP chyba: {e}\nCQL fallback zlyhal: {ee}", [], first_url or url)
            else:
                return FetchResult(False, f"HTTP chyba: {e}", [], first_url or url)
        except Exception as e:
            return FetchResult(False, f"Chyba: {e}", [], first_url or url)

        nr = _gml_number_returned(xmlb)
        if (nr is not None and nr == 0) or not _gml_has_features(xmlb): break
        pages.append(xmlb)
        if nr is not None:
            if nr < PAGE_SIZE: break
            start += nr
        else:
            if len(xmlb) < 10000: break
            start += PAGE_SIZE
        if start > 500_000: break

    if not pages: return FetchResult(False, "Server vrátil 0 prvkov pre daný filter.", [], first_url)
    return FetchResult(True, f"Počet stránok: {len(pages)}", pages, first_url)

# --- WFS: GeoJSON paging (pre preview/DXF) ---
def fetch_geojson_pages(register: str, ku: str, parcels_csv: str, wfs_srs: Optional[str] = None) -> FetchResult:
    reg = (register or "").upper().strip()
    ku = (ku or "").strip()
    parcels = [p.strip() for p in re.split(r"[,\s]+", parcels_csv or "") if p.strip()]
    base = CP_UO_WFS_BASE if reg == "E" else CP_WFS_BASE
    typename = TYPE_E if reg == "E" else TYPE_C
    from urllib.parse import urlencode
    filt_xml = build_fes_filter(ku, parcels)
    if not filt_xml: return FetchResult(False, "Neplatný filter (chýba KU aj parcely)", [], "")

    pages: List[bytes] = []
    start = 0
    first_url = ""
    while True:
        params = {"service":"WFS","version":"2.0.0","request":"GetFeature","typeNames":typename,
                  "count":str(PAGE_SIZE),"startIndex":str(start),"filter":filt_xml,"outputFormat":"application/json"}
        if wfs_srs: params["srsName"] = wfs_srs
        url = f"{base}?{urlencode(params)}"; first_url = first_url or url
        try:
            jb = http_get_bytes(url)
        except requests.HTTPError as e:
            if pages and getattr(e.response, "status_code", None) == 400: break
            return FetchResult(False, f"HTTP chyba: {e}", [], first_url or url)
        except Exception as e:
            return FetchResult(False, f"Chyba: {e}", [], first_url or url)
        try:
            obj = json.loads(jb.decode("utf-8", "ignore")); feats = obj.get("features", [])
        except Exception:
            feats = []
        if not feats: break
        pages.append(jb)
        if len(feats) < PAGE_SIZE: break
        start += PAGE_SIZE
        if start > 500_000: break
    if not pages: return FetchResult(False, "Server vrátil 0 prvkov pre daný filter.", [], first_url)
    return FetchResult(True, f"Počet stránok: {len(pages)}", pages, first_url)

# --- GeoJSON helpers ---
def merge_geojson_pages(pages: List[bytes], max_features: int = 8000):
    feats, total = [], 0
    for jb in pages:
        try: obj = json.loads(jb.decode("utf-8", "ignore")); f = obj.get("features", [])
        except Exception: f = []
        total += len(f)
        if len(feats) < max_features:
            room = max_features - len(feats)
            feats.extend(f[:room])
        if len(feats) >= max_features: break
    return {"type": "FeatureCollection", "features": feats}, total, len(feats)

def _walk_coords(geom: dict, agg: List[float]):
    if not geom: return
    coords = geom.get("coordinates")
    def _rec(c):
        if isinstance(c, (list, tuple)):
            if c and isinstance(c[0], (int, float)) and isinstance(c[1], (int, float)):
                x, y = float(c[0]), float(c[1])
                agg[0] = min(agg[0], x); agg[1] = min(agg[1], y)
                agg[2] = max(agg[2], x); agg[3] = max(agg[3], y)
            else:
                for cc in c: _rec(cc)
    _rec(coords)

def bbox_from_geojson(obj: dict) -> Optional[Tuple[float, float, float, float]]:
    if not obj: return None
    agg = [float("inf"), float("inf"), float("-inf"), float("-inf")]
    if obj.get("type") == "FeatureCollection":
        for f in obj.get("features", []): _walk_coords((f or {}).get("geometry") or {}, agg)
    elif obj.get("type") == "Feature":
        _walk_coords((obj or {}).get("geometry") or {}, agg)
    else:
        _walk_coords(obj, agg)
    return None if agg[0] == float("inf") else tuple(agg)  # type: ignore[return-value]

def view_from_bbox(bbox: Tuple[float, float, float, float]):
    minx, miny, maxx, maxy = bbox
    cx, cy = (minx + maxx)/2, (miny + maxy)/2
    return dict(center=(cy, cx), zoom=14) 

def fetch_zone_bbox(register: str, ku_code: str) -> Optional[Tuple[float,float,float,float]]:
    """Rýchly bbox pre katastrálne územie cez Zoning WFS (ľahké dáta).
    Skúsi vrstvy podľa registra (E/C) a vráti bbox v EPSG:4326.
    """
    reg = (register or "").upper().strip()
    ku = (ku_code or "").strip()
    if not ku:
        return None
    # kandidáti: prioritne vo vetve daného registra, potom fallback
    candidates = [
        ("cp_uo:CP.CadastralZoningUO", CP_UO_WFS_BASE),
        ("cp:CP.CadastralZoning", CP_WFS_BASE),
    ] if reg == "E" else [
        ("cp:CP.CadastralZoning", CP_WFS_BASE),
        ("cp_uo:CP.CadastralZoningUO", CP_UO_WFS_BASE),
    ]
    from urllib.parse import urlencode
    for type_name, base in candidates:
        try:
            params = {
                "service": "WFS", "version": "2.0.0", "request": "GetFeature",
                "typeNames": type_name, "outputFormat": "application/json",
                "srsName": "EPSG:4326", "CQL_FILTER": f"nationalCadastralReference='{ku}'",
            }
            url = f"{base}?{urlencode(params)}"
            jb = http_get_bytes(url)
            obj = json.loads(jb.decode("utf-8", "ignore"))
            fc = {"type":"FeatureCollection","features": obj.get("features", [])}
            bb = bbox_from_geojson(fc)
            if bb:
                return bb # minx,miny,maxx,maxy in EPSG:4326
            except Exception:
                continue
        return None

# path: parcelone/convert.py
from __future__ import annotations
from typing import List, Tuple
import io, os, shutil, subprocess, tempfile

# Pozn.: držiak na GDAL_DATA, ak ho nájdeme
GDAL_DATA_DIR: str | None = None

def _find_gdal_data() -> str | None:
    candidates = [
        os.environ.get("GDAL_DATA"),
        "/usr/share/gdal", "/usr/share/gdal/3.6", "/usr/share/gdal/3.5",
        "/usr/share/gdal/3.4", "/usr/share/gdal/3.3", "/usr/share/gdal/3.2",
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return None

def ensure_gdal():
    """Vráti tuple (mode, handle) – mode ∈ {'python-gdal','ogr2ogr'}."""
    global GDAL_DATA_DIR
    try:
        from osgeo import gdal  # type: ignore
        GDAL_DATA_DIR = _find_gdal_data() or GDAL_DATA_DIR
        if GDAL_DATA_DIR and not os.environ.get("GDAL_DATA"):
            os.environ["GDAL_DATA"] = GDAL_DATA_DIR
        return ("python-gdal", gdal)
    except Exception:
        pass
    ogr = shutil.which("ogr2ogr")
    if not ogr:
        raise RuntimeError("GDAL/OGR nie je dostupný. Na Streamlit Cloud pridaj packages.txt s gdal-bin.")
    GDAL_DATA_DIR = _find_gdal_data() or GDAL_DATA_DIR
    if GDAL_DATA_DIR and not os.environ.get("GDAL_DATA"):
        os.environ["GDAL_DATA"] = GDAL_DATA_DIR
    return ("ogr2ogr", ogr)

def _run_ogr(ogr: str, args: list[str]):
    # why: ak ogr zlyhá, chceme vidieť jeho stderr
    cp = subprocess.run([ogr] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if cp.returncode != 0:
        raise RuntimeError(cp.stderr.decode("utf-8", "ignore") or "ogr2ogr failed")

def convert_pages_with_gdal(gml_pages: List[bytes], driver: str, out_ext: str) -> tuple[bytes, str, str]:
    """GML stránky → cieľový formát cez GDAL/OGR. Vráti (data, mime, mode)."""
    if not gml_pages:
        raise RuntimeError("Žiadne GML stránky na konverziu.")
    mode, handle = ensure_gdal()

    import zipfile
    with tempfile.TemporaryDirectory() as td:
        # zapíš GML stránky
        gml_paths: list[str] = []
        for i, b in enumerate(gml_pages, 1):
            p = os.path.join(td, f"in_{i:03d}.gml")
            with open(p, "wb") as f:
                f.write(b)
            gml_paths.append(p)

        # pre DXF/SHP: spoľahlivý merge cez GPKG a potom export
        if driver in {"DXF", "ESRI Shapefile"}:
            layer = "parcely"
            gpkg_path = os.path.join(td, "merge.gpkg")
            if mode == "python-gdal":
                from osgeo import gdal  # type: ignore
                gdal.UseExceptions()
                opts = gdal.VectorTranslateOptions(
                    format="GPKG", layerName=layer,
                    geometryType="MULTIPOLYGON", explodeCollections=True
                )
                gdal.VectorTranslate(gpkg_path, gml_paths[0], options=opts)
                for p in gml_paths[1:]:
                    opts_app = gdal.VectorTranslateOptions(
                        format="GPKG", layerName=layer, accessMode="append",
                        geometryType="MULTIPOLYGON", explodeCollections=True
                    )
                    try:
                        gdal.VectorTranslate(gpkg_path, p, options=opts_app)
                    except Exception:
                        pass
                if driver == "DXF":
                    out_path = os.path.join(td, "parcely.dxf")
                    opts_dxf = gdal.VectorTranslateOptions(format="DXF", layerName=layer)
                    gdal.VectorTranslate(out_path, gpkg_path, options=opts_dxf)
                    return open(out_path, "rb").read(), "application/dxf", mode
                else:
                    shp_path = os.path.join(td, "parcely.shp")
                    opts_shp = gdal.VectorTranslateOptions(format="ESRI Shapefile", layerName=layer)
                    gdal.VectorTranslate(shp_path, gpkg_path, options=opts_shp)
                    mem = io.BytesIO()
                    with zipfile.ZipFile(mem, "w") as z:
                        base = os.path.splitext(shp_path)[0]
                        for ext in (".shp", ".shx", ".dbf", ".prj", ".cpg"):
                            fp = base + ext
                            if os.path.exists(fp):
                                z.write(fp, os.path.basename(fp))
                    return mem.getvalue(), "application/zip", mode
            else:
                ogr = handle  # type: ignore[assignment]
                _run_ogr(ogr, ["-f","GPKG", gpkg_path, gml_paths[0], "-nln", layer, "-nlt","MULTIPOLYGON", "-explodecollections"])
                for p in gml_paths[1:]:
                    try:
                        _run_ogr(ogr, ["-f","GPKG", gpkg_path, p, "-nln", layer, "-update","-append", "-nlt","MULTIPOLYGON", "-explodecollections"])
                    except Exception:
                        pass
                if driver == "DXF":
                    out_path = os.path.join(td, "parcely.dxf")
                    _run_ogr(ogr, ["-f","DXF", out_path, gpkg_path, "-nln", layer])
                    return open(out_path, "rb").read(), "application/dxf", mode
                else:
                    shp_path = os.path.join(td, "parcely.shp")
                    _run_ogr(ogr, ["-f","ESRI Shapefile", shp_path, gpkg_path, "-nln", layer])
                    mem = io.BytesIO()
                    with zipfile.ZipFile(mem, "w") as z:
                        base = os.path.splitext(shp_path)[0]
                        for ext in (".shp", ".shx", ".dbf", ".prj", ".cpg"):
                            fp = base + ext
                            if os.path.exists(fp):
                                z.write(fp, os.path.basename(fp))
                    return mem.getvalue(), "application/zip", mode

        # ostatné formáty priamo
        out_path = os.path.join(td, f"out{out_ext}")
        if mode == "python-gdal":
            from osgeo import gdal  # type: ignore
            gdal.UseExceptions()
            opts = gdal.VectorTranslateOptions(format=driver, layerName="parcely")
            gdal.VectorTranslate(out_path, gml_paths[0], options=opts)
            for p in gml_paths[1:]:
                opts_app = gdal.VectorTranslateOptions(format=driver, layerName="parcely", accessMode="append")
                try:
                    gdal.VectorTranslate(out_path, p, options=opts_app)
                except Exception:
                    pass
        else:
            ogr = handle  # type: ignore[assignment]
            _run_ogr(ogr, ["-f", driver, out_path, gml_paths[0], "-nln", "parcely"])
            for p in gml_paths[1:]:
                try:
                    _run_ogr(ogr, ["-f", driver, out_path, p, "-nln", "parcely", "-update", "-append"])
                except Exception:
                    pass

        mime = {
            ".geojson": "application/geo+json",
            ".gpkg": "application/geopackage+sqlite3",
        }.get(out_ext, "application/octet-stream")
        return open(out_path, "rb").read(), mime, mode

def geojson_pages_to_dxf(json_pages: List[bytes]) -> Tuple[bytes, str]:
    """GeoJSON → ASCII DXF (R2000) s LWPOLYLINE. Čistý Python, bez GDAL/ezdxf."""
    import json
    def fmt(v: float) -> str: return ("%.8f" % float(v)).rstrip("0").rstrip(".")
    def add(code, val, out): out.append(str(code)); out.append(str(val))
    def to_polylines(obj):
        rings = []
        def add_ring(ring):
            pts = [(float(x), float(y)) for x, y in ring if isinstance(x,(int,float)) and isinstance(y,(int,float))]
            if len(pts) < 2: return
            if pts[0] == pts[-1]: pts.pop()
            if len(pts) >= 2: rings.append(pts)
        g = (obj or {}).get("geometry") or {}; t = g.get("type")
        if t == "Polygon":
            for ring in g.get("coordinates", []): add_ring(ring)
        elif t == "MultiPolygon":
            for poly in g.get("coordinates", []):
                for ring in poly: add_ring(ring)
        return rings
    polylines = []
    for jb in json_pages:
        try: obj = json.loads(jb.decode("utf-8", "ignore"))
        except Exception: obj = {}
        for f in obj.get("features", []): polylines.extend(to_polylines(f))
    LAYER = "PARCELY"; out: list[str] = []
    add(0, "SECTION", out); add(2, "HEADER", out); add(9, "$ACADVER", out); add(1, "AC1024", out)
    add(0, "ENDSEC", out)
    add(0, "SECTION", out); add(2, "TABLES", out)
    add(0, "TABLE", out); add(2, "LAYER", out); add(70, 1, out)
    add(0, "LAYER", out); add(2, LAYER, out); add(70, 0, out); add(62, 7, out); add(6, "CONTINUOUS", out)
    add(0, "ENDTAB", out); add(0, "ENDSEC", out)
    add(0, "SECTION", out); add(2, "ENTITIES", out)
    for pts in polylines:
        if len(pts) < 2: continue
        add(0, "LWPOLYLINE", out); add(100, "AcDbEntity", out); add(8, LAYER, out)
        add(100, "AcDbPolyline", out); add(90, len(pts), out); add(70, 1, out)
        for x, y in pts: add(10, fmt(x), out); add(20, fmt(y), out)
    add(0, "ENDSEC", out); add(0, "EOF", out)
    return ("\r\n".join(out) + "\r\n").encode("utf-8"), "application/dxf"

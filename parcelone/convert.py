from __future__ import annotations
from typing import List, Tuple
import io, os, shutil, subprocess, tempfile

GDAL_DATA_DIR: str | None = None  # set when found

def _find_gdal_data() -> str | None:
    # why: host paths differ; try common locations
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
    """Return (mode, handle) where mode ∈ {"python-gdal","ogr2ogr"}."""
    global GDAL_DATA_DIR
    # Prefer Python GDAL if available
    try:
        from osgeo import gdal  # type: ignore
        GDAL_DATA_DIR = _find_gdal_data() or GDAL_DATA_DIR
        if GDAL_DATA_DIR and not os.environ.get("GDAL_DATA"):
            os.environ["GDAL_DATA"] = GDAL_DATA_DIR
        return ("python-gdal", gdal)
    except Exception:
        pass
    # Fallback to runtime `ogr2ogr` CLI from `gdal-bin`
    ogr = shutil.which("ogr2ogr")
    if not ogr:
        raise RuntimeError("GDAL/OGR nie je dostupný. Na Streamlit Cloud pridaj packages.txt s gdal-bin.")
    GDAL_DATA_DIR = _find_gdal_data() or GDAL_DATA_DIR
    if GDAL_DATA_DIR and not os.environ.get("GDAL_DATA"):
        os.environ["GDAL_DATA"] = GDAL_DATA_DIR
    return ("ogr2ogr", ogr)

def _run_ogr(ogr: str, args: list[str]):
    cp = subprocess.run([ogr] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if cp.returncode != 0:
        raise RuntimeError(cp.stderr.decode("utf-8", "ignore") or "ogr2ogr failed")

def convert_pages_with_gdal(gml_pages: List[bytes], driver: str, out_ext: str) -> tuple[bytes, str, str]:
    """Merge GML chunks → convert to target format using GDAL/OGR.
    Returns (data, mime, mode)."""
    if not gml_pages:
        raise RuntimeError("Žiadne GML stránky na konverziu.")

    mode, handle = ensure_gdal()

    import zipfile
    with tempfile.TemporaryDirectory() as td:
        # 1) write GML pages to disk
        gml_paths: list[str] = []
        for i, b in enumerate(gml_pages, 1):
            p = os.path.join(td, f"in_{i:03d}.gml")
            with open(p, "wb") as f: f.write(b)
            gml_paths.append(p)

        # 2) For DXF/SHP do a robust merge via GPKG first (handles layer append)
        if driver in {"DXF", "ESRI Shapefile"}:
            layer = "parcely"
            gpkg_path = os.path.join(td, "merge.gpkg")
            if mode == "python-gdal":
                from osgeo import gdal  # type: ignore
                gdal.UseExceptions()
                # create/append into GPKG
                opts = gdal.VectorTranslateOptions(format="GPKG", layerName=layer, geometryType="MULTIPOLYGON", explodeCollections=True)
                gdal.VectorTranslate(gpkg_path, gml_paths[0], options=opts)
                for p in gml_paths[1:]:
                    opts_app = gdal.VectorTranslateOptions(format="GPKG", layerName=layer, accessMode="append", geometryType="MULTIPOLYGON", explodeCollections=True)
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
                            if os.path.exists(fp): z.write(fp, os.path.basename(fp))
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
                            if os.path.exists(fp): z.write(fp, os.path.basename(fp))
                    return mem.getvalue(), "application/zip", mode

        # 3) Other formats directly
        out_path = os.path.join(td, f"out{out_ext}")
        if mode == "python-gdal":
            from osgeo import gdal  # type: ignore
            gdal.UseExceptions()
            opts = gdal.VectorTranslateOptions(format=driver, layerName="parcely")
            gdal.VectorTranslate(out_path, gml_paths[0], options=opts)
            for p in gml_paths[1:]:
                try:
                    opts_app = gdal.VectorTranslateOptions(format=driver, layerName="parcely", accessMode="append")
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
        mime = {".geojson": "application/geo+json", ".gpkg": "application/geopackage+sqlite3"}.get(out_ext, "application/octet-stream")
        return open(out_path, "rb").read(), mime, mode

from __future__ import annotations

import glob
import io
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple

# ----------------------------- GDAL helpers ---------------------------------

def _unique(seq: List[str]) -> List[str]:
    out: List[str] = []
    for x in seq:
        if x and x not in out:
            out.append(x)
    return out


def gdal_data_candidates() -> List[str]:
    cands: List[str] = []
    gd_env = os.environ.get("GDAL_DATA")
    if gd_env:
        cands.append(gd_env)

    ogr = shutil.which("ogr2ogr.exe") or shutil.which("ogr2ogr")
    if ogr:
        p = Path(ogr).resolve()
        root = p.parent.parent
        cands += [
            str(root / "share" / "gdal"),
            str(root / "apps" / "gdal" / "share" / "gdal"),
        ]

    for q in glob.glob(r"C:\\Program Files\\QGIS *"):
        base = Path(q)
        cands += [
            str(base / "share" / "gdal"),
            str(base / "apps" / "gdal" / "share" / "gdal"),
        ]

    try:
        import osgeo  # type: ignore
        cands.append(str(Path(osgeo.__file__).resolve().parent / "data"))
    except Exception:
        pass

    for pat in [
        "/usr/share/gdal",
        "/usr/local/share/gdal",
        "/opt/homebrew/share/gdal",
    ]:
        cands += glob.glob(pat)

    return _unique(cands)


def find_gdal_data_dir() -> Optional[str]:
    cands = gdal_data_candidates()
    for c in cands:
        if Path(c, "header.dxf").exists() and Path(c, "gml_registry.xml").exists():
            return c
    for c in cands:
        if Path(c).exists():
            return c
    return None


def ensure_gdal_data_env() -> Optional[str]:
    gd = find_gdal_data_dir()
    if gd:
        os.environ["GDAL_DATA"] = gd
        try:
            from osgeo import gdal  # type: ignore
            gdal.SetConfigOption("GDAL_DATA", gd)
        except Exception:
            pass
    return gd


def find_ogr2ogr_windows() -> Optional[str]:
    candidates = [
        r"C:\\Program Files\\QGIS*\\bin\\ogr2ogr.exe",
        r"C:\\Program Files\\QGIS*\\apps\\gdal*\\bin\\ogr2ogr.exe",
        r"C:\\OSGeo4W64\\bin\\ogr2ogr.exe",
        r"C:\\OSGeo4W\\bin\\ogr2ogr.exe",
        r"C:\\Program Files\\GDAL\\ogr2ogr.exe",
    ]
    for pat in candidates:
        hits = sorted(glob.glob(pat), reverse=True)
        if hits:
            return hits[0]
    return None


def ensure_gdal() -> Tuple[str, object]:
    try:
        from osgeo import gdal as _gdal  # type: ignore
        return ("python-gdal", _gdal)
    except Exception:
        pass
    ogr = shutil.which("ogr2ogr") or shutil.which("ogr2ogr.exe") or find_ogr2ogr_windows()
    if ogr:
        return ("ogr2ogr-cli", ogr)
    raise RuntimeError(
        "GDAL nie je k dispozícii. Nainštaluj Python balík 'GDAL' (pip install GDAL) "
        "alebo QGIS/OSGeo4W a pridaj 'ogr2ogr' do PATH, prípadne zvoľ výstup 'gml-zip'."
    )


def run_ogr(ogr_path: str, args: List[str]) -> None:
    env = os.environ.copy()
    if not env.get("GDAL_DATA"):
        gd = ensure_gdal_data_env()
        if gd:
            env["GDAL_DATA"] = gd
    proc = subprocess.run([ogr_path] + args, capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"ogr2ogr failed: {args}")


GDAL_DATA_DIR = ensure_gdal_data_env()

# ----------------------------- Konverzie (GDAL) -----------------------------

def convert_pages_with_gdal(pages: List[bytes], driver: str, out_ext: str) -> Tuple[bytes, str, str]:
    mode, handle = ensure_gdal()
    with tempfile.TemporaryDirectory() as td:
        gml_paths: List[str] = []
        for i, b in enumerate(pages, 1):
            p = os.path.join(td, f"page_{i:03d}.gml")
            with open(p, "wb") as f:
                f.write(b)
            gml_paths.append(p)

        if driver == "ESRI Shapefile":
            gpkg_path = os.path.join(td, "merge_for_shp.gpkg")
            layer_name = "parcely"

            if mode == "python-gdal":
                gdal = handle  # type: ignore
                gdal.UseExceptions()
                opts = gdal.VectorTranslateOptions(
                    format="GPKG",
                    layerName=layer_name,
                    geometryType="MULTIPOLYGON",
                    explodeCollections=True,
                )
                gdal.VectorTranslate(destNameOrDestDS=gpkg_path, srcDS=gml_paths[0], options=opts)
                for p in gml_paths[1:]:
                    opts_app = gdal.VectorTranslateOptions(
                        format="GPKG",
                        layerName=layer_name,
                        accessMode="append",
                        geometryType="MULTIPOLYGON",
                        explodeCollections=True,
                    )
                    gdal.VectorTranslate(destNameOrDestDS=gpkg_path, srcDS=p, options=opts_app)
                shp_path = os.path.join(td, "parcely.shp")
                opts_shp = gdal.VectorTranslateOptions(format="ESRI Shapefile", layerName=layer_name)
                gdal.VectorTranslate(destNameOrDestDS=shp_path, srcDS=gpkg_path, options=opts_shp)

            else:
                ogr = handle
                run_ogr(ogr, ["-f", "GPKG", gpkg_path, gml_paths[0],
                              "-nln", layer_name, "-nlt", "MULTIPOLYGON", "-explodecollections"])
                for p in gml_paths[1:]:
                    run_ogr(ogr, ["-f", "GPKG", gpkg_path, p,
                                  "-nln", layer_name, "-update", "-append",
                                  "-nlt", "MULTIPOLYGON", "-explodecollections"])
                shp_path = os.path.join(td, "parcely.shp")
                run_ogr(ogr, ["-f", "ESRI Shapefile", shp_path, gpkg_path, "-nln", layer_name])

            mem = io.BytesIO()
            import zipfile
            with zipfile.ZipFile(mem, mode="w") as z:
                base = os.path.splitext(shp_path)[0]
                for ext in (".shp", ".shx", ".dbf", ".prj", ".cpg"):
                    fp = base + ext
                    if os.path.exists(fp):
                        z.write(fp, arcname=os.path.basename(fp))
            return mem.getvalue(), "application/zip", mode

        if driver == "DXF":
            gpkg_path = os.path.join(td, "merge.gpkg")
            layer_name = "parcely"
            out_path = os.path.join(td, "parcely.dxf")

            if mode == "python-gdal":
                gdal = handle  # type: ignore
                gdal.UseExceptions()
                opts = gdal.VectorTranslateOptions(format="GPKG", layerName=layer_name)
                gdal.VectorTranslate(destNameOrDestDS=gpkg_path, srcDS=gml_paths[0], options=opts)
                for p in gml_paths[1:]:
                    opts_app = gdal.VectorTranslateOptions(format="GPKG", layerName=layer_name, accessMode="append")
                    gdal.VectorTranslate(destNameOrDestDS=gpkg_path, srcDS=p, options=opts_app)
                opts_dxf = gdal.VectorTranslateOptions(format="DXF", layerName=layer_name)
                gdal.VectorTranslate(destNameOrDestDS=out_path, srcDS=gpkg_path, options=opts_dxf)
            else:
                ogr = handle
                run_ogr(ogr, ["-f", "GPKG", gpkg_path, gml_paths[0], "-nln", layer_name])
                for p in gml_paths[1:]:
                    run_ogr(ogr, ["-f", "GPKG", gpkg_path, p, "-nln", layer_name, "-update", "-append"])
                run_ogr(ogr, ["-f", "DXF", out_path, gpkg_path, "-nln", layer_name])

            return open(out_path, "rb").read(), "application/dxf", mode

        out_path = os.path.join(td, f"parcely{out_ext}")
        if mode == "python-gdal":
            gdal = handle  # type: ignore
            gdal.UseExceptions()
            opts = gdal.VectorTranslateOptions(format=driver, layerName="parcely")
            gdal.VectorTranslate(destNameOrDestDS=out_path, srcDS=gml_paths[0], options=opts)
            for p in gml_paths[1:]:
                opts_app = gdal.VectorTranslateOptions(format=driver, layerName="parcely", accessMode="append")
                try:
                    gdal.VectorTranslate(destNameOrDestDS=out_path, srcDS=p, options=opts_app)
                except Exception:
                    pass
        else:
            ogr = handle
            run_ogr(ogr, ["-f", driver, out_path, gml_paths[0], "-nln", "parcely"])
            for p in gml_paths[1:]:
                try:
                    run_ogr(ogr, ["-f", driver, out_path, p, "-nln", "parcely", "-update", "-append"])
                except Exception:
                    pass

        mime = {
            ".geojson": "application/geo+json",
            ".gpkg": "application/geopackage+sqlite3",
        }.get(out_ext, "application/octet-stream")
        return open(out_path, "rb").read(), mime, mode


def gml_pages_to_dxf(gml_pages: List[bytes]) -> Tuple[bytes, str]:
    """
    Minimalistický fallback: vyparsuje gml:posList/gml:pos z GML a vykreslí
    uzavreté polyline do DXF. Nevyžaduje GDAL ani nové sieťové volania.
    """
    import xml.etree.ElementTree as ET
    import ezdxf

    ns = {"gml": "http://www.opengis.net/gml"}
    doc = ezdxf.new(setup=True)
    msp = doc.modelspace()
    LAYER = "PARCELY"
    if LAYER not in doc.layers:
        doc.layers.new(name=LAYER)

    def parse_poslist(txt: str) -> list[tuple[float, float]]:
        vals = [float(x) for x in re.split(r"[ ,\s]+", (txt or "").strip()) if x]
        pts = [(vals[i], vals[i+1]) for i in range(0, len(vals) - 1, 2)]
        if pts and pts[0] != pts[-1]:
            pts.append(pts[0])
        return pts

    def add_ring(pts: list[tuple[float, float]]):
        if len(pts) >= 4:
            msp.add_lwpolyline(pts, format="xy", dxfattribs={"layer": LAYER, "closed": True})

    for b in gml_pages:
        try:
            root = ET.fromstring(b)
        except Exception:
            continue

        for poslist in root.findall(".//gml:posList", ns):
            add_ring(parse_poslist(poslist.text or ""))

        rings = []
        cur = []
        for pos in root.findall(".//gml:pos", ns):
            parts = [float(x) for x in (pos.text or "").replace(",", " ").split() if x]
            if len(parts) >= 2:
                cur.append((parts[0], parts[1]))
            if len(cur) >= 4 and cur[0] == cur[-1]:
                rings.append(cur); cur = []
        if cur:
            rings.append(cur)
        for r in rings:
            if r and r[0] != r[-1]:
                r = r + [r[0]]
            if len(r) >= 4:
                msp.add_lwpolyline(r, format="xy", dxfattribs={"layer": LAYER, "closed": True})

    with tempfile.TemporaryDirectory() as td:
        out_path = os.path.join(td, "parcely.dxf")
        try:
            doc.saveas(out_path)
        except AttributeError:
            doc.save()
        with open(out_path, "rb") as f:
            data = f.read()
    return data, "application/dxf"


def geojson_pages_to_dxf(json_pages: List[bytes]) -> Tuple[bytes, str]:
    import ezdxf
    import tempfile
    import json
    import os

    doc = ezdxf.new(setup=True)
    msp = doc.modelspace()
    LAYER = "PARCELY"
    if LAYER not in doc.layers:
        doc.layers.new(name=LAYER)

    def add_polygon(coords):
        for ring in coords:
            pts = [(float(x), float(y)) for x, y in ring]
            if pts and pts[0] != pts[-1]:
                pts.append(pts[0])
            if len(pts) >= 4:
                msp.add_lwpolyline(pts, format="xy", dxfattribs={"layer": LAYER, "closed": True})

    for b in json_pages:
        try:
            obj = json.loads(b.decode("utf-8", "ignore"))
            for feat in obj.get("features", []):
                geom = feat.get("geometry", {})
                if geom.get("type") == "Polygon":
                    add_polygon(geom.get("coordinates", []))
                elif geom.get("type") == "MultiPolygon":
                    for part in geom.get("coordinates", []):
                        add_polygon(part)
        except Exception:
            continue

    with tempfile.TemporaryDirectory() as td:
        out_path = os.path.join(td, "parcely.dxf")
        try:
            doc.saveas(out_path)
        except AttributeError:
            doc.save()
        with open(out_path, "rb") as f:
            data = f.read()
    return data, "application/dxf"

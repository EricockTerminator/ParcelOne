from importlib import import_module


__all__ = [
    "ui", "wfs", "convert", "ku",
    "fetch_gml_pages", "fetch_geojson_pages", "merge_geojson_pages",
    "bbox_from_geojson", "view_from_bbox",
    "convert_pages_with_gdal", "geojson_pages_to_dxf",
    "load_ku_table", "lookup_ku_code",
]


def __getattr__(name: str):
    if name in {"fetch_gml_pages","fetch_geojson_pages","merge_geojson_pages","bbox_from_geojson","view_from_bbox"}:
        return getattr(import_module(".wfs", __name__), name)
    if name in {"convert_pages_with_gdal","geojson_pages_to_dxf"}:
        return getattr(import_module(".convert", __name__), name)
    if name in {"load_ku_table","lookup_ku_code"}:
        return getattr(import_module(".ku", __name__), name)
    raise AttributeError(name)

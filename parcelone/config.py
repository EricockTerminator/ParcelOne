from __future__ import annotations
import json
import os

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - python-dotenv optional
    def load_dotenv(*_args, **_kwargs):  # type: ignore[override]
        return False

# Load environment variables from a .env file if present
load_dotenv()

# Default constants
_DEFAULT_CP_WFS_BASE = "https://inspirews.skgeodesy.sk/geoserver/cp/ows"
_DEFAULT_PAGE_SIZE = 1000
_DEFAULT_WFS_CRS_CHOICES = {
    "auto (server default)": None,
    "EPSG:5514 (S-JTSK / Krovák EN)": "EPSG:5514",
    "EPSG:4258 (ETRS89)": "EPSG:4258",
    "EPSG:4326 (WGS84)": "EPSG:4326",
}

# Exposed configuration with environment variable overrides
CP_WFS_BASE: str = os.getenv("CP_WFS_BASE", _DEFAULT_CP_WFS_BASE)
PAGE_SIZE: int = int(os.getenv("PAGE_SIZE", str(_DEFAULT_PAGE_SIZE)))

_env_crs = os.getenv("WFS_CRS_CHOICES")
if _env_crs:
    try:
        WFS_CRS_CHOICES: dict[str, str | None] = json.loads(_env_crs)
    except json.JSONDecodeError:
        WFS_CRS_CHOICES = _DEFAULT_WFS_CRS_CHOICES
else:
    WFS_CRS_CHOICES = _DEFAULT_WFS_CRS_CHOICES

__all__ = ["CP_WFS_BASE", "PAGE_SIZE", "WFS_CRS_CHOICES"]

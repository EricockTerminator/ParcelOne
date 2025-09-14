

"""Index and lookup for katastrálne územie (KU) kódy podľa názvu.

- Expects packaged data file with pairs: "Meno" KOD (space separated) or CSV
- Provides robust, diacritics-insensitive search with simple fuzzy matching
- Stays pure-Python, no extra deps
"""
from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
import io
import re
import unicodedata
from typing import Iterable, List, Tuple, Dict

_DATA_PACKAGE = __name__.rsplit(".", 1)[0] + ".data"
_DATA_FILE_TXT = "KodKU.txt"   # original format: "Name" 800040
_DATA_FILE_CSV = "kod_ku.csv"  # optional fallback CSV: name,code


@dataclass(frozen=True)
class KU:
    name: str
    code: str  # zero-padded numeric-as-string


# --- normalization ---------------------------------------------------------

def _normalize(s: str) -> str:
    """Lowercase, strip diacritics, collapse spaces & punctuation.
    Why: to match user input like "bratislava - stare mesto" to canonical names.
    """
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# --- loading ---------------------------------------------------------------

def _load_text_pairs(blob: str) -> List[KU]:
    rows: List[KU] = []
    # Lines like: "Abrahám" 800058
    for line in blob.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^\s*\"(.+?)\"\s+(\d{6})\s*$", line)
        if not m:
            continue
        name, code = m.group(1), m.group(2)
        rows.append(KU(name=name, code=code))
    return rows


def _load_csv_pairs(blob: str) -> List[KU]:
    rows: List[KU] = []
    for ln in blob.splitlines():
        if not ln.strip() or ln.lstrip().startswith("#"):
            continue
        parts = [p.strip().strip('"') for p in ln.split(",")]
        if len(parts) < 2:
            continue
        name, code = parts[0], parts[1]
        if re.fullmatch(r"\d{6}", code):
            rows.append(KU(name=name, code=code))
    return rows


def _read_pkg_text(filename: str) -> str:
    with resources.files(_DATA_PACKAGE).joinpath(filename).open("rb") as fh:
        return io.TextIOWrapper(fh, encoding="utf-8").read()


def load_index() -> Tuple[List[KU], Dict[str, List[KU]]]:
    """Load KU rows and a normalized index dict."""
    # Try TXT first, then CSV
    blob: str
    try:
        blob = _read_pkg_text(_DATA_FILE_TXT)
        rows = _load_text_pairs(blob)
    except FileNotFoundError:
        blob = _read_pkg_text(_DATA_FILE_CSV)
        rows = _load_csv_pairs(blob)

    index: Dict[str, List[KU]] = {}
    for ku in rows:
        key = _normalize(ku.name)
        index.setdefault(key, []).append(ku)
    return rows, index


# Cache at module import (small ~ few thousand rows)
_ROWS, _INDEX = load_index()


# --- API -------------------------------------------------------------------

def find_by_name(name: str) -> List[KU]:
    """Return all KUs whose normalized name equals the normalized query.

    If no exact normalized match, fall back to prefix contains heuristic.
    """
    query = _normalize(name)
    exact = _INDEX.get(query, [])
    if exact:
        return exact
    # Very light fallback: return any whose normalized form contains the query
    # or vice versa (helps with "bratislava stare mesto" vs "stare mesto").
    out: List[KU] = []
    for key, lst in _INDEX.items():
        if query and (query in key or key in query):
            out.extend(lst)
    return out


def code_for(name_or_code: str) -> Tuple[str | None, List[KU]]:
    """If input is a 6-digit code, return it. Otherwise search by name.

    Returns (code or None, candidates). If ambiguous, code is None and
    `candidates` contains options for the UI to disambiguate.
    """
    s = name_or_code.strip()
    if re.fullmatch(r"\d{6}", s):
        return s, []
    hits = find_by_name(s)
    if len(hits) == 1:
        return hits[0].code, []
    return None, hits


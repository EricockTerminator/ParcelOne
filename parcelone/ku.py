from __future__ import annotations
from typing import Tuple
import importlib.resources as res
import io, re, unicodedata

_KU_QUOTED_RE = re.compile(r'^\s*"(?P<name>.+?)"\s+(?P<code>\d{6,})\s*$')

def _strip_accents(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower()
    s = re.sub(r"[\-–—]+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return " ".join(s.split())

def _parse_ku_line(line: str):
    m = _KU_QUOTED_RE.match((line or "").strip())
    if not m: return None, None
    return m.group("code"), m.group("name").strip()

def load_ku_table(file_bytes: bytes | None = None) -> list[dict]:
    """Load KU codes from `parcelone/data/KodKU.txt` or provided bytes."""
    text = ""
    if file_bytes is not None:
        try: text = file_bytes.decode("utf-8", "ignore")
        except Exception: text = file_bytes.decode("cp1250", "ignore")
    else:
        try:
            with res.files("parcelone.data").joinpath("KodKU.txt").open("rb") as f:
                raw = f.read()
            try: text = raw.decode("utf-8", "ignore")
            except Exception: text = raw.decode("cp1250", "ignore")
        except Exception:
            return []  # why: app funguje aj bez tabuľky (užívateľ môže zadať kód KU ručne)
    items, seen = [], set()
    for line in text.splitlines():
        code, name = _parse_ku_line(line)
        if not code or code in seen: continue
        seen.add(code)
        nm = name or code
        items.append({"code": code, "name": nm, "norm": _strip_accents(nm)})
    return items

def lookup_ku_code(ku_table: list[dict], query: str) -> tuple[str | None, list[dict]]:
    q = (query or "").strip()
    if not q: return None, []
    if q.isdigit(): return q, []
    nq = _strip_accents(q)
    for it in ku_table:
        if it["norm"] == nq: return it["code"], [it]
    hits = [it for it in ku_table if nq in it["norm"] or it["norm"].startswith(nq)]
    hits.sort(key=lambda x: (len(x["norm"]), x["norm"]))
    return (hits[0]["code"], hits[:10]) if hits else (None, [])

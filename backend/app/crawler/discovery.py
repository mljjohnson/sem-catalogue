from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from bs4 import BeautifulSoup

from app.crawler.coupons import COUPON_MARKERS, CODE_PATTERN


def extract_brand_candidates(html: str) -> List[Tuple[str, str]]:
    """Return list of (source, value) brand candidates.

    Sources: data_attr, img_alt, anchor_text, domain
    """
    soup = BeautifulSoup(html, "lxml")
    out: List[Tuple[str, str]] = []

    # data-* attributes on anchors/cards
    for el in soup.select("a, [data-brand], [data-partner]"):
        for attr in ("data-brand", "data-partner", "data_name", "data-company"):
            v = el.get(attr)
            if v:
                out.append(("data_attr", v.strip()))

    # image alts near anchors
    for img in soup.find_all("img"):
        alt = img.get("alt")
        if alt and alt.strip():
            out.append(("img_alt", alt.strip()))

    # anchor text
    for a in soup.find_all("a"):
        txt = (a.get_text(" ", strip=True) or "").strip()
        if txt and len(txt) <= 80:
            out.append(("anchor_text", txt))

    # linked domains
    for a in soup.find_all("a", href=True):
        href = a.get("href") or ""
        if href.startswith("http://") or href.startswith("https://"):
            from urllib.parse import urlparse
            try:
                host = (urlparse(href).netloc or "").lower()
                if host:
                    out.append(("domain", host))
            except Exception:
                pass

    return out


def extract_coupon_candidates(html: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=False)
    lowered = text.lower()
    hits: List[Tuple[str, str]] = []
    for marker in COUPON_MARKERS:
        start = 0
        while True:
            idx = lowered.find(marker, start)
            if idx == -1:
                break
            window = text[max(0, idx - 120): idx + len(marker) + 140]
            for token in CODE_PATTERN.findall(window):
                hits.append((marker, token))
            start = idx + len(marker)
    return hits


def append_rows_csv(path: Path, header: Iterable[str], rows: Iterable[Iterable[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(list(header))
        for r in rows:
            w.writerow(list(r))





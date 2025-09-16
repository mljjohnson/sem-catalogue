from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup

from app.utils.mappings import load_affiliate_networks, load_brand_domains, normalize_brand


@dataclass
class AffiliateBrand:
    brand_slug: str
    brand_name: str
    position: Optional[str]
    module_type: Optional[str]


def _is_affiliate_href(href: str) -> bool:
    nets = load_affiliate_networks()
    hosts = {h.lower() for h in nets.get("hosts", [])}
    params = set(nets.get("param_patterns", []))
    try:
        parsed = urlparse(href)
    except Exception:
        return False
    host = (parsed.netloc or "").lower()
    if any(h in host for h in hosts):
        return True
    q = parse_qs(parsed.query)
    if any(k in q for k in params):
        return True
    return False


def _infer_brand_from_context(a_tag) -> Optional[str]:
    # Data attributes
    for attr in ("data-brand", "data-partner", "data_name", "data-company"):
        v = a_tag.get(attr)
        if v:
            nb = normalize_brand(v)
            if nb:
                return nb
    # Nearby image alt
    img = a_tag.find("img") or a_tag.find_next("img")
    if img and img.get("alt"):
        nb = normalize_brand(img.get("alt") or "")
        if nb:
            return nb
    # Domain mapping
    try:
        parsed = urlparse(a_tag.get("href") or "")
        domain_map = load_brand_domains()
        host = (parsed.netloc or "").lower()
        for dom, slug in domain_map.items():
            if dom in host:
                return slug
    except Exception:
        pass
    # Fallback: text near anchor
    text = (a_tag.get_text(" ", strip=True) or "").lower()
    if text:
        nb = normalize_brand(text)
        return nb
    return None


def extract_affiliate_brands(html: str) -> Tuple[List[AffiliateBrand], List[str]]:
    soup = BeautifulSoup(html, "lxml")

    affiliates: List[AffiliateBrand] = []
    brands_seen: set[str] = set()

    anchors = soup.find_all("a", href=True)
    # Detect modules/cards for position inference
    cards = soup.select('[class*="card"], [class*="list-item"], [class*="result"], [role="listitem"]')
    card_positions = {id(card): idx + 1 for idx, card in enumerate(cards)}

    for a in anchors:
        href = a.get("href") or ""
        if not href:
            continue
        if not _is_affiliate_href(href):
            continue
        brand = _infer_brand_from_context(a)
        if not brand:
            continue
        if brand in brands_seen:
            # still capture another position if earlier
            pass
        # position by closest card ancestor index
        pos = None
        module_type = None
        parent = a.parent
        while parent is not None and getattr(parent, "name", None) != "body":
            cid = id(parent)
            if cid in card_positions:
                pos = f"P{card_positions[cid]}"
                classes = parent.get("class") or []
                module_type = "card" if any("card" in c for c in classes) else None
                break
            parent = parent.parent

        affiliates.append(AffiliateBrand(brand_slug=brand, brand_name=brand, position=pos, module_type=module_type))
        brands_seen.add(brand)

    brand_list = sorted(brands_seen)
    return affiliates, brand_list





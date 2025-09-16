import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional


ROOT = Path(__file__).resolve().parents[3]


@lru_cache(maxsize=1)
def load_brand_aliases() -> Dict[str, List[str]]:
    path = ROOT / "config" / "brand_aliases.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


@lru_cache(maxsize=1)
def load_category_to_vertical() -> Dict[str, str]:
    path = ROOT / "config" / "category_to_vertical.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


@lru_cache(maxsize=1)
def load_affiliate_networks() -> Dict[str, List[str]]:
    path = ROOT / "config" / "affiliate_networks.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"hosts": [], "param_patterns": []}


@lru_cache(maxsize=1)
def load_brand_domains() -> Dict[str, str]:
    path = ROOT / "config" / "brand_domains.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def normalize_brand(name: str) -> Optional[str]:
    if not name:
        return None
    target = name.strip().lower()
    aliases = load_brand_aliases()
    for slug, variants in aliases.items():
        if target == slug:
            return slug
        for v in variants:
            if target == v.strip().lower():
                return slug
    return target or None


def map_vertical(primary_category: Optional[str]) -> Optional[str]:
    if not primary_category:
        return None
    mapping = load_category_to_vertical()
    return mapping.get(primary_category) or None





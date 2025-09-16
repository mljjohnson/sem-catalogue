import re
from dataclasses import dataclass
from typing import List, Tuple

from bs4 import BeautifulSoup


COUPON_MARKERS = [
    "coupon",
    "promo",
    "promo code",
    "promotion",
    "offer",
    "discount",
    "deal",
    "voucher",
    "code",
]

# Candidate code tokens: allow A-Z, 0-9 and hyphen/underscore, avoid all-numeric
CODE_PATTERN = re.compile(r"\b(?=[A-Za-z0-9_-]{5,20}\b)(?=.*[A-Za-z])[A-Za-z0-9][A-Za-z0-9_-]{3,18}[A-Za-z0-9]\b")

# Exclusions that often collide with codes
EXCLUSION_PATTERNS = [
    re.compile(r"\b\d{3}[-\s]?\d{3}[-\s]?\d{4}\b"),  # phone numbers
    re.compile(r"\b\d{1,2}[\-/]\d{1,2}[\-/]\d{2,4}\b"),  # dates
]


@dataclass
class CouponDetection:
    has_coupons: bool
    codes: List[str]
    debug_hits: List[Tuple[str, str]]  # (marker, code)


def _is_excluded(token: str) -> bool:
    for pat in EXCLUSION_PATTERNS:
        if pat.search(token):
            return True
    return False


def detect_coupons(html: str) -> CouponDetection:
    soup = BeautifulSoup(html, "lxml")

    # Fast path: look for obvious components
    obvious = soup.select('[class*="coupon"], [id*="coupon"], [class*="promo"], [id*="promo"], [class*="deal"], [id*="deal"], [class*="offer"], [id*="offer"]')
    for node in obvious:
        text = node.get_text(" ", strip=True)
        codes = [t for t in CODE_PATTERN.findall(text) if not _is_excluded(t)]
        if codes:
            uniq = sorted(set(codes), key=lambda x: text.find(x))
            return CouponDetection(True, uniq, [("component", c) for c in uniq])

    # General text scan with context windows
    body_text = soup.get_text(" ")
    lowered = body_text.lower()
    hits: List[Tuple[str, str]] = []

    for marker in COUPON_MARKERS:
        start = 0
        while True:
            idx = lowered.find(marker, start)
            if idx == -1:
                break
            window_start = max(0, idx - 100)
            window_end = min(len(body_text), idx + len(marker) + 120)
            window = body_text[window_start:window_end]
            for token in CODE_PATTERN.findall(window):
                if not _is_excluded(token):
                    hits.append((marker, token))
            start = idx + len(marker)

    codes = [c for _, c in hits]
    if codes:
        # Normalize codes to uppercase for consistency
        normalized = []
        seen = set()
        for c in codes:
            cu = c.upper()
            if cu not in seen:
                seen.add(cu)
                normalized.append(cu)
        return CouponDetection(True, normalized, hits)

    return CouponDetection(False, [], [])






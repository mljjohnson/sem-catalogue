import hashlib
import re
from urllib.parse import urlparse, urlunparse, parse_qsl

TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "msclkid",
    "aff_id",
    "aff_sub",
    "subid",
    "sid",
}


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    # Lowercase scheme and netloc
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    path = re.sub(r"//+", "/", parsed.path)
    # Remove trailing slash except root
    if path.endswith("/") and len(path) > 1:
        path = path[:-1]
    # Strip tracking params
    q = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k not in TRACKING_PARAMS]
    query = "&".join([f"{k}={v}" for k, v in q]) if q else ""
    return urlunparse((scheme, netloc, path, "", query, ""))


def page_id_from_canonical(canonical_url: str) -> str:
    normalized = normalize_url(canonical_url)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()





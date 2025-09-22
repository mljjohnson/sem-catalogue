import argparse
import asyncio
import time
import csv
import io
from datetime import date
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

from loguru import logger
import sqlalchemy as sa
from app.crawler.scrape import fetch_html
from app.crawler.coupons import detect_coupons
from app.crawler.affiliates import extract_affiliate_brands
from app.core.config import settings
from app.models.db import get_session
from app.models.tables import PageSEMInventory
from app.services.pages import upsert_page
from app.utils.canonical import normalize_url, page_id_from_canonical
from app.utils.mappings import map_vertical


def extract_canonical(html: str, fallback_url: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    link = soup.find("link", rel=lambda v: v and "canonical" in v)
    href = link.get("href") if link else None
    if href:
        return normalize_url(href)
    return normalize_url(fallback_url)


def extract_page_meta(html: str) -> tuple[Optional[str], Optional[str]]:
    # PrimaryCategory and TemplateName from the page-level data script if present
    soup = BeautifulSoup(html, "lxml")
    script = soup.find("script", id="pageLevelData")
    if script and script.string:
        text = script.string
        def _grab(key: str) -> Optional[str]:
            import re
            m = re.search(rf'"{key}":\s*"(.*?)"', text)
            return m.group(1) if m else None
        return _grab("PrimaryCategory"), _grab("TemplateName")
    # fallback
    return None, None


_rate_lock = asyncio.Lock()
_last_fetch_ts: float = 0.0


async def _respect_rate_limit(min_interval_s: float = 1.0) -> None:
    global _last_fetch_ts
    async with _rate_lock:
        now = time.monotonic()
        wait = min_interval_s - (now - _last_fetch_ts)
        if wait > 0:
            await asyncio.sleep(wait)
        _last_fetch_ts = time.monotonic()


async def crawl_url(url: str) -> None:
    # Sanitize incoming URL from CSV
    url = url.strip().strip('"').strip("'")
    if url and not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url
    try:
        await _respect_rate_limit(1.0)
        status, html = await fetch_html(url, render_js=True)
    except Exception as e:
        logger.warning("JS fetch failed for {}: {}", url, e)
        status, html = 0, ""

    # Fallback to non-JS if first attempt failed or body is suspiciously small
    if status == 0 or (status >= 400) or len(html) < 5000:
        try:
            await _respect_rate_limit(1.0)
            status2, html2 = await fetch_html(url, render_js=False)
            if status2 and (status == 0 or len(html) < len(html2)):
                status, html = status2, html2
        except Exception as e:
            logger.warning("Non-JS fetch failed for {}: {}", url, e)

    try:
        canonical = extract_canonical(html, url) if html else normalize_url(url)
        page_id = page_id_from_canonical(canonical)
        primary_category, template_name = extract_page_meta(html) if html else (None, None)
        coupons = detect_coupons(html) if html else type("C", (), {"has_coupons": False})()
        affiliates, brand_list = extract_affiliate_brands(html) if html else ([], [])
        brand_positions = None
        if affiliates:
            parts = []
            for ab in affiliates:
                if ab.position:
                    parts.append(f"{ab.brand_slug}:{ab.position}")
            brand_positions = "; ".join(parts) if parts else None

        with get_session() as session:
            # Check if this page has Airtable category/vertical data
            existing = session.execute(
                sa.select(PageSEMInventory).where(PageSEMInventory.page_id == page_id)
            ).scalars().first()
            
            # AIRTABLE ALWAYS WINS: Don't overwrite if Airtable data exists
            final_category = None  # Don't update category if Airtable data exists
            final_vertical = None  # Don't update vertical if Airtable data exists
            
            if existing:
                # If Airtable has data (indicated by having channel/team/brand), don't update category/vertical
                has_airtable_data = any([existing.channel, existing.team, existing.brand])
                if not has_airtable_data:
                    # No Airtable data, safe to use crawler data
                    final_category = primary_category
                    final_vertical = map_vertical(primary_category)
            else:
                # No existing record, use crawler data
                final_category = primary_category
                final_vertical = map_vertical(primary_category)
            
            # Prepare upsert arguments - don't overwrite Airtable category/vertical
            upsert_args = {
                "session": session,
                "page_id": page_id,
                "url": normalize_url(url),
                "canonical_url": canonical,
                "status_code": int(status or 0),
                "template_type": template_name,
                "has_coupons": bool(getattr(coupons, "has_coupons", False)),
                "brand_list": brand_list,
                "brand_positions": brand_positions,
            }
            
            # Only set category/vertical if we determined they should be updated
            if final_category is not None:
                upsert_args["primary_category"] = final_category
            if final_vertical is not None:
                upsert_args["vertical"] = final_vertical
                
            upsert_page(**upsert_args)
            session.commit()
    except Exception as e:
        logger.error("Failed to process {}: {}", url, e)


async def main(seed_csv: Path, sample: int, concurrency: int) -> None:
    urls: list[str] = []
    text = seed_csv.read_text(encoding="utf-8")

    # Try to parse as CSV with header and auto-detect URL column
    buf = io.StringIO(text)
    reader = csv.DictReader(buf)
    fieldnames = [fn or "" for fn in (reader.fieldnames or [])]
    detected_col: str | None = None
    if fieldnames:
        sample_rows = []
        counts: dict[str, int] = {fn: 0 for fn in fieldnames}
        for i, row in enumerate(reader):
            sample_rows.append(row)
            for fn in fieldnames:
                val = (row.get(fn) or "").strip()
                if val.startswith("http://") or val.startswith("https://"):
                    counts[fn] += 1
            if i >= 200:
                break
        # pick best column
        detected_col = max(counts, key=lambda k: counts[k]) if any(counts.values()) else None
        if detected_col:
            # process all rows again using detected column
            buf2 = io.StringIO(text)
            reader2 = csv.DictReader(buf2)
            for row in reader2:
                u = (row.get(detected_col) or "").strip()
                if u:
                    urls.append(u)
    if not urls:
        # fallback: each line is a URL
        for line in text.splitlines():
            u = line.strip()
            if u and not u.lower().startswith("landing page,"):
                urls.append(u)

    if sample and sample > 0:
        urls = urls[:sample]

    tasks = [crawl_url(u) for u in urls]
    # bounded concurrency
    sem = asyncio.Semaphore(max(1, concurrency))

    async def limited(task_coro):
        async with sem:
            return await task_coro

    await asyncio.gather(*(limited(t) for t in tasks), return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ACE-SEM crawler runner")
    parser.add_argument("--seed", default="auto")
    parser.add_argument("--sample", type=int, default=25)
    parser.add_argument("--concurrency", type=int, default=3)
    args = parser.parse_args()

    # Resolve seed path: prefer explicit, otherwise try common locations
    candidates: list[Path] = []
    if args.seed and args.seed != "auto":
        p = Path(args.seed)
        if p.name.startswith("@"):
            p = p.with_name(p.name.lstrip("@"))
        candidates.append(p)
    else:
        candidates.extend(
            [
                Path(__file__).resolve().parents[2] / "sem-pages.csv",  # backend/sem-pages.csv
                Path.cwd() / "sem-pages.csv",  # current dir
                Path(__file__).resolve().parents[1] / "sem-pages.csv",  # app/sem-pages.csv (fallback)
            ]
        )

    seed_path: Path | None = next((c for c in candidates if c.exists()), None)
    if not seed_path:
        raise FileNotFoundError(
            f"Seed CSV not found. Tried: {[str(c) for c in candidates]}. Pass --seed path explicitly."
        )

    asyncio.run(main(seed_path, args.sample, args.concurrency))



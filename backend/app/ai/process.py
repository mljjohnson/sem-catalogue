from __future__ import annotations

import asyncio
from typing import List, Dict, Any

from loguru import logger
from app.crawler.scrape import fetch_html, fetch_screenshot
from app.ai.extract import extract_with_openai
from app.ai.reconcile import reconcile_one
from app.crawler.run import extract_canonical, extract_page_meta
from app.utils.canonical import normalize_url, page_id_from_canonical
from app.utils.mappings import map_vertical
from app.models.db import get_session
import sqlalchemy as sa
from app.models.tables import PageSEMInventory
from app.services.pages import upsert_page, save_ai_extract


async def process_url(url: str, skip_if_exists: bool = True) -> Dict[str, Any]:
    url = url.strip().strip('"').strip("'")
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url

    logger.info("Working on {}", url)
    # Skip if already present by url match
    if skip_if_exists:
        with get_session() as session:
            existing = session.execute(
                sa.select(PageSEMInventory).where(PageSEMInventory.url == normalize_url(url))
            ).scalars().first()
        if existing:
            logger.info("Skipping existing page {}", url)
            return {"url": url, "skipped": True}
    logger.info("Getting HTML for {}", url)
    html_status, html = await fetch_html(url, render_js=True)
    if html_status >= 400 or len(html or "") < 1000:
        html_status2, html2 = await fetch_html(url, render_js=False)
        if html_status2 < 400 and len(html2 or "") > len(html or ""):
            html_status, html = html_status2, html2
    logger.info("HTML fetched for {} status={} bytes={}", url, html_status, len(html or ""))

    logger.info("Getting screenshot for {}", url)
    screenshot = b""
    try:
        screenshot = await fetch_screenshot(url, render_js=True)
    except Exception:
        screenshot = b""
    logger.info("Screenshot {} for {} bytes={}", "succeeded" if screenshot else "failed", url, len(screenshot or b""))

    logger.info("Sending {} to OpenAI", url)
    try:
        data = await extract_with_openai(url, html or "", screenshot or None)
    except Exception as e:
        logger.error("OpenAI extraction failed for {}: {}", url, e)
        # Do NOT write anything to the database if extraction fails
        raise
    logger.info("Response received for {}", url)
    merged = reconcile_one(data if isinstance(data, dict) else {})
    logger.info("Merged signals for {} has_promotions={} brands={} products={}", url, merged.get("has_promotions"), len(merged.get("brand_list", [])), len(merged.get("product_list", [])))

    canonical = extract_canonical(html or "", url)
    page_id = page_id_from_canonical(canonical)
    primary_category, template = extract_page_meta(html or "")

    with get_session() as session:
        # save raw extraction
        save_ai_extract(
            session,
            page_id=page_id,
            url=normalize_url(url),
            html_bytes=len(html or ""),
            screenshot_bytes=len(screenshot or b""),
            data=data if isinstance(data, dict) else {"raw": str(data)},
        )

        # upsert page with merged flags/brands
        upsert_page(
            session,
            page_id=page_id,
            url=normalize_url(url),
            canonical_url=canonical,
            status_code=int(html_status or 0),
            primary_category=primary_category,
            vertical=map_vertical(primary_category),
            template_type=template,
            has_coupons=bool(data.get("has_coupons")) if isinstance(data, dict) else False,
            has_promotions=bool(merged.get("has_promotions")),
            brand_list=merged.get("brand_list", []),
            brand_positions=merged.get("brand_positions"),
            product_list=merged.get("product_list", []),
            product_positions=merged.get("product_positions"),
        )
        session.commit()
    logger.info("Upserted {} has_promotions={} brands={}", url, bool(merged.get("has_promotions")), len(merged.get("brand_list", [])))

    return {
        "url": url,
        "page_id": page_id,
        "has_promotions": bool(merged.get("has_promotions")),
        "brands": merged.get("brand_list", []),
    }



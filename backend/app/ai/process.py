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


async def process_url(url: str, skip_if_exists: bool = True, record_id: int = None) -> Dict[str, Any]:
    # Parse record_id from URL if it's in format "id|url"
    if "|" in url:
        parts = url.split("|", 1)
        record_id = int(parts[0])
        url = parts[1]
    
    url = url.strip().strip('"').strip("'")
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url

    logger.info("Working on {} (record_id={})", url, record_id)
    # Skip if already present by url match (only if no specific record_id)
    if skip_if_exists and not record_id:
        with get_session() as session:
            existing = session.execute(
                sa.select(PageSEMInventory).where(PageSEMInventory.url == normalize_url(url))
            ).scalars().first()
        if existing:
            logger.info("Skipping existing page {}", url)
            return {"url": url, "skipped": True}
    logger.info("Getting HTML for {}", url)
    html_status, html, resolved_url = await fetch_html(url, render_js=True)
    if html_status >= 400 or len(html or "") < 1000:
        html_status2, html2, resolved_url2 = await fetch_html(url, render_js=False)
        if html_status2 < 400 and len(html2 or "") > len(html or ""):
            html_status, html, resolved_url = html_status2, html2, resolved_url2
    logger.info("HTML fetched for {} status={} resolved={} bytes={}", url, html_status, resolved_url or url, len(html or ""))

    # If 301/302 redirect, upsert status and exit early (no screenshot/LLM)
    if int(html_status or 0) in [301, 302, 303, 307, 308]:
        canonical = resolved_url or normalize_url(url)
        page_id = page_id_from_canonical(canonical)
        _, template = extract_page_meta(html or "")
        with get_session() as session:
            upsert_page(
                session,
                page_id=page_id,
                url=normalize_url(url),
                canonical_url=canonical,
                status_code=html_status,
                # Don't set category/vertical - Airtable owns that data
                template_type=template,
                has_coupons=False,
                has_promotions=False,
                brand_list=[],
                brand_positions=None,
                product_list=[],
                product_positions=None,
                record_id=record_id,
            )
            session.commit()
        logger.info("Upserted {} redirect for {} -> {}", html_status, url, canonical)
        return {"url": url, "status": html_status, "skipped": True, "redirect": canonical}

    # If 404, upsert status and exit early (no screenshot/LLM)
    if int(html_status or 0) == 404:
        # Use resolved URL from ScrapingBee, or extract canonical from HTML, or fall back to original URL
        canonical = resolved_url or extract_canonical(html or "", url) or normalize_url(url)
        page_id = page_id_from_canonical(canonical)
        _, template = extract_page_meta(html or "")
        with get_session() as session:
            upsert_page(
                session,
                page_id=page_id,
                url=normalize_url(url),
                canonical_url=canonical,
                status_code=404,
                # Don't set category/vertical - Airtable owns that data
                template_type=template,
                has_coupons=False,
                has_promotions=False,
                brand_list=[],
                brand_positions=None,
                product_list=[],
                product_positions=None,
                record_id=record_id,
            )
            session.commit()
        logger.info("Upserted 404 for {}", url)
        return {"url": url, "status": 404, "skipped": True}

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

    # Determine canonical URL:
    # 1. For redirects (301/302): Use resolved_url from ScrapingBee
    # 2. For 200s: Extract canonical from HTML <link rel="canonical"> tag
    # 3. Fallback: Use the original URL
    if html_status in [301, 302, 303, 307, 308] and resolved_url:
        # Redirect - use the resolved URL
        canonical = resolved_url
    else:
        # 200 or other - check HTML for canonical tag first, then use resolved_url if different
        canonical = extract_canonical(html or "", url)
        if not canonical and resolved_url and resolved_url != url:
            # No canonical tag but ScrapingBee resolved to different URL
            canonical = resolved_url
        if not canonical:
            canonical = normalize_url(url)
    
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

        # IMPORTANT: Category/vertical are ONLY set by Airtable sync
        # The cataloguer NEVER updates these fields - Airtable is the source of truth
        # If a URL is not in Airtable (airtable_id = NULL), category/vertical stay NULL
        
        # Prepare upsert arguments - NEVER include category/vertical
        upsert_args = {
            "session": session,
            "page_id": page_id,
            "url": normalize_url(url),
            "canonical_url": canonical,
            "status_code": int(html_status or 0),
            "template_type": template,
            "has_coupons": bool(data.get("has_coupons")) if isinstance(data, dict) else False,
            "has_promotions": bool(merged.get("has_promotions")),
            "brand_list": merged.get("brand_list", []),
            "brand_positions": merged.get("brand_positions"),
            "product_list": merged.get("product_list", []),
            "product_positions": merged.get("product_positions"),
        }
            
        # upsert page with merged flags/brands
        upsert_page(**upsert_args, record_id=record_id)
        session.commit()
    logger.info("Upserted {} has_promotions={} brands={}", url, bool(merged.get("has_promotions")), len(merged.get("brand_list", [])))

    return {
        "url": url,
        "page_id": page_id,
        "has_promotions": bool(merged.get("has_promotions")),
        "brands": merged.get("brand_list", []),
    }



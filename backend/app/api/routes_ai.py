from typing import Optional

from fastapi import APIRouter, Body
from sqlalchemy import select

from app.crawler.scrape import fetch_html, fetch_screenshot
from app.ai.extract import extract_with_openai
from app.ai.process import process_url
from app.models.db import get_session
from app.models.tables import PageAIExtract


router = APIRouter(prefix="/ai")


@router.post("/extract")
async def ai_extract(url: Optional[str] = Body(default=None), html: Optional[str] = Body(default=None)):
    if not html:
        if not url:
            return {"error": "Provide url or html"}
        html_status, html = await fetch_html(url, render_js=True)
        if html_status >= 400 or not html:
            html_status, html2 = await fetch_html(url, render_js=False)
            if html2:
                html = html2
    screenshot = None
    if url:
        try:
            screenshot = await fetch_screenshot(url, render_js=True)
        except Exception:
            screenshot = None
    data = await extract_with_openai(url or "", html or "", screenshot)
    return {
        "meta": {
            "url": url,
            "html_success": bool(html),
            "screenshot_success": bool(screenshot),
            "html_bytes": len(html or ""),
            "screenshot_bytes": len(screenshot or b"") if screenshot else 0,
        },
        "data": data,
    }


@router.post("/process")
async def ai_process(urls: list[str] = Body(default=[])):
    if not urls:
        return {"error": "Provide at least one URL"}
    results = []
    for u in urls:
        try:
            res = await process_url(u)
            results.append({"url": u, "ok": True, "result": res})
        except Exception as e:
            results.append({"url": u, "ok": False, "error": str(e)})
    return {"results": results}


@router.get("/extracts/{page_id}")
async def get_latest_extract(page_id: str):
    with get_session() as session:
        latest = (
            session.execute(
                select(PageAIExtract).where(PageAIExtract.page_id == page_id).order_by(PageAIExtract.id.desc()).limit(1)
            ).scalars().first()
        )
        if not latest:
            return {"error": "No extract found"}
        return {
            "page_id": latest.page_id,
            "url": latest.url,
            "created_at": latest.created_at,
            "html_bytes": latest.html_bytes,
            "screenshot_bytes": latest.screenshot_bytes,
            "data": latest.data,
        }



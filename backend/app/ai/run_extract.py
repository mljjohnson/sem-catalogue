import argparse
import json
from pathlib import Path

from app.crawler.scrape import fetch_html, fetch_screenshot
from app.ai.extract import extract_with_openai
from app.crawler.run import extract_canonical
from app.utils.canonical import page_id_from_canonical, normalize_url
from app.models.db import get_session
from app.services.pages import save_ai_extract


async def main(url: str, out: Path | None) -> None:
    html_status, html = await fetch_html(url, render_js=True)
    html_ok = html_status < 400 and len(html or "") >= 1000
    if not html_ok:
        html_status2, html2 = await fetch_html(url, render_js=False)
        if html_status2 < 400 and len(html2 or "") > len(html or ""):
            html_status, html = html_status2, html2
            html_ok = True

    screenshot = b""
    shot_ok = False
    try:
        screenshot = await fetch_screenshot(url, render_js=True)
        shot_ok = bool(screenshot and len(screenshot) > 100)
    except Exception:
        shot_ok = False

    data = await extract_with_openai(url, html or "", screenshot if shot_ok else None)
    result = {
        "meta": {
            "url": url,
            "html_status": html_status,
            "html_bytes": len(html or ""),
            "html_success": html_ok,
            "screenshot_bytes": len(screenshot or b"") if shot_ok else 0,
            "screenshot_success": shot_ok,
        },
        "data": data,
    }
    # Persist selectors JSON per page
    try:
        canonical = extract_canonical(html or "", url)
        page_id = page_id_from_canonical(canonical)
        with get_session() as session:
            save_ai_extract(
                session,
                page_id=page_id,
                url=normalize_url(url),
                html_bytes=len(html or ""),
                screenshot_bytes=len(screenshot or b"") if shot_ok else 0,
                data=data,
            )
            session.commit()
    except Exception:
        pass
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
    print(f"URL: {url}")
    print(f"HTML: status={html_status} bytes={len(html or '')} success={html_ok}")
    print(f"Screenshot: bytes={(len(screenshot) if screenshot else 0)} success={shot_ok}")
    print(text)


if __name__ == "__main__":
    import asyncio

    p = argparse.ArgumentParser(description="Run OpenAI extractor for a URL")
    p.add_argument("--url", required=True)
    p.add_argument("--out", default=None)
    args = p.parse_args()
    asyncio.run(main(args.url, Path(args.out) if args.out else None))



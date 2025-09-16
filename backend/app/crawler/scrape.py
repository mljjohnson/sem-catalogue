from typing import Optional

import httpx
from loguru import logger
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.core.config import settings


SCRAPINGBEE_ENDPOINT = "https://app.scrapingbee.com/api/v1/"


async def fetch_html(
    url: str,
    render_js: bool = True,
    country_code: Optional[str] = None,
    timeout_s: int = 15,
) -> tuple[int, str]:
    if not settings.scrapingbee_api_key:
        raise RuntimeError("SCRAPINGBEE_API_KEY not configured")

    params = {
        "api_key": settings.scrapingbee_api_key,
        "url": url,
        "render_js": "true" if render_js else "false",
        # reduce bandwidth and flakiness
        "block_resources": "true",
        # give JS a brief time to settle without blowing our timeout
        "wait": "2000",
        # ScrapingBee expects milliseconds for timeout
        "timeout": str(int(timeout_s * 1000)),
    }
    if country_code:
        params["country_code"] = country_code

    timeout = httpx.Timeout(connect=5.0, read=timeout_s, write=10.0, pool=None)

    async with httpx.AsyncClient(timeout=timeout) as client:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type((httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError)),
            reraise=True,
        ):
            with attempt:
                resp = await client.get(SCRAPINGBEE_ENDPOINT, params=params)
                status = resp.status_code
                text = resp.text if resp.text else ""
                if status >= 400:
                    snippet = text[:240].replace("\n", " ")
                    logger.warning(
                        "ScrapingBee 4xx for {} (render_js={}): {}",
                        url,
                        render_js,
                        snippet,
                    )
                else:
                    logger.debug("Fetched {} status {} bytes {}", url, status, len(text))
                return status, text


async def fetch_screenshot(
    url: str,
    render_js: bool = True,
    width: int = 1280,
    height: int = 2000,
    full_page: bool = True,
    timeout_s: int = 15,
) -> bytes:
    if not settings.scrapingbee_api_key:
        raise RuntimeError("SCRAPINGBEE_API_KEY not configured")

    params = {
        "api_key": settings.scrapingbee_api_key,
        "url": url,
        "render_js": "true",
        "screenshot": "true",
        "screenshot_full_page": "true",
        "block_resources": "true",
        "wait": "2000",
        "timeout": str(int(timeout_s * 1000)),
        "window_width": str(width),
        "window_height": str(height),
    }

    timeout = httpx.Timeout(connect=5.0, read=timeout_s, write=10.0, pool=None)
    async with httpx.AsyncClient(timeout=timeout) as client:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=1, max=20),
            retry=retry_if_exception_type((httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError)),
            reraise=True,
        ):
            with attempt:
                resp = await client.get(SCRAPINGBEE_ENDPOINT, params=params)
                if resp.status_code >= 400:
                    logger.warning("Screenshot 4xx for {}: {}", url, (resp.text or "")[:180].replace("\n", " "))
                    return b""
                ctype = resp.headers.get("content-type", "")
                if not ctype.startswith("image/"):
                    logger.warning("Screenshot non-image content-type={} for {}", ctype, url)
                    return b""
                return resp.content or b""




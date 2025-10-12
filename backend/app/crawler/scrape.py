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
) -> tuple[int, str, Optional[str]]:
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
        # Return origin status/body instead of collapsing to 500
        "transparent_status_code": "true",
    }
    if country_code:
        params["country_code"] = country_code

    timeout = httpx.Timeout(connect=5.0, read=timeout_s, write=10.0, pool=None)

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
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
                
                # Get initial status code (before redirects) and resolved URL
                initial_status = resp.headers.get("Spb-Initial-Status-Code")
                resolved_url = resp.headers.get("Spb-Resolved-Url")
                origin = resp.headers.get("X-Scrapingbee-Status") or resp.headers.get("X-Scrapingbee-Status-Code")
                
                origin_code = None
                try:
                    origin_code = int(origin) if origin is not None else None
                except Exception:
                    origin_code = None
                
                # Use initial status code if available (this is the actual status before redirects)
                if initial_status:
                    try:
                        origin_code = int(initial_status)
                    except Exception:
                        pass

                mode = "JS" if render_js else "HTML"
                via = "standard"

                # If non-success, retry once with premium proxy enabled
                # BUT skip retry for 404s - those won't be fixed by premium proxy
                if status >= 400 or (origin_code is not None and origin_code >= 400):
                    reason = (text[:140] or "").replace("\n", " ")
                    logger.warning(
                        "Fetch FAIL url={} mode={} via=standard http={} origin={} reason={}",
                        url,
                        mode,
                        status,
                        origin_code,
                        reason,
                    )
                    
                    # Don't retry 404s - page doesn't exist
                    if status != 404 and origin_code != 404:
                        # Premium proxy fallback (single attempt)
                        params_pp = dict(params)
                        params_pp["premium_proxy"] = "true"
                        logger.info("Fetch RETRY (premium_proxy) url={} mode={}", url, mode)
                        resp = await client.get(SCRAPINGBEE_ENDPOINT, params=params_pp)
                        via = "premium"
                        status = resp.status_code
                        text = resp.text if resp.text else ""
                        origin = resp.headers.get("X-Scrapingbee-Status") or resp.headers.get("X-Scrapingbee-Status-Code")
                        try:
                            origin_code = int(origin) if origin is not None else None
                        except Exception:
                            origin_code = None
                    else:
                        logger.info("Skipping retry for 404 - page not found")

                if status >= 400 or (origin_code is not None and origin_code >= 400):
                    reason = (text[:140] or "").replace("\n", " ")
                    logger.error(
                        "Fetch FAIL url={} mode={} via={} http={} origin={} reason={}",
                        url,
                        mode,
                        via,
                        status,
                        origin_code,
                        reason,
                    )
                else:
                    logger.info(
                        "Fetch OK url={} mode={} via={} http={} origin={} resolved={} bytes={}",
                        url,
                        mode,
                        via,
                        status,
                        origin_code,
                        resolved_url or url,
                        len(text),
                    )
                return origin_code or status, text, resolved_url
        except (httpx.TimeoutException, httpx.HTTPError) as e:
            logger.error("Fetch ERROR url={} mode={} via=standard exception={}", url, "JS" if render_js else "HTML", repr(e))
            return 599, "", None


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




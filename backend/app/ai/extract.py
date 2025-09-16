from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict

from bs4 import BeautifulSoup
from openai import AsyncOpenAI
import base64

from app.core.config import settings


def clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    # Drop scripts/styles/nav/header/footer
    for selector in ["script", "style", "noscript", "header", "footer", "nav", "iframe"]:
        for node in soup.select(selector):
            node.decompose()
    # Attribute selectors (no leading dot!)
    for selector in [
        "[class*='header']",
        "[class*='footer']",
        "[role='navigation']",
        "[class*='cookie']",
        "[id*='header']",
        "[id*='footer']",
    ]:
        for node in soup.select(selector):
            node.decompose()
    text = str(soup)
    # collapse whitespace
    text = re.sub(r"\s+", " ", text)
    return text


def build_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "url": {"type": "string"},
            "primary_category": {"type": "string"},
            "template_type": {"type": "string"},
            "page_type": {"type": "string", "enum": ["listing", "single_product"]},
            "has_coupons": {"type": "boolean"},
            "has_promotions": {"type": "boolean"},
            "listings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "selector": {"type": "string"},
                        "description": {"type": "string"},
                        "code": {"type": "string"},
                        "affiliate_link": {"type": "string"},
                        "brand_name": {"type": "string"},
                        "product_name": {"type": "string"},
                        "product_offer_name": {"type": "string"},
                        "position": {"type": "string"},
                        "location": {"type": "string", "enum": ["main_list", "other"]},
                        "container_type": {"type": "string", "enum": ["main_list", "sidebar", "banner", "inline", "other"]},
                        "container_selector": {"type": "string"},
                        "has_promotion": {"type": "boolean"}
                    },
                    "required": [
                        "selector",
                        "description",
                        "code",
                        "affiliate_link",
                        "brand_name",
                        "product_name",
                        "product_offer_name",
                        "position",
                        "location",
                        "container_type",
                        "container_selector",
                        "has_promotion"
                    ]
                }
            },
            "other_promotions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "description": {"type": "string"},
                        "code": {"type": "string"},
                        "affiliate_link": {"type": "string"}
                    },
                    "required": ["description", "code", "affiliate_link"]
                }
            }
        },
        "required": [
            "url",
            "primary_category",
            "template_type",
            "page_type",
            "has_coupons",
            "has_promotions",
            "listings",
            "other_promotions"
        ]
    }


def build_prompt(url: str) -> str:
    return (
        "You are extracting structured selectors and signals from an SEM page.\n"
        "Output must strictly match the provided JSON Schema. Do not add extra keys.\n"
        "\n"
        "Page classification:\n"
        "- Set page_type='listing' when the page presents multiple distinct brand/product entries.\n"
        "- Set page_type='single_product' when the page focuses on one product/brand (e.g., a review or dedicated product page).\n"
        "\n"
        "Listings (only for listing pages):\n"
        "- For page_type='listing', identify the primary ranked list/grid. For EACH listing card/row, fill fields as follows:\n"
        "  • selector: CSS for the smallest container wrapping the entire listing card/row. Prefer stable id/class/data-*; avoid brittle selectors; only use :nth-of-type when unavoidable.\n"
        "  • description: One concise sentence describing ONLY the deal/offer/promotion (percent/currency off, free months, intro pricing, free gift/credit). Include numbers/units. If there is no promotion for this listing, set description to ''. Do not include general features/specs.\n"
        "  • code: Visible coupon/promo code text (e.g., SAVE20). Extract from phrases like 'Use code SAVE20' or 'Copy code'. If hidden behind 'Reveal code' or absent, set ''. Never invent.\n"
        "  • affiliate_link: Outbound URL used by the primary CTA/button in the listing. Prefer external brand domain or known affiliate networks (impact.com, cj.com, awin.com, partnerize.com). If only internal anchors or none, set ''. Must be absolute URL or ''.\n"
        "  • brand_name: The product/provider brand shown to users on the page (not affiliate networks). Derive from logo alt, data-brand, nearby heading, or the destination domain after excluding affiliate network hosts (impact.com, cj.com, awin.com, partnerize.com, etc.). No plan names.\n"
        "  • product_name: ONLY the product/plan name; do NOT include deal/offer words (exclude 'free', '% off', 'sale', '$X for Y', etc.). If there is no distinct product name, or the brand is the product, set product_name = brand_name.\n"
        "  • position: If in the main ranked list, return P1, P2, … by visual order; else ''.\n"
        "  • location: 'main_list' if part of primary ranked list; else 'other'.\n"
        "  • container_type: main_list | sidebar | banner | inline | other based on the surrounding area (aside=sidebar, hero=banner).\n"
        "  • container_selector: CSS selector for the section containing the listing (list wrapper, sidebar block, hero). Prefer stable ids/classes.\n"
        "  • has_promotion: true if description has discount/offer language OR code != ''; else false.\n"
        "\n"
        "Critical rule for single-product pages:\n"
        "- For page_type='single_product', do NOT add any objects to listings (leave listings=[]).\n"
        "- Instead, put all detected promotions/coupons/intro offers in other_promotions using the same rules for description, code, and affiliate_link.\n"
        "- Ensure any product/prose references still include the brand in brand_name when relevant; when unclear, use the page's primary brand as both brand_name and product_name.\n"
        "\n"
        "Other promotions (not in the main list):\n"
        "- Always scan for hero/inline banners, sidebar offers, sitewide bars. For each, add description, code ('' if none), and affiliate_link (or ''). Use screenshot and HTML cues to avoid missing banners/sidebars.\n"
        "\n"
        "Promotion definition (STRICT):\n"
        "- Include ONLY items that confer direct economic value to the user: explicit discounts (percent or currency off), free/bonus periods (e.g., '3 months free'), free gifts/credits, introductory/limited-time pricing ('X for $Y for first Z months'), or benefits unlocked by a coupon code.\n"
        "- EXCLUDE general features/specs/benefits that do not represent a discount or free value. Do NOT treat statements like 'Rechargeable model gives you added juice for on-the-go lifestyles', product specs, feature bullets, shipping speed, warranties, accessibility, or device capabilities as promotions.\n"
        "- If uncertain whether a text is a promotion or a generic feature, EXCLUDE it.\n"
        "\n"
        "Metadata and flags:\n"
        "- Extract primary_category and template_type from explicit page data if present (e.g., dataLayer/pageLevelData).\n"
        "- Set has_promotions=true if any listing has has_promotion=true OR other_promotions length > 0.\n"
        "\n"
        "Strictness:\n"
        "- Do not hallucinate. Use '' for missing strings. Keep strings short and trimmed. No HTML/Markdown.\n"
        "- Always return listings and other_promotions arrays; they may be empty.\n"
        "- Normalize URLs by dropping fragments when obvious; keep query strings.\n"
        f"Page URL: {url}\n"
    )


async def extract_with_openai(url: str, html: str, screenshot_bytes: bytes | None = None) -> Dict[str, Any]:
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY not configured")
    client = AsyncOpenAI(api_key=settings.openai_api_key)

    content_clean = clean_html(html)
    schema = build_schema()
    system_msg = "You are a meticulous extraction engine that outputs strict JSON conforming to the provided schema."
    user_msg = build_prompt(url)

    # Build sidebar/banner/brand hints to steer the model
    def build_hints(raw_html: str) -> str:
        soup = BeautifulSoup(raw_html, "lxml")
        hints: list[str] = []
        # potential promo containers
        containers = soup.select(
            "aside, [class*='sidebar'], [id*='sidebar'], [class*='banner'], [id*='banner'], [class*='promo'], [class*='deal'], [class*='offer']"
        )
        for idx, el in enumerate(containers[:20], start=1):
            cls = " ".join(el.get("class") or [])
            idv = el.get("id") or ""
            hints.append(f"container#{idx}: tag={el.name} id={idv} class={cls}")
        # brand candidates from img alt and anchor text
        for img in soup.find_all("img")[:50]:
            alt = (img.get("alt") or "").strip()
            if alt and 2 <= len(alt) <= 80:
                hints.append(f"brand_candidate_img_alt: {alt}")
        for a in soup.find_all("a")[:100]:
            txt = (a.get_text(" ", strip=True) or "").strip()
            if txt and 2 <= len(txt) <= 80:
                hints.append(f"brand_candidate_anchor: {txt}")
        return "\n".join(hints[:200])

    # Build multimodal input for Responses API
    content_parts: list[dict] = [
        {"type": "input_text", "text": "Schema (JSON Schema):\n" + json.dumps(schema)},
        {"type": "input_text", "text": user_msg},
        {"type": "input_text", "text": content_clean},
        {"type": "input_text", "text": "Hints (containers and brand candidates):\n" + build_hints(html)},
    ]
    if screenshot_bytes:
        b64 = base64.b64encode(screenshot_bytes).decode("ascii")
        content_parts.append({
            "type": "input_image",
            "image_url": f"data:image/png;base64,{b64}",
            "detail": "high",
        })

    resp = await client.responses.create(
        model=settings.openai_model,
        input=[
            {"role": "system", "content": [{"type": "input_text", "text": system_msg}]},
            {"role": "user", "content": content_parts},
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "ace_sem_extract",
                "schema": schema,
            }
        },
    )
    # recent SDKs expose output_text
    text = getattr(resp, "output_text", None) or (resp.output[0].content[0].text if getattr(resp, "output", None) else "{}")
    try:
        data = json.loads(text)
    except Exception:
        data = {"raw": text}
    return data



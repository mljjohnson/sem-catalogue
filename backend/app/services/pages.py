from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, func, select, or_
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from app.models.tables import PageSEMInventory, PageAIExtract
from datetime import datetime


def upsert_page(
    session: Session,
    *,
    page_id: str,
    url: str,
    canonical_url: str,
    status_code: int,
    primary_category: Optional[str],
    vertical: Optional[str],
    template_type: Optional[str],
    has_coupons: bool,
    has_promotions: bool = False,
    brand_list: Optional[List[str]] = None,
    brand_positions: Optional[str] = None,
    product_list: Optional[List[str]] = None,
    product_positions: Optional[str] = None,
) -> None:
    today = date.today()
    brand_list = brand_list or []
    product_list = product_list or []
    # Build base values
    values = dict(
        page_id=page_id,
        url=url,
        canonical_url=canonical_url,
        status_code=status_code,
        primary_category=primary_category,
        vertical=vertical,
        template_type=template_type,
        has_coupons=has_coupons,
        has_promotions=has_promotions,
        brand_list=brand_list,
        brand_positions=brand_positions,
        product_list=product_list,
        product_positions=product_positions,
        first_seen=today,
        last_seen=today,
    )
    update_cols = {
        "url": url,
        "canonical_url": canonical_url,
        "status_code": status_code,
        "primary_category": primary_category,
        "vertical": vertical,
        "template_type": template_type,
        "has_coupons": has_coupons,
        "has_promotions": has_promotions,
        "brand_list": brand_list,
        "brand_positions": brand_positions,
        "product_list": product_list,
        "product_positions": product_positions,
        "last_seen": today,
    }
    dialect = session.bind.dialect.name  # type: ignore[attr-defined]
    if dialect == "sqlite":
        stmt = sqlite_insert(PageSEMInventory).values(values)
        onconf = stmt.on_conflict_do_update(
            index_elements=[PageSEMInventory.page_id],
            set_=update_cols,
        )
        session.execute(onconf)
    else:
        stmt = mysql_insert(PageSEMInventory).values(values)
        ondup = stmt.on_duplicate_key_update(**update_cols)
        session.execute(ondup)


def query_pages(
    session: Session,
    *,
    coupons: Optional[bool] = None,
    promotions: Optional[bool] = None,
    brands: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    primary_category: Optional[str] = None,
    vertical: Optional[str] = None,
    template_type: Optional[str] = None,
    status: Optional[int] = None,
    search: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    sort: Optional[str] = "last_seen:desc",
) -> Tuple[List[Dict[str, Any]], int]:
    q = select(PageSEMInventory)
    dialect = session.bind.dialect.name  # type: ignore[attr-defined]

    if coupons is not None:
        q = q.where(PageSEMInventory.has_coupons == coupons)
    if promotions is not None:
        q = q.where(PageSEMInventory.has_promotions == promotions)
    if brands:
        if dialect == "sqlite":
            for b in brands:
                q = q.where(PageSEMInventory.brand_list.like(f'%"{b}"%'))  # type: ignore
        else:
            for b in brands:
                q = q.where(func.json_search(PageSEMInventory.brand_list, "one", b) != None)  # type: ignore
    if products:
        if dialect == "sqlite":
            for p in products:
                q = q.where(PageSEMInventory.product_list.like(f'%"{p}"%'))  # type: ignore
        else:
            for p in products:
                q = q.where(func.json_search(PageSEMInventory.product_list, "one", p) != None)  # type: ignore
    if primary_category:
        q = q.where(PageSEMInventory.primary_category == primary_category)
    if vertical:
        q = q.where(PageSEMInventory.vertical == vertical)
    if template_type:
        q = q.where(PageSEMInventory.template_type == template_type)
    if status is not None:
        q = q.where(PageSEMInventory.status_code == status)
    if search:
        like = f"%{search}%"
        q = q.where(or_(PageSEMInventory.url.like(like), PageSEMInventory.canonical_url.like(like)))  # type: ignore

    total = session.execute(select(func.count()).select_from(q.subquery())).scalar() or 0

    if sort:
        col, _, direction = sort.partition(":")
        direction = (direction or "asc").lower()
        sort_col = getattr(PageSEMInventory, col, PageSEMInventory.last_seen)
        if direction == "desc":
            q = q.order_by(sort_col.desc())
        else:
            q = q.order_by(sort_col.asc())

    q = q.limit(limit).offset(offset)
    rows = session.execute(q).scalars().all()

    # Attach latest page_type from AI extracts (simple per-row lookup; acceptable for current sizes)
    items = []
    for r in rows:
        page_type = None
        try:
            latest = session.execute(
                select(PageAIExtract)
                .where(PageAIExtract.page_id == r.page_id)
                .order_by(PageAIExtract.id.desc())
                .limit(1)
            ).scalars().first()
            if latest and isinstance(latest.data, dict):
                pt = latest.data.get("page_type")
                if isinstance(pt, str):
                    page_type = pt
        except Exception:
            page_type = None
        items.append(
            {
                "page_id": r.page_id,
                "url": r.url,
                "canonical_url": r.canonical_url,
                "status_code": r.status_code,
                "primary_category": r.primary_category,
                "vertical": r.vertical,
                "template_type": r.template_type,
                "has_coupons": r.has_coupons,
                "has_promotions": getattr(r, "has_promotions", None),
                "brand_list": r.brand_list or [],
                "brand_positions": r.brand_positions,
                "product_list": getattr(r, "product_list", []) or [],
                "product_positions": getattr(r, "product_positions", None),
                "first_seen": r.first_seen.isoformat() if r.first_seen else None,
                "last_seen": r.last_seen.isoformat() if r.last_seen else None,
                "ga_sessions_14d": r.ga_sessions_14d,
                "ga_key_events_14d": r.ga_key_events_14d,
                "page_type": page_type,
            }
        )
    return items, int(total)


def save_ai_extract(
    session: Session,
    *,
    page_id: str,
    url: str,
    html_bytes: int,
    screenshot_bytes: int,
    data: Dict[str, Any],
) -> None:
    record = PageAIExtract(
        page_id=page_id,
        url=url,
        created_at=datetime.utcnow().isoformat(),
        html_bytes=html_bytes,
        screenshot_bytes=screenshot_bytes,
        data=data,
    )
    session.add(record)



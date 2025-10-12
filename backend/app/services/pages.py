from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, func, select, or_
import sqlalchemy as sa
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
    primary_category: Optional[str] = None,
    vertical: Optional[str] = None,
    template_type: Optional[str],
    has_coupons: bool,
    has_promotions: bool = False,
    brand_list: Optional[List[str]] = None,
    brand_positions: Optional[str] = None,
    product_list: Optional[List[str]] = None,
    product_positions: Optional[str] = None,
    # Airtable sync fields
    airtable_id: Optional[str] = None,
    channel: Optional[str] = None,
    team: Optional[str] = None,
    brand: Optional[str] = None,
    page_status: Optional[str] = None,
    # Cataloguing status
    catalogued: Optional[int] = None,
    # Specific record ID to update
    record_id: Optional[int] = None,
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
        # Airtable fields
        airtable_id=airtable_id,
        channel=channel,
        team=team,
        brand=brand,
        page_status=page_status,
        # Cataloguing status (auto-set based on status_code if not provided)
        catalogued=catalogued if catalogued is not None else (1 if status_code != 0 else 0),
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
        # Airtable fields
        "airtable_id": airtable_id,
        "channel": channel,
        "team": team,
        "brand": brand,
        "page_status": page_status,
        # Cataloguing status (auto-set based on status_code if not provided)
        "catalogued": catalogued if catalogued is not None else (1 if status_code != 0 else 0),
    }
    # If we have a specific record_id, update that exact record
    if record_id:
        existing = session.execute(
            sa.select(PageSEMInventory).where(PageSEMInventory.id == record_id)
        ).scalars().first()
        
        if existing:
            # UPDATE the specific record
            for key, val in update_cols.items():
                setattr(existing, key, val)
        else:
            # Record not found - this shouldn't happen but insert anyway
            new_record = PageSEMInventory(**values)
            session.add(new_record)
    else:
        # No specific record_id - find the LATEST uncatalogued record for this page_id
        existing = session.execute(
            sa.select(PageSEMInventory)
            .where(PageSEMInventory.page_id == page_id)
            .where(PageSEMInventory.catalogued == 0)
            .order_by(PageSEMInventory.id.desc())
        ).scalars().first()
        
        if existing:
            # UPDATE the existing uncatalogued record
            for key, val in update_cols.items():
                setattr(existing, key, val)
        else:
            # No uncatalogued record found - check if ANY record exists for this page_id
            any_existing = session.execute(
                sa.select(PageSEMInventory)
                .where(PageSEMInventory.page_id == page_id)
                .order_by(PageSEMInventory.last_seen.desc())
            ).scalars().first()
            
            if any_existing:
                # Record exists but is already catalogued - INSERT new version
                new_record = PageSEMInventory(**values)
                session.add(new_record)
            else:
                # No record exists at all - INSERT new
                new_record = PageSEMInventory(**values)
                session.add(new_record)


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
    publisher: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    sort: Optional[str] = "page_id:asc",
) -> Tuple[List[Dict[str, Any]], int]:
    # IMPORTANT: Only get the latest catalogued version of each URL
    # AND only show URLs that exist in Airtable (airtable_id IS NOT NULL)
    # Using a subquery to get the max last_seen for each page_id
    latest_subq = (
        select(
            PageSEMInventory.page_id,
            func.max(PageSEMInventory.last_seen).label('max_last_seen')
        )
        .where(PageSEMInventory.catalogued == 1)
        .where(PageSEMInventory.airtable_id.isnot(None))
        .group_by(PageSEMInventory.page_id)
        .subquery()
    )
    
    q = select(PageSEMInventory).join(
        latest_subq,
        and_(
            PageSEMInventory.page_id == latest_subq.c.page_id,
            PageSEMInventory.last_seen == latest_subq.c.max_last_seen
        )
    )
    
    dialect = session.bind.dialect.name  # type: ignore[attr-defined]
    
    # Filter out Inactive pages - only show Active pages or pages without status
    q = q.where(or_(PageSEMInventory.page_status.is_(None), PageSEMInventory.page_status != "Inactive"))
    
    # Filter out excluded domains
    excluded_domains = ['usatoday.com', 'gorenewalbyandersen.com', 'carshieldplans.com']
    for domain in excluded_domains:
        q = q.where(~PageSEMInventory.url.like(f'%{domain}%'))

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
    if publisher:
        q = q.where(PageSEMInventory.url.like(f'%{publisher}%'))
    if search:
        like = f"%{search}%"
        q = q.where(or_(PageSEMInventory.url.like(like), PageSEMInventory.canonical_url.like(like)))  # type: ignore

    total = session.execute(select(func.count()).select_from(q.subquery())).scalar() or 0

    if sort:
        col, _, direction = sort.partition(":")
        direction = (direction or "asc").lower()
        sort_col = getattr(PageSEMInventory, col, PageSEMInventory.page_id)
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
                "id": r.id,
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
                "sessions": getattr(r, "sessions", None),
                "ga_key_events_14d": r.ga_key_events_14d,
                "page_type": page_type,
                # Airtable fields
                "airtable_id": getattr(r, "airtable_id", None),
                "channel": getattr(r, "channel", None),
                "team": getattr(r, "team", None),
                "brand": getattr(r, "brand", None),
                "page_status": getattr(r, "page_status", None),
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



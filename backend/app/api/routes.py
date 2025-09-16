from typing import List, Optional

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
import io
import csv

from app.models.db import get_session
from app.models.tables import PageSEMInventory
from sqlalchemy import select
from app.api.routes_ai import router as ai_router
from app.services.pages import query_pages

router = APIRouter()


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}

# Mount AI routes
router.include_router(ai_router)


@router.get("/pages")
def list_pages(
    coupons: Optional[bool] = None,
    promotions: Optional[bool] = None,
    brands: Optional[List[str]] = Query(default=None),
    products: Optional[List[str]] = Query(default=None),
    primary_category: Optional[str] = None,
    vertical: Optional[str] = None,
    template_type: Optional[str] = None,
    status: Optional[int] = 200,
    search: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    sort: Optional[str] = "last_seen:desc",
):
    with get_session() as session:
        items, total = query_pages(
            session,
            coupons=coupons,
            promotions=promotions,
            brands=brands,
            products=products,
            primary_category=primary_category,
            vertical=vertical,
            template_type=template_type,
            status=status,
            search=search,
            limit=limit,
            offset=offset,
            sort=sort,
        )
        return {"items": items, "total": total, "limit": limit, "offset": offset}


@router.get("/pages/export.csv")
def export_pages_csv(
    coupons: Optional[bool] = None,
    brands: Optional[List[str]] = Query(default=None),
    products: Optional[List[str]] = Query(default=None),
    primary_category: Optional[str] = None,
    vertical: Optional[str] = None,
    template_type: Optional[str] = None,
    status: Optional[int] = 200,
    search: Optional[str] = None,
    sort: Optional[str] = "last_seen:desc",
):
    with get_session() as session:
        items, _ = query_pages(
            session,
            coupons=coupons,
            brands=brands,
            products=products,
            primary_category=primary_category,
            vertical=vertical,
            template_type=template_type,
            status=status,
            search=search,
            limit=100000,
            offset=0,
            sort=sort,
        )
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=[
            "url",
            "canonical_url",
            "primary_category",
            "vertical",
            "has_coupons",
            "brand_list",
            "brand_positions",
            "product_list",
            "product_positions",
            "status_code",
            "last_seen",
        ],
    )
    writer.writeheader()
    for it in items:
        writer.writerow(
            {
                "url": it.get("url"),
                "canonical_url": it.get("canonical_url"),
                "primary_category": it.get("primary_category"),
                "vertical": it.get("vertical"),
                "has_coupons": it.get("has_coupons"),
                "brand_list": ",".join(it.get("brand_list", [])),
                "brand_positions": it.get("brand_positions"),
                "product_list": ",".join(it.get("product_list", [])),
                "product_positions": it.get("product_positions"),
                "status_code": it.get("status_code"),
                "last_seen": it.get("last_seen"),
            }
        )
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=pages.csv"})


@router.get("/brands")
def list_brands():
    # Placeholder: will read normalized brands from DB
    return {"brands": []}


@router.get("/facets")
def get_facets():
    with get_session() as session:
        rows = session.execute(select(PageSEMInventory)).scalars().all()
        brands_set: set[str] = set()
        cats_set: set[str] = set()
        verts_set: set[str] = set()
        for r in rows:
            try:
                for b in (r.brand_list or []):
                    if b:
                        brands_set.add(str(b))
            except Exception:
                pass
            if r.primary_category:
                cats_set.add(r.primary_category)
            if r.vertical:
                verts_set.add(r.vertical)
        return {
            "brands": sorted(brands_set),
            "primary_categories": sorted(cats_set),
            "verticals": sorted(verts_set),
        }




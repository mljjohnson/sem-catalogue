import argparse
from typing import Dict, Any

from sqlalchemy import select

from app.models.db import get_session
from app.models.tables import PageAIExtract, PageSEMInventory


def reconcile_one(data: Dict[str, Any]) -> Dict[str, Any]:
    listings = data.get("listings", []) if isinstance(data, dict) else []
    other_promos = data.get("other_promotions", []) if isinstance(data, dict) else []
    brands = data.get("brands", []) if isinstance(data, dict) else []
    has_promotions = any(bool(li.get("has_promotion")) for li in listings) or (len(other_promos) > 0)
    # brand list from brands array fallback to listings brand_name
    brand_names = [b.get("brand_name") for b in brands if b.get("brand_name")]
    if not brand_names:
        brand_names = [li.get("brand_name") for li in listings if li.get("brand_name")]
    brand_names = [b for b in { (b or "").strip(): None for b in brand_names }.keys() if b]
    # brand positions from listings in main_list
    positions = []
    for li in listings:
        bn = (li.get("brand_name") or "").strip()
        pos = (li.get("position") or "").strip()
        loc = (li.get("location") or "other").strip()
        if bn and pos and loc == "main_list":
            positions.append(f"{bn}:{pos}")
    brand_positions = "; ".join(positions) if positions else None

    # products derived from listings
    product_names = []
    for li in listings:
        pname = (li.get("product_name") or li.get("product_offer_name") or "").strip()
        if pname:
            product_names.append(pname)
    product_names = [p for p in { (p or "").strip(): None for p in product_names }.keys() if p]
    product_positions_list = []
    for li in listings:
        pname = (li.get("product_name") or li.get("product_offer_name") or "").strip()
        pos = (li.get("position") or "").strip()
        loc = (li.get("location") or "other").strip()
        if pname and pos and loc == "main_list":
            product_positions_list.append(f"{pname}:{pos}")
    product_positions = "; ".join(product_positions_list) if product_positions_list else None
    return {
        "has_promotions": has_promotions,
        "brand_list": brand_names,
        "brand_positions": brand_positions,
        "product_list": product_names,
        "product_positions": product_positions,
    }


def main() -> None:
    with get_session() as session:
        # latest extract per page_id
        page_ids = [r[0] for r in session.execute(select(PageAIExtract.page_id).distinct())]
        updated = 0
        for pid in page_ids:
            latest = (
                session.execute(
                    select(PageAIExtract).where(PageAIExtract.page_id == pid).order_by(PageAIExtract.id.desc()).limit(1)
                ).scalars().first()
            )
            if not latest:
                continue
            merged = reconcile_one(latest.data or {})
            page = session.execute(select(PageSEMInventory).where(PageSEMInventory.page_id == pid)).scalars().first()
            if not page:
                continue
            page.has_promotions = bool(merged.get("has_promotions", False))
            if merged.get("brand_list"):
                page.brand_list = merged.get("brand_list")
            if merged.get("brand_positions"):
                page.brand_positions = merged.get("brand_positions")
            updated += 1
        session.commit()
        print(f"Reconciled {updated} pages from AI extracts")


if __name__ == "__main__":
    main()



from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Optional

import sqlalchemy as sa
from loguru import logger

from app.models.db import get_session
from app.models.tables import PageSEMInventory
from app.services.pages import upsert_page
from app.utils.canonical import normalize_url


def _safe_int(v: Optional[str]) -> int:
    try:
        if v is None:
            return 0
        v = v.strip()
        return int(v) if v else 0
    except Exception:
        return 0


def ingest(csv_path: Path, update_primary_category: bool = False) -> None:
    rows = list(csv.DictReader(csv_path.open("r", encoding="utf-8")))
    updated, inserted, skipped = 0, 0, 0

    with get_session() as session:
        for r in rows:
            raw_vertical = (r.get("vertical") or "").strip()
            if not raw_vertical:
                skipped += 1
                continue

            page_id = (r.get("page_id") or "").strip()
            url = normalize_url((r.get("url") or "").strip())
            canonical_url = (r.get("canonical_url") or url or "").strip()
            status_code = _safe_int(r.get("status_code"))
            primary_category = (r.get("primary_category") or None)

            # Try to find existing row by page_id, else by url
            existing: Optional[PageSEMInventory] = None
            if page_id:
                existing = (
                    session.execute(
                        sa.select(PageSEMInventory).where(PageSEMInventory.page_id == page_id)
                    ).scalars().first()
                )
            if existing is None and url:
                existing = (
                    session.execute(
                        sa.select(PageSEMInventory).where(PageSEMInventory.url == url)
                    ).scalars().first()
                )

            if existing:
                existing.vertical = raw_vertical
                if update_primary_category and primary_category:
                    existing.primary_category = primary_category
                updated += 1
            else:
                # Insert a minimal row so vertical is captured for future use
                upsert_page(
                    session,
                    page_id=page_id or canonical_url or url,
                    url=url or canonical_url,
                    canonical_url=canonical_url or url,
                    status_code=status_code,
                    primary_category=primary_category,
                    vertical=raw_vertical,
                    template_type=None,
                    has_coupons=False,
                    has_promotions=False,
                    brand_list=[],
                    brand_positions=None,
                    product_list=[],
                    product_positions=None,
                )
                inserted += 1

        session.commit()

    logger.info("Vertical ingest complete: updated={} inserted={} skipped_blank={}", updated, inserted, skipped)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Ingest verticals from CSV into pages_sem_inventory")
    p.add_argument("--csv", default=str(Path(__file__).resolve().parents[2] / "sem-catalogue-verticals.csv"))
    p.add_argument("--update-primary", action="store_true", help="Also update primary_category when provided")
    args = p.parse_args()

    ingest(Path(args.csv), update_primary_category=args.update_primary)



from __future__ import annotations

import argparse
import asyncio
import csv
import io
from pathlib import Path
from typing import Dict, List, Tuple

from loguru import logger
import sqlalchemy as sa
from app.models.db import get_session
from app.models.tables import PageSEMInventory
from app.utils.canonical import normalize_url


def read_seed_list(seed_path: Path) -> List[str]:
    txt = seed_path.read_text(encoding="utf-8")
    buf = io.StringIO(txt)
    try:
        reader = csv.DictReader(buf)
        fns = reader.fieldnames or []
        url_col = None
        if fns:
            # choose the column with most http-like values
            rows = list(reader)
            scores = {fn: 0 for fn in fns}
            for row in rows[:200]:
                for fn in fns:
                    v = (row.get(fn) or "").strip()
                    if v.startswith("http://") or v.startswith("https://"):
                        scores[fn] += 1
            url_col = max(scores, key=lambda k: scores[k]) if any(scores.values()) else None
            if url_col:
                return [
                    (row.get(url_col) or "").strip()
                    for row in rows
                    if (row.get(url_col) or "").strip()
                ]
    except Exception:
        pass
    # fallback: one URL per line
    return [line.strip() for line in txt.splitlines() if line.strip()]


async def main(seed: Path, out_dir: Path, _concurrency: int) -> None:
    urls = [u if u.startswith("http") else f"https://{u}" for u in read_seed_list(seed)]
    norm_urls = [normalize_url(u) for u in urls]

    with get_session() as session:
        rows = (
            session.execute(
                sa.select(PageSEMInventory.url, PageSEMInventory.status_code)
                .where(PageSEMInventory.url.in_(norm_urls))
            ).all()
        )
    in_db: Dict[str, int] = {normalize_url(u): (sc or 0) for (u, sc) in rows}

    # Build unified output set (no network probes):
    # - All URLs missing from DB (blank status_code)
    # - All URLs present in DB but status_code != 200
    out_rows: Dict[str, int | None] = {}
    for u in urls:
        nu = normalize_url(u)
        if nu not in in_db:
            out_rows[nu] = None
    # add DB non-200
    for u, sc in in_db.items():
        if sc != 200:
            out_rows[u] = sc or None

    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / "seed_audit.csv"
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["url", "status_code"])  # status_code may be empty if unknown
        for u in sorted(out_rows.keys()):
            w.writerow([u, out_rows[u] if out_rows[u] is not None else ""])

    logger.info("Wrote {} ({} rows)", p, len(out_rows))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Audit seed CSV vs database and statuses")
    p.add_argument("--seed", default=str(Path(__file__).resolve().parents[2] / "sem-pages.csv"))
    p.add_argument("--out-dir", default=str(Path(__file__).resolve().parents[3] / "data" / "latest"))
    p.add_argument("--concurrency", type=int, default=1)
    args = p.parse_args()
    asyncio.run(main(Path(args.seed), Path(args.out_dir), args.concurrency))



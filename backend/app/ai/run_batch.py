import argparse
import asyncio
import csv
import io
import time
import sys
from collections import deque
from pathlib import Path
from typing import List

from app.ai.process import process_url
from loguru import logger


async def _wrap_process(u: str) -> dict:
    try:
        res = await process_url(u, skip_if_exists=True)
        return {
            "url": res.get("url") or u,
            "page_id": res.get("page_id"),
            "html_status": None,
            "html_bytes": None,
            "screenshot_bytes": None,
            "has_coupons": None,
            "has_promotions": res.get("has_promotions"),
            "listings_count": None,
            "brands_count": len(res.get("brands", [])),
            "skipped": res.get("skipped", False),
            "error": None,
        }
    except Exception as e:
        return {"url": u, "error": str(e), "skipped": False}


def read_seed_list(seed_path: Path, count: int) -> List[str]:
    txt = seed_path.read_text(encoding="utf-8")
    buf = io.StringIO(txt)
    try:
        reader = csv.DictReader(buf)
        fns = reader.fieldnames or []
        url_col = None
        if fns:
            # pick the column with most http-like values
            sample = list(reader)[:200]
            scores = {fn: 0 for fn in fns}
            for row in sample:
                for fn in fns:
                    v = (row.get(fn) or "").strip()
                    if v.startswith("http://") or v.startswith("https://"):
                        scores[fn] += 1
            url_col = max(scores, key=lambda k: scores[k]) if any(scores.values()) else None
            if url_col:
                return [
                    (row.get(url_col) or "").strip()
                    for row in csv.DictReader(io.StringIO(txt))
                    if (row.get(url_col) or "").strip()
                ][:count]
    except Exception:
        pass
    # fallback: one URL per line
    urls = [line.strip() for line in txt.splitlines() if line.strip()]
    return urls[:count]


async def main(seed: Path, count: int, concurrency: int, out_csv: Path, quiet: bool) -> None:
    if quiet:
        try:
            logger.remove()
            logger.add(sys.stderr, level="WARNING")
        except Exception:
            pass
    urls = read_seed_list(seed, count)
    total = len(urls)
    print(f"Starting batch: {total} urls, concurrency={concurrency}")
    start_ts = time.time()

    q: asyncio.Queue[str] = asyncio.Queue()
    for u in urls:
        q.put_nowait(u)

    results: List[dict] = []
    completed = 0
    ok = 0
    skipped = 0
    failed = 0
    lock = asyncio.Lock()
    recent = deque(maxlen=20)

    def _fmt_eta(seconds: float | None) -> str:
        if not seconds or seconds <= 0 or seconds == float('inf'):
            return "--:--:--"
        seconds = int(seconds)
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    async def worker(wid: int):
        nonlocal completed, ok, skipped, failed
        while True:
            try:
                u = await q.get()
            except Exception:
                return
            r = await _wrap_process(u)
            status = "ok"
            if r.get("error"):
                status = "fail"
            elif r.get("skipped"):
                status = "skip"
            async with lock:
                results.append(r)
                completed += 1
                if status == "ok":
                    ok += 1
                    recent.append(False)
                elif status == "skip":
                    skipped += 1
                    recent.append(False)
                else:
                    failed += 1
                    recent.append(True)
                elapsed = max(1e-3, time.time() - start_ts)
                rate_per_min = completed / elapsed * 60.0
                remaining = max(0, total - completed)
                eta_sec = (remaining / rate_per_min * 60.0) if rate_per_min > 0 else None
                pct = (completed / total * 100.0) if total else 100.0
                success = (status == "ok")
                print(f"[{completed}/{total} {pct:5.1f}%] success={str(success).lower()} :: {u}", flush=True)
            # Simple adaptive pause: if >50% of last 20 attempts failed and at least 5 failures, pause 5 minutes
            if len(recent) >= 10 and sum(recent) / len(recent) >= 0.5 and sum(recent) >= 5:
                print("High error rate detected. Pausing for 300 seconds to cool down...")
                await asyncio.sleep(300)
                recent.clear()
            q.task_done()

    workers = [asyncio.create_task(worker(i)) for i in range(max(1, concurrency))]
    await q.join()
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "url",
            "page_id",
            "html_status",
            "html_bytes",
            "screenshot_bytes",
            "has_coupons",
            "has_promotions",
            "listings_count",
            "brands_count",
            "skipped",
            "error",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            if isinstance(r, dict):
                # Write only known fields to avoid crashes on unexpected keys
                row = {k: r.get(k) for k in fieldnames}
                writer.writerow(row)
                print({"url": r.get("url"), "has_promotions": r.get("has_promotions"), "brands_count": r.get("brands_count"), "skipped": r.get("skipped")})
            else:
                print({"error": str(r)})

    print(f"Wrote summary: {out_csv}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Run OpenAI extraction over a batch of URLs")
    p.add_argument("--seed", default=str(Path(__file__).resolve().parents[2] / "sem-pages.csv"))
    p.add_argument("--count", type=int, default=10)
    p.add_argument("--concurrency", type=int, default=2)
    p.add_argument("--out", default=str(Path(__file__).resolve().parents[3] / "data" / "latest" / "ai_extract_summary.csv"))
    p.add_argument("--quiet", action="store_true", help="Suppress INFO/DEBUG logs; show only progress lines")
    args = p.parse_args()

    asyncio.run(main(Path(args.seed), args.count, args.concurrency, Path(args.out), args.quiet))



import argparse
import asyncio
import sys
from pathlib import Path

from app.ai.process import process_url
from loguru import logger


async def main(urls: list[str], force: bool, quiet: bool) -> None:
    if quiet:
        try:
            logger.remove()
            logger.add(sys.stderr, level="WARNING")
        except Exception:
            pass
    for u in urls:
        res = await process_url(u, skip_if_exists=not force)
        print(res)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Process specific URL(s): fetch + AI + upsert")
    p.add_argument("--url", action="append", required=True, help="URL to process (can repeat)")
    p.add_argument("--force", action="store_true", help="Reprocess even if the URL already exists")
    p.add_argument("--quiet", action="store_true", help="Suppress INFO/DEBUG logs")
    args = p.parse_args()
    asyncio.run(main(args.url, args.force, args.quiet))



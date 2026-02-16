from __future__ import annotations

import argparse
import sys

from nasol import CollectorConfig, NasolCollector, NasolRepository
from nasol.parsing import ensure_season_list


def parse_seasons(raw: str) -> list[int]:
    tokens = [token.strip() for token in raw.split(",") if token.strip()]
    numbers = [int(token) for token in tokens]
    return ensure_season_list(numbers)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run NASOL collection in background process.")
    parser.add_argument("--db-path", required=True, help="SQLite DB path")
    parser.add_argument("--seasons", required=True, help="Comma separated seasons, e.g. 10,11,12")
    parser.add_argument(
        "--include-fallback",
        default="1",
        choices=("0", "1"),
        help="Use general search fallback when official content is missing",
    )
    parser.add_argument(
        "--dry-run",
        default="0",
        choices=("0", "1"),
        help="Collect metadata only without transcript download",
    )
    parser.add_argument(
        "--force-refresh",
        default="0",
        choices=("0", "1"),
        help="Re-download transcript even if already exists",
    )

    args = parser.parse_args()
    seasons = parse_seasons(args.seasons)
    if not seasons:
        print("No valid seasons given.", file=sys.stderr)
        return 2

    repo = NasolRepository(args.db_path)
    collector = NasolCollector(repo, CollectorConfig())
    collector.collect(
        seasons=seasons,
        include_fallback_search=args.include_fallback == "1",
        dry_run=args.dry_run == "1",
        force_transcript_refresh=args.force_refresh == "1",
        logger=print,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

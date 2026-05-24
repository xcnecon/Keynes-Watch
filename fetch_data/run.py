#!/usr/bin/env python3
"""Unified data fetcher runner for KeynesWatch.

Usage:
    python -m fetch_data.run                    # Update all data sources
    python -m fetch_data.run --source fred      # Only update FRED
    python -m fetch_data.run --source fred --series claims  # Only update FRED claims
    python -m fetch_data.run --list             # List all available sources and series

Can also be run directly:
    cd fetch_data && python run.py
"""

import argparse
import os
import sys
import logging

# ---------------------------------------------------------------------------
# Path setup: support both ``python -m fetch_data.run`` (from project root)
# and ``python run.py`` (from fetch_data/ directory).
# ---------------------------------------------------------------------------
_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_this_dir)

# Ensure project root is on sys.path so ``from fetch_data.xxx`` works
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Keep fetch_data/ itself importable for direct execution from this directory.
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from fetch_data.base import setup_logging  # noqa: E402

# ---------------------------------------------------------------------------
# Lazy imports -- each fetcher is imported only when needed so that missing
# optional dependencies for one source don't block others.
# ---------------------------------------------------------------------------

def _import_fetcher(name):
    """Import and return the fetcher class for *name*."""
    if name == 'fred':
        from fetch_data.sources.fred import FREDFetcher
        return FREDFetcher
    elif name == 'fiscal':
        from fetch_data.sources.fiscal import FiscalDataFetcher
        return FiscalDataFetcher
    elif name == 'nyfed':
        from fetch_data.sources.nyfed import NYFedFetcher
        return NYFedFetcher
    elif name == 'treasury':
        from fetch_data.sources.treasury import TreasuryFetcher
        return TreasuryFetcher
    elif name == 'bea':
        from fetch_data.sources.bea import BEAFetcher
        return BEAFetcher
    elif name == 'indeed':
        from fetch_data.sources.indeed import IndeedFetcher
        return IndeedFetcher
    elif name == 'pboc':
        from fetch_data.sources.pboc import PBOCFetcher
        return PBOCFetcher
    elif name == 'nbs':
        from fetch_data.sources.nbs import NBSFetcher
        return NBSFetcher
    elif name == 'mof':
        from fetch_data.sources.mof import MOFFetcher
        return MOFFetcher
    else:
        raise ValueError(f"Unknown source: {name}")


# Canonical ordering of all data sources
ALL_SOURCES = ['fred', 'fiscal', 'nyfed', 'treasury', 'bea', 'indeed', 'pboc', 'nbs', 'mof']


def list_sources():
    """Print available sources and their series."""
    for name in ALL_SOURCES:
        try:
            cls = _import_fetcher(name)
            print(f"\n{name}:")
            for s in cls.SERIES:
                print(f"  - {s.get('table', 'unknown')}")
        except Exception as exc:
            print(f"\n{name}: (import error: {exc})")


def main():
    parser = argparse.ArgumentParser(description='KeynesWatch Data Fetcher')
    parser.add_argument(
        '--source',
        choices=ALL_SOURCES,
        help='Only run this data source',
    )
    parser.add_argument(
        '--series',
        help='Filter series by table name substring',
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='List all sources and series',
    )
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger('run')

    if args.list:
        list_sources()
        return

    sources = [args.source] if args.source else ALL_SOURCES
    all_ok = True

    for name in sources:
        try:
            cls = _import_fetcher(name)
        except Exception as exc:
            logger.error(f"Failed to import source '{name}': {exc}")
            all_ok = False
            continue

        fetcher = cls()
        ok = fetcher.run(series_filter=args.series)
        if not ok:
            all_ok = False

    sys.exit(0 if all_ok else 1)


if __name__ == '__main__':
    main()

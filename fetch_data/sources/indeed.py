"""IndeedFetcher -- downloads Indeed Hiring Lab CSV datasets from GitHub.

Unlike other fetchers, this one does NOT write to a database.  It downloads
CSV files to disk, using SHA-256 hashing to detect changes and avoid
unnecessary overwrites.  The logic mirrors ``github/indeed.py``.
"""

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Optional

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from fetch_data.base import BaseFetcher

# ---------------------------------------------------------------------------
# Source URLs
# ---------------------------------------------------------------------------
RAW_CSV_URL = (
    'https://raw.githubusercontent.com/hiring-lab/indeed-wage-tracker/main/'
    'posted-wage-growth-by-country.csv'
)
RAW_SECTOR_CSV_URL = (
    'https://raw.githubusercontent.com/hiring-lab/indeed-wage-tracker/main/'
    'posted-wage-growth-by-sector.csv'
)
RAW_JP_US_AGG_URL = (
    'https://raw.githubusercontent.com/hiring-lab/job_postings_tracker/master/'
    'US/aggregate_job_postings_US.csv'
)
RAW_JP_US_SECTOR_URL = (
    'https://raw.githubusercontent.com/hiring-lab/job_postings_tracker/master/'
    'US/job_postings_by_sector_US.csv'
)

# Default output directory: fetch_data/github/
_DEFAULT_DIR = Path(__file__).resolve().parent.parent / 'github'


def _compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_meta(meta_path: Path) -> Dict[str, Optional[str]]:
    if meta_path.exists():
        try:
            with meta_path.open('r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _write_meta(meta_path: Path, meta: Dict) -> None:
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        'w', delete=False, dir=str(meta_path.parent), encoding='utf-8'
    ) as tmp:
        json.dump(meta, tmp, ensure_ascii=False, indent=2)
        tmp_name = tmp.name
    os.replace(tmp_name, str(meta_path))


class IndeedFetcher(BaseFetcher):
    """Downloads Indeed Hiring Lab CSV files from GitHub raw URLs."""

    SERIES = [
        {
            'table': 'indeed_wage_country',
            'url': RAW_CSV_URL,
            'dest': 'posted-wage-growth-by-country.csv',
            'meta': 'posted-wage-growth-by-country.meta.json',
        },
        {
            'table': 'indeed_wage_sector',
            'url': RAW_SECTOR_CSV_URL,
            'dest': 'posted-wage-growth-by-sector.csv',
            'meta': 'posted-wage-growth-by-sector.meta.json',
        },
        {
            'table': 'indeed_jp_us_agg',
            'url': RAW_JP_US_AGG_URL,
            'dest': 'aggregate_job_postings_US.csv',
            'meta': 'aggregate_job_postings_US.meta.json',
        },
        {
            'table': 'indeed_jp_us_sector',
            'url': RAW_JP_US_SECTOR_URL,
            'dest': 'job_postings_by_sector_US.csv',
            'meta': 'job_postings_by_sector_US.meta.json',
        },
    ]

    def __init__(self, name=None, output_dir=None):
        super().__init__(name)
        self.output_dir = Path(output_dir) if output_dir else _DEFAULT_DIR

    # ------------------------------------------------------------------
    # Download logic (mirrors github/indeed.py)
    # ------------------------------------------------------------------

    def _download_if_updated(self, url, dest_csv_path, meta_path, force=False):
        """Download *url* to *dest_csv_path* if content has changed.

        Returns True if a new file was written, False otherwise.
        """
        previous_meta = _read_meta(meta_path)
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; IndeedFetcher/2.0)',
            'Accept': 'text/csv, */*;q=0.1',
            'Cache-Control': 'no-cache',
        }

        if not force:
            etag = previous_meta.get('etag')
            last_modified = previous_meta.get('last_modified')
            if etag:
                headers['If-None-Match'] = etag
            elif last_modified:
                headers['If-Modified-Since'] = last_modified

        resp = self.session.get(url, headers=headers, timeout=30)

        if resp.status_code == 304:
            self.logger.info(f'  Not modified (304): {dest_csv_path.name}')
            return False

        resp.raise_for_status()

        data = resp.content
        new_sha256 = _compute_sha256(data)
        etag_value = resp.headers.get('ETag')
        last_modified_value = resp.headers.get('Last-Modified')
        content_length = int(
            resp.headers.get('Content-Length', len(data))
        )

        # Check hash
        prev_sha = previous_meta.get('sha256')
        if prev_sha == new_sha256 and dest_csv_path.exists() and not force:
            updated_meta = {
                'url': url,
                'etag': etag_value,
                'last_modified': last_modified_value,
                'sha256': new_sha256,
                'content_length': content_length,
                'downloaded_at': datetime.now(timezone.utc).isoformat(),
            }
            _write_meta(meta_path, updated_meta)
            self.logger.info(
                f'  Content unchanged (by hash): {dest_csv_path.name}'
            )
            return False

        # Write CSV atomically
        dest_csv_path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(
            'wb', delete=False, dir=str(dest_csv_path.parent)
        ) as tmp:
            tmp.write(data)
            tmp_name = tmp.name
        os.replace(tmp_name, str(dest_csv_path))

        # Write metadata
        meta = {
            'url': url,
            'etag': etag_value,
            'last_modified': last_modified_value,
            'sha256': new_sha256,
            'content_length': content_length,
            'downloaded_at': datetime.now(timezone.utc).isoformat(),
        }
        _write_meta(meta_path, meta)
        self.logger.info(f'  Downloaded update: {dest_csv_path.name}')
        return True

    # ------------------------------------------------------------------
    # BaseFetcher interface
    # ------------------------------------------------------------------

    def fetch_series(self, series_config):
        url = series_config['url']
        dest = self.output_dir / series_config['dest']
        meta = self.output_dir / series_config['meta']
        self._download_if_updated(url, dest, meta)


if __name__ == '__main__':
    fetcher = IndeedFetcher()
    fetcher.run()

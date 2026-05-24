import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


RAW_CSV_URL = (
    "https://raw.githubusercontent.com/hiring-lab/indeed-wage-tracker/main/"
    "posted-wage-growth-by-country.csv"
)

RAW_SECTOR_CSV_URL = (
    "https://raw.githubusercontent.com/hiring-lab/indeed-wage-tracker/main/"
    "posted-wage-growth-by-sector.csv"
)

RAW_JP_US_AGG_URL = (
    "https://raw.githubusercontent.com/hiring-lab/job_postings_tracker/master/US/aggregate_job_postings_US.csv"
)

RAW_JP_US_SECTOR_URL = (
    "https://raw.githubusercontent.com/hiring-lab/job_postings_tracker/master/US/job_postings_by_sector_US.csv"
)


def compute_sha256(data: bytes) -> str:
    hasher = hashlib.sha256()
    hasher.update(data)
    return hasher.hexdigest()


def read_meta(meta_path: Path) -> Dict[str, Optional[str]]:
    if meta_path.exists():
        try:
            with meta_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def write_meta(meta_path: Path, meta: Dict) -> None:
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", delete=False, dir=str(meta_path.parent), encoding="utf-8") as tmp:
        json.dump(meta, tmp, ensure_ascii=False, indent=2)
        tmp_name = tmp.name
    os.replace(tmp_name, meta_path)


def download_if_updated(
    url: str,
    dest_csv_path: Path,
    meta_path: Path,
    force: bool = False,
    timeout_seconds: int = 30,
) -> bool:
    """
    Returns True if a new file was downloaded, False if not modified.
    """
    previous_meta = read_meta(meta_path)
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; IndeedWageTrackerFetcher/1.0)",
        "Accept": "text/csv, */*;q=0.1",
        "Cache-Control": "no-cache",
    }

    if not force:
        etag = previous_meta.get("etag") if isinstance(previous_meta, dict) else None
        last_modified = previous_meta.get("last_modified") if isinstance(previous_meta, dict) else None
        if etag:
            headers["If-None-Match"] = etag
        elif last_modified:
            headers["If-Modified-Since"] = last_modified

    request = Request(url=url, headers=headers, method="GET")

    try:
        with urlopen(request, timeout=timeout_seconds) as resp:
            status_code = resp.getcode()
            if status_code == 304:
                # Rare path: some handlers may not raise on 304; treat as not modified
                print("No update: remote CSV not modified.")
                return False
            data = resp.read()
            new_sha256 = compute_sha256(data)
            response_headers = {k.lower(): v for k, v in resp.headers.items()}
            etag_value = response_headers.get("etag")
            last_modified_value = response_headers.get("last-modified")
            content_length = int(response_headers.get("content-length", len(data)))
    except HTTPError as e:
        if e.code == 304:
            print("No update: remote CSV not modified (304).")
            return False
        raise
    except URLError as e:
        raise SystemExit(f"Network error fetching CSV: {e}") from e

    # If content hash matches previous, skip writing the file but refresh meta.
    prev_sha = previous_meta.get("sha256") if isinstance(previous_meta, dict) else None
    is_same_content = prev_sha == new_sha256 and dest_csv_path.exists()
    if is_same_content and not force:
        # Update meta timestamp in case headers changed upstream.
        updated_meta = {
            "url": url,
            "etag": etag_value,
            "last_modified": last_modified_value,
            "sha256": new_sha256,
            "content_length": content_length,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }
        write_meta(meta_path, updated_meta)
        print("No update: content unchanged (by hash).")
        return False

    # Write CSV atomically
    dest_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("wb", delete=False, dir=str(dest_csv_path.parent)) as tmp:
        tmp.write(data)
        tmp_name = tmp.name
    os.replace(tmp_name, dest_csv_path)

    # Write metadata
    meta = {
        "url": url,
        "etag": etag_value,
        "last_modified": last_modified_value,
        "sha256": new_sha256,
        "content_length": content_length,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
    write_meta(meta_path, meta)

    print(f"Downloaded update to: {dest_csv_path}")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check for updates and download Indeed wage tracker CSV if updated."
    )
    default_dir = Path(__file__).parent
    parser.add_argument(
        "--url",
        default=RAW_CSV_URL,
        help="Source CSV URL (default: raw GitHub CSV for country).",
    )
    parser.add_argument(
        "--dest",
        default=str(default_dir / "posted-wage-growth-by-country.csv"),
        help="Destination CSV path for country dataset.",
    )
    parser.add_argument(
        "--meta",
        default=str(default_dir / "posted-wage-growth-by-country.meta.json"),
        help="Metadata JSON path for country dataset (ETag/Last-Modified/hash).",
    )
    parser.add_argument(
        "--sector",
        action="store_true",
        help="Also download the sector dataset alongside the country dataset.",
    )
    parser.add_argument(
        "--only-sector",
        action="store_true",
        help="Download only the sector dataset and skip the country dataset.",
    )
    parser.add_argument(
        "--sector-dest",
        default=str(default_dir / "posted-wage-growth-by-sector.csv"),
        help="Destination CSV path for sector dataset.",
    )
    parser.add_argument(
        "--sector-meta",
        default=str(default_dir / "posted-wage-growth-by-sector.meta.json"),
        help="Metadata JSON path for sector dataset (ETag/Last-Modified/hash).",
    )
    parser.add_argument(
        "--only-job-postings",
        action="store_true",
        help="Download only the job postings datasets (aggregate and sector).",
    )
    parser.add_argument(
        "--jp-agg-dest",
        default=str(default_dir / "aggregate_job_postings_US.csv"),
        help="Destination CSV path for US aggregate job postings dataset.",
    )
    parser.add_argument(
        "--jp-agg-meta",
        default=str(default_dir / "aggregate_job_postings_US.meta.json"),
        help="Metadata JSON path for US aggregate job postings dataset.",
    )
    parser.add_argument(
        "--jp-sector-dest",
        default=str(default_dir / "job_postings_by_sector_US.csv"),
        help="Destination CSV path for US job postings by sector dataset.",
    )
    parser.add_argument(
        "--jp-sector-meta",
        default=str(default_dir / "job_postings_by_sector_US.meta.json"),
        help="Metadata JSON path for US job postings by sector dataset.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force download even if server reports not modified or hash matches.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    # Sector-only mode
    if args.only_sector:
        try:
            sector_dest_path = Path(args.sector_dest)
            sector_meta_path = Path(args.sector_meta)
            download_if_updated(RAW_SECTOR_CSV_URL, sector_dest_path, sector_meta_path, force=args.force)
        except HTTPError as e:
            raise SystemExit(f"HTTP error {e.code} fetching sector CSV: {e}") from e
        return

    # Job postings only mode
    if args.only_job_postings:
        try:
            jp_agg_dest = Path(args.jp_agg_dest)
            jp_agg_meta = Path(args.jp_agg_meta)
            jp_sector_dest = Path(args.jp_sector_dest)
            jp_sector_meta = Path(args.jp_sector_meta)
            download_if_updated(RAW_JP_US_AGG_URL, jp_agg_dest, jp_agg_meta, force=args.force)
            download_if_updated(RAW_JP_US_SECTOR_URL, jp_sector_dest, jp_sector_meta, force=args.force)
        except HTTPError as e:
            raise SystemExit(f"HTTP error {e.code} fetching job postings CSV: {e}") from e
        return

    # Country (default behavior)
    try:
        url = args.url
        dest_path = Path(args.dest)
        meta_path = Path(args.meta)
        download_if_updated(url, dest_path, meta_path, force=args.force)

        # Always also fetch sector by default (unless --only-sector was used above)
        sector_dest_path = Path(args.sector_dest)
        sector_meta_path = Path(args.sector_meta)
        download_if_updated(RAW_SECTOR_CSV_URL, sector_dest_path, sector_meta_path, force=args.force)

        # Also fetch job postings datasets by default
        jp_agg_dest = Path(args.jp_agg_dest)
        jp_agg_meta = Path(args.jp_agg_meta)
        jp_sector_dest = Path(args.jp_sector_dest)
        jp_sector_meta = Path(args.jp_sector_meta)
        download_if_updated(RAW_JP_US_AGG_URL, jp_agg_dest, jp_agg_meta, force=args.force)
        download_if_updated(RAW_JP_US_SECTOR_URL, jp_sector_dest, jp_sector_meta, force=args.force)
    except HTTPError as e:
        raise SystemExit(f"HTTP error {e.code} fetching CSV: {e}") from e


if __name__ == "__main__":
    main()
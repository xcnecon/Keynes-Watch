"""
NYFedFetcher -- unified fetcher for New York Federal Reserve data.

Replaces the following standalone scripts:
  - nyfed/repo.py
  - nyfed/onrates.py
"""

import os
import sys
import argparse
from datetime import timedelta

import pandas as pd

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from fetch_data.base import BaseFetcher


# ---------------------------------------------------------------------------
# Value-cleaning helper (inlined from common/functions.py)
# ---------------------------------------------------------------------------

def clean_value(value, data_type="text"):
    """Return cleaned value, or None for missing / sentinel values."""
    if pd.isna(value) if not isinstance(value, str) else False:
        return None
    if value in (None, 'null', '', ' ', '*', 'nan', 'NaN'):
        return None
    try:
        if data_type == "numeric":
            return float(value)
        elif data_type == "date":
            return value
        return value
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Series configurations
# ---------------------------------------------------------------------------

SERIES_REPO = {
    'table': 'repo',
    'create_sql': """
        CREATE TABLE IF NOT EXISTS repo (
            record_date DATE,
            type VARCHAR(100),
            amount DECIMAL(18,2),
            PRIMARY KEY (record_date, type)
        )
    """,
    'url': 'https://markets.newyorkfed.org/api/rp/results/search.json',
    'default_start_date': '2003-02-07',
    'date_column': 'record_date',
    'handler': '_handle_repo',
}

SERIES_ONRATES = {
    'table': 'onrates',
    'create_sql': """
        CREATE TABLE IF NOT EXISTS onrates (
            record_date DATE,
            type VARCHAR(100),
            rate1 DECIMAL(18, 2),
            rate25 DECIMAL(18, 2),
            rate50 DECIMAL(18, 2),
            rate75 DECIMAL(18, 2),
            rate99 DECIMAL(18, 2),
            target_high DECIMAL(18, 2),
            target_low DECIMAL(18, 2),
            vol DECIMAL(18,2),
            PRIMARY KEY (record_date, type)
        )
    """,
    'url': 'https://markets.newyorkfed.org/api/rates/all/search.json',
    'default_start_date': '2000-07-03',
    'date_column': 'record_date',
    'handler': '_handle_onrates',
}


# ---------------------------------------------------------------------------
# NYFedFetcher
# ---------------------------------------------------------------------------

class NYFedFetcher(BaseFetcher):
    """Fetcher for New York Federal Reserve market data APIs."""

    SERIES = [
        SERIES_REPO,
        SERIES_ONRATES,
    ]

    # -- Main dispatch ------------------------------------------------------

    def fetch_series(self, series_config):
        handler_name = series_config.get('handler')
        getattr(self, handler_name)(series_config)

    # -- repo handler -------------------------------------------------------

    def _handle_repo(self, config):
        conn = self.get_connection()
        try:
            self.ensure_table(conn, config['create_sql'])
            latest_date = self.get_latest_date(conn, config['table'],
                                               config.get('date_column', 'record_date'))

            url = config['url']
            latest_dt = pd.to_datetime(latest_date).date() if latest_date else None
            start_date = (
                (latest_dt + timedelta(days=1)).isoformat()
                if latest_dt else config['default_start_date']
            )
            resp = self.session.get(f"{url}?startDate={start_date}")
            resp.raise_for_status()
            all_data = resp.json().get('repo', {}).get('operations', [])

            if not all_data:
                self.logger.info("  No new repo data")
                return

            self.logger.info("  Fetched %d repo operations", len(all_data))

            repo_totals = {}
            for record in all_data:
                record_date = clean_value(record.get("operationDate"), data_type='date')
                op_type = clean_value(record.get("operationType"), data_type='text')
                if not record_date or not op_type:
                    continue

                if latest_dt and pd.to_datetime(record_date).date() <= latest_dt:
                    continue

                raw_amount = clean_value(record.get("totalAmtAccepted"), data_type='numeric')
                if raw_amount is None:
                    raw_amount = 0.0
                amount = raw_amount / 1_000_000_000
                key = (record_date, op_type)
                repo_totals[key] = repo_totals.get(key, 0.0) + amount

            if not repo_totals:
                self.logger.info("  No new repo rows after latest-date filtering")
                return

            cursor = conn.cursor()
            try:
                insert_sql = """
                    INSERT INTO repo (record_date, type, amount)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE amount = VALUES(amount)
                """
                rows = [
                    (record_date, op_type, amount)
                    for (record_date, op_type), amount in sorted(repo_totals.items())
                ]
                cursor.executemany(insert_sql, rows)

                conn.commit()
                self.logger.info("  Repo data upserted successfully")
            finally:
                cursor.close()
        finally:
            conn.close()

    # -- onrates handler ----------------------------------------------------

    def _handle_onrates(self, config):
        conn = self.get_connection()
        try:
            self.ensure_table(conn, config['create_sql'])
            latest_date = self.get_latest_date(conn, config['table'],
                                               config.get('date_column', 'record_date'))

            url = config['url']
            start_date = str(latest_date) if latest_date else config['default_start_date']
            resp = self.session.get(f"{url}?startDate={start_date}")
            resp.raise_for_status()
            all_data = resp.json().get('refRates', [])

            if not all_data:
                self.logger.info("  No new onrates data")
                return

            self.logger.info("  Fetched %d rate records", len(all_data))

            columns = ['record_date', 'type', 'rate1', 'rate25', 'rate50',
                       'rate75', 'rate99', 'target_high', 'target_low', 'vol']
            update_cols = ['rate1', 'rate25', 'rate50', 'rate75', 'rate99',
                           'target_high', 'target_low', 'vol']

            rows = []
            for record in all_data:
                record_date = clean_value(record.get("effectiveDate"), data_type='date')
                rate_type = clean_value(record.get("type"), data_type='text')

                # Only keep SOFR and EFFR
                if rate_type not in ("SOFR", "EFFR"):
                    continue

                rate1 = clean_value(record.get("percentPercentile1"), data_type='numeric')
                rate25 = clean_value(record.get("percentPercentile25"), data_type='numeric')
                rate50 = clean_value(record.get("percentRate"), data_type='numeric')
                rate75 = clean_value(record.get("percentPercentile75"), data_type='numeric')
                rate99 = clean_value(record.get("percentPercentile99"), data_type='numeric')
                target_high = clean_value(record.get("targetRateTo"), data_type='numeric')
                target_low = clean_value(record.get("targetRateFrom"), data_type='numeric')
                vol = clean_value(record.get("volumeInBillions"), data_type='numeric')

                rows.append((
                    record_date, rate_type, rate1, rate25, rate50,
                    rate75, rate99, target_high, target_low, vol,
                ))

            self.upsert_rows(conn, config['table'], columns, rows,
                             on_duplicate_update=update_cols)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Fetch NY Fed data series")
    parser.add_argument('--series', type=str, default=None,
                        help="Only fetch series whose table name contains this string")
    args = parser.parse_args()

    fetcher = NYFedFetcher()
    success = fetcher.run(series_filter=args.series)
    raise SystemExit(0 if success else 1)

"""
FREDFetcher -- unified fetcher for all FRED-sourced series.

Replaces the six standalone scripts:
    fred/claims.py   -> fred_claims            (multi-series, full refresh)
    fred/payrolls.py -> fred_payrolls_panel     (multi-series, full refresh)
    fred/ur.py       -> fred_unemployment       (multi-series, full refresh)
    fred/cpi.py      -> fred_cpiaucl            (single-series, incremental)
    fred/gdp.py      -> fred_gdp               (single-series, incremental)
    fred/fed.py      -> fred_wresbal            (single-series, incremental)
                      + fedwire_monthly_stats   (custom HTML scrape)
"""

import os
import re
import sys
from datetime import datetime, timedelta
from html import unescape

# Ensure the project root is on sys.path so `fetch_data.base` is importable
# when running this file directly.
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from fetch_data.base import BaseFetcher


# ---------------------------------------------------------------------------
# Series configurations
# ---------------------------------------------------------------------------

_CLAIMS_SERIES_IDS = [
    "ICSA",   # Initial Claims (Weekly SA)
    "CCSA",   # Continuing Claims (Weekly SA)
]

_PAYROLLS_SERIES_IDS = [
    # Headline + splits
    "PAYEMS",         # All Employees, Total Nonfarm (headline NFP)
    "USPRIV",         # All Employees, Total Private
    "USGOVT",         # All Employees, Government
    "CES9091000001",  # All Employees, Federal (level detail)
    "CES9092000001",  # All Employees, State Government
    "CES9093000001",  # All Employees, Local Government
    # Broad aggregates
    "USGOOD",         # Goods-Producing (Mining+Construction+Manufacturing)
    "SRVPRD",         # Service-Providing (private + government services)
    # Major supersectors
    "USMINE",         # Mining and Logging
    "USCONS",         # Construction
    "MANEMP",         # Manufacturing (total)
    "USTPU",          # Trade, Transportation, and Utilities (TTU)
    "USWTRADE",       # Wholesale Trade
    "USTRADE",        # Retail Trade
    "CES4300000001",  # Transportation and Warehousing
    "CES4422000001",  # Utilities (component of TTU)
    "USINFO",         # Information
    "USFIRE",         # Financial Activities (Finance & Real Estate)
    "USPBS",          # Professional & Business Services
    "USEHS",          # Private Education & Health Services
    "USLAH",          # Leisure & Hospitality
    "USSERV",         # Other Services (except Public Admin)
    # Useful manufacturing splits
    "DMANEMP",        # Durable Goods Manufacturing
    "NDMANEMP",       # Nondurable Goods Manufacturing
]

_UNEMPLOYMENT_SERIES_IDS = [
    "UNRATE",       # U3 Unemployment Rate
    "U6RATE",       # U6 Unemployment Rate
    "LNS14000003",  # White Unemployment Rate
    "LNS14000006",  # Black Unemployment Rate
    "LNS14000009",  # Hispanic Unemployment Rate
    "UEMPMEAN",     # Average Weeks Unemployed
    "UEMPMED",      # Median Weeks Unemployed
]


class FREDFetcher(BaseFetcher):
    """Fetches economic data from the FRED API and Fedwire HTML page."""

    FRED_URL = "https://api.stlouisfed.org/fred/series/observations"

    SERIES = [
        # ---- Multi-series tables (full refresh + upsert) ----
        {
            'table': 'fred_claims',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS fred_claims (
                    series_id VARCHAR(64) NOT NULL,
                    observation_date DATE NOT NULL,
                    value DECIMAL(18,4),
                    PRIMARY KEY (series_id, observation_date)
                )
            """,
            'series_ids': _CLAIMS_SERIES_IDS,
            'strategy': 'full',
            'date_column': 'observation_date',
            'columns': ['series_id', 'observation_date', 'value'],
            'update_columns': ['value'],
            'multi_series': True,
        },
        {
            'table': 'fred_payrolls_panel',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS fred_payrolls_panel (
                    series_id VARCHAR(64) NOT NULL,
                    observation_date DATE NOT NULL,
                    value DECIMAL(18,4),
                    PRIMARY KEY (series_id, observation_date)
                )
            """,
            'series_ids': _PAYROLLS_SERIES_IDS,
            'strategy': 'full',
            'date_column': 'observation_date',
            'columns': ['series_id', 'observation_date', 'value'],
            'update_columns': ['value'],
            'multi_series': True,
        },
        {
            'table': 'fred_unemployment',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS fred_unemployment (
                    series_id VARCHAR(64) NOT NULL,
                    observation_date DATE NOT NULL,
                    value DECIMAL(18,4),
                    PRIMARY KEY (series_id, observation_date)
                )
            """,
            'series_ids': _UNEMPLOYMENT_SERIES_IDS,
            'strategy': 'full',
            'date_column': 'observation_date',
            'columns': ['series_id', 'observation_date', 'value'],
            'update_columns': ['value'],
            'multi_series': True,
        },

        # ---- Single-series tables (incremental) ----
        {
            'table': 'fred_cpiaucl',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS fred_cpiaucl (
                    observation_date DATE PRIMARY KEY,
                    value DECIMAL(18,4)
                )
            """,
            'series_ids': ["CPIAUCSL"],
            'strategy': 'incremental',
            'date_column': 'observation_date',
            'columns': ['observation_date', 'value'],
            'update_columns': ['value'],
            'multi_series': False,
        },
        {
            'table': 'fred_gdp',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS fred_gdp (
                    observation_date DATE PRIMARY KEY,
                    value DECIMAL(18,4)
                )
            """,
            'series_ids': ["GDP"],
            'strategy': 'incremental',
            'date_column': 'observation_date',
            'columns': ['observation_date', 'value'],
            'update_columns': ['value'],
            'multi_series': False,
        },
        {
            'table': 'fred_wresbal',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS fred_wresbal (
                    observation_date DATE PRIMARY KEY,
                    value DECIMAL(18,4)
                )
            """,
            'series_ids': ["WRESBAL"],
            'strategy': 'incremental',
            'date_column': 'observation_date',
            'columns': ['observation_date', 'value'],
            'update_columns': ['value'],
            'multi_series': False,
        },

        # ---- Custom: Fedwire monthly stats (HTML scrape) ----
        {
            'table': 'fedwire_monthly_stats',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS fedwire_monthly_stats (
                    period_date DATE PRIMARY KEY,
                    transfers_total DECIMAL(18,4),
                    transfers_change_pct DECIMAL(18,4),
                    value_total_millions DECIMAL(18,4),
                    value_change_pct DECIMAL(18,4),
                    average_value_millions DECIMAL(18,4),
                    daily_avg_transfers DECIMAL(18,4),
                    daily_avg_value_millions DECIMAL(18,4)
                )
            """,
            'strategy': 'custom',
            'date_column': 'period_date',
            'columns': [
                'period_date',
                'transfers_total',
                'transfers_change_pct',
                'value_total_millions',
                'value_change_pct',
                'average_value_millions',
                'daily_avg_transfers',
                'daily_avg_value_millions',
            ],
            'update_columns': [
                'transfers_total',
                'transfers_change_pct',
                'value_total_millions',
                'value_change_pct',
                'average_value_millions',
                'daily_avg_transfers',
                'daily_avg_value_millions',
            ],
        },
    ]

    def __init__(self):
        super().__init__(name='FRED')
        self.api_key = os.getenv('FRED_API_KEY')

    # ------------------------------------------------------------------
    # FRED API helper
    # ------------------------------------------------------------------

    def _fetch_fred_observations(self, series_id, start_date=None):
        """Call the FRED API and return a list of observation dicts."""
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
        }
        if start_date:
            params["observation_start"] = start_date

        self.logger.info("Requesting FRED observations for %s (start=%s)",
                         series_id, start_date or 'all')
        resp = self.session.get(self.FRED_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data.get("observations", [])

    # ------------------------------------------------------------------
    # Observation parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_observations(observations, include_series_id=False,
                            series_id=None):
        """Parse FRED observations into row tuples.

        Returns list of tuples:
            - (series_id, date, value) if include_series_id is True
            - (date, value) otherwise
        """
        rows = []
        for obs in observations:
            val_str = obs.get("value")
            if val_str is None or val_str.strip() == ".":
                continue

            date_str = obs.get("date")
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d").date()
                val = float(val_str)
            except Exception:
                continue

            if include_series_id:
                rows.append((series_id, dt, val))
            else:
                rows.append((dt, val))
        return rows

    # ------------------------------------------------------------------
    # Main dispatch
    # ------------------------------------------------------------------

    def fetch_series(self, series_config):
        """Fetch and store data for one series config entry."""
        strategy = series_config.get('strategy', 'full')
        if strategy == 'custom':
            self._fetch_custom(series_config)
        elif strategy == 'incremental':
            self._fetch_incremental(series_config)
        else:
            # 'full' -- full-history refresh with upsert
            self._fetch_full(series_config)

    # ------------------------------------------------------------------
    # Full-refresh strategy (multi-series tables)
    # ------------------------------------------------------------------

    def _fetch_full(self, sc):
        """Full-history refresh: fetch ALL observations for every series_id,
        upsert into the table.  Used for claims / payrolls / unemployment
        where FRED may revise historical data."""
        conn = self.get_connection()
        try:
            self.ensure_table(conn, sc['create_sql'])
            table = sc['table']
            columns = sc['columns']
            update_cols = sc['update_columns']
            multi = sc.get('multi_series', False)
            series_ids = sc.get('series_ids', [])

            for i, sid in enumerate(series_ids):
                observations = self._fetch_fred_observations(sid, start_date=None)
                rows = self._parse_observations(
                    observations,
                    include_series_id=multi,
                    series_id=sid,
                )
                self.upsert_rows(conn, table, columns, rows,
                                 on_duplicate_update=update_cols)

                # Rate-limit between API calls (skip after the last one)
                if i < len(series_ids) - 1:
                    self.rate_limit_pause()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Incremental strategy (single-series tables)
    # ------------------------------------------------------------------

    def _fetch_incremental(self, sc):
        """Incremental fetch: only pull observations after the latest date
        already stored.  Used for CPI, GDP, WRESBAL."""
        conn = self.get_connection()
        try:
            self.ensure_table(conn, sc['create_sql'])
            table = sc['table']
            date_col = sc['date_column']
            columns = sc['columns']
            update_cols = sc['update_columns']
            multi = sc.get('multi_series', False)
            series_ids = sc.get('series_ids', [])

            latest = self.get_latest_date(conn, table, date_col)
            if latest is None:
                start_date = "1945-01-01"
            else:
                start_date = (latest + timedelta(days=1)).strftime("%Y-%m-%d")

            for i, sid in enumerate(series_ids):
                observations = self._fetch_fred_observations(sid, start_date=start_date)
                rows = self._parse_observations(
                    observations,
                    include_series_id=multi,
                    series_id=sid,
                )
                self.upsert_rows(conn, table, columns, rows,
                                 on_duplicate_update=update_cols)

                if i < len(series_ids) - 1:
                    self.rate_limit_pause()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Custom: Fedwire monthly stats (HTML scrape)
    # ------------------------------------------------------------------

    FEDWIRE_MONTHLY_URL = (
        "https://www.frbservices.org/resources/financial-services/"
        "wires/volume-value-stats/monthly-stats.html"
    )

    MONTH_MAP = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
        'January': 1, 'February': 2, 'March': 3, 'April': 4,
        'June': 6, 'July': 7, 'August': 8, 'September': 9,
        'October': 10, 'November': 11, 'December': 12,
    }

    def _fetch_custom(self, sc):
        """Handle custom strategy entries.  Currently only
        fedwire_monthly_stats uses this."""
        if sc['table'] == 'fedwire_monthly_stats':
            self._fetch_fedwire(sc)
        else:
            raise ValueError(f"Unknown custom table: {sc['table']}")

    def _fetch_fedwire(self, sc):
        """Scrape the Fedwire monthly statistics HTML page and upsert rows."""
        conn = self.get_connection()
        try:
            self.ensure_table(conn, sc['create_sql'])
            table = sc['table']
            date_col = sc['date_column']
            columns = sc['columns']
            update_cols = sc['update_columns']

            latest = self.get_latest_date(conn, table, date_col)
            if latest:
                self.logger.info("Fedwire latest stored period: %s", latest)
            else:
                self.logger.info("Fedwire: no existing period; full backfill will run")

            html = self._fw_fetch_html()
            data_rows = self._fw_find_monthly_stats_rows(html)
            if not data_rows:
                raise RuntimeError(
                    "Fedwire: could not locate monthly statistics table on page"
                )

            rows = []
            for r in data_rows:
                period_date = self._fw_parse_period_to_date(r[0])
                if not period_date:
                    continue
                # Skip rows already stored (incremental)
                if latest is not None and period_date <= latest:
                    continue

                rows.append((
                    period_date,
                    self._fw_normalize_number(r[1]),
                    self._fw_normalize_number(r[2]),
                    self._fw_normalize_number(r[3]),
                    self._fw_normalize_number(r[4]),
                    self._fw_normalize_number(r[5]),
                    self._fw_normalize_number(r[6]),
                    self._fw_normalize_number(r[7]),
                ))

            self.upsert_rows(conn, table, columns, rows,
                             on_duplicate_update=update_cols)
        finally:
            conn.close()

    # ---- Fedwire HTML parsing helpers (ported from fed.py) ----

    def _fw_fetch_html(self):
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        self.logger.info("Requesting Fedwire monthly stats: %s",
                         self.FEDWIRE_MONTHLY_URL)
        resp = self.session.get(self.FEDWIRE_MONTHLY_URL,
                                headers=headers, timeout=60)
        resp.raise_for_status()
        return resp.text

    @staticmethod
    def _fw_normalize_number(text):
        if text is None:
            return None
        s = unescape(text).strip()
        if s == '' or s == '-' or s == '\u2014':
            return None
        s = s.replace('%', '').replace(',', '')
        if s.startswith('(') and s.endswith(')'):
            s = '-' + s[1:-1]
        try:
            return float(s)
        except Exception:
            return None

    def _fw_parse_period_to_date(self, period_text):
        s = unescape(period_text).strip()
        m = re.match(r"^(\d{4})\s*:\s*([A-Za-z]+)$", s)
        if not m:
            return None
        year = int(m.group(1))
        month_name = m.group(2).title()
        month = self.MONTH_MAP.get(month_name) or self.MONTH_MAP.get(month_name[:3])
        if not month:
            return None
        return datetime(year, month, 1).date()

    @staticmethod
    def _fw_strip_tags(html):
        return re.sub(r"<[^>]+>", "", html)

    @staticmethod
    def _fw_extract_tables(html):
        return re.findall(
            r"<table[\s\S]*?>[\s\S]*?</table>", html, flags=re.IGNORECASE
        )

    @classmethod
    def _fw_extract_rows_from_table(cls, table_html):
        rows = re.findall(
            r"<tr[\s\S]*?>[\s\S]*?</tr>", table_html, flags=re.IGNORECASE
        )
        parsed_rows = []
        for tr in rows:
            cells = re.findall(
                r"<(?:td|th)[^>]*>([\s\S]*?)</(?:td|th)>",
                tr, flags=re.IGNORECASE,
            )
            if cells:
                parsed_rows.append(cells)
        return parsed_rows

    @classmethod
    def _fw_find_monthly_stats_rows(cls, html):
        tables = cls._fw_extract_tables(html)
        for tbl in tables:
            rows = cls._fw_extract_rows_from_table(tbl)
            cleaned = []
            for cells in rows:
                cleaned_cells = [cls._fw_strip_tags(c).strip() for c in cells]
                cleaned.append(cleaned_cells)
            data_rows = [
                r for r in cleaned
                if len(r) >= 8
                and re.match(r"^\d{4}\s*:\s*[A-Za-z]+\s*$", r[0].strip())
            ]
            if len(data_rows) >= 3:
                return data_rows
        return []


# ---------------------------------------------------------------------------
# Entry point for standalone execution
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch FRED / Fedwire data into MySQL"
    )
    parser.add_argument(
        '--series',
        help='Filter by table name substring (e.g. "claims", "fedwire")',
    )
    args = parser.parse_args()

    FREDFetcher().run(series_filter=args.series)

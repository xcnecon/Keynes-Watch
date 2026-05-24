"""
MOFFetcher -- fetcher for Ministry of Finance land transfer revenue data.

Series:
    mof_land_revenue    国有土地使用权出让收入 (from monthly fiscal reports)

Source: 财政部国库司 https://gks.mof.gov.cn/tongjishuju/
Data:   Monthly cumulative YTD values, published ~17-25 days after period end.
        12 reports per year (Jan-Feb combined, Q1/H1/Q3 use special names).

Requires: pip install requests beautifulsoup4
"""

import os
import re
import sys
import time
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

from bs4 import BeautifulSoup

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from fetch_data.base import BaseFetcher

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MOF_BASE_URL = 'https://gks.mof.gov.cn/tongjishuju/'
MOF_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/131.0.0.0 Safari/537.36'),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
}


class MOFFetcher(BaseFetcher):
    """Fetches MOF land transfer revenue from monthly fiscal reports."""

    SERIES = [
        {
            'table': 'mof_land_revenue',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS mof_land_revenue (
                    period DATE NOT NULL COMMENT '累计期间末日 (e.g. 2025-02-28 = 1-2月)',
                    report_date DATE COMMENT '报告发布日期',
                    gov_fund_revenue DECIMAL(18,2) COMMENT '全国政府性基金预算收入 (亿元)',
                    gov_fund_revenue_yoy DECIMAL(10,2) COMMENT '政府性基金收入同比 (%)',
                    land_revenue DECIMAL(18,2) COMMENT '国有土地使用权出让收入 (亿元)',
                    land_revenue_yoy DECIMAL(10,2) COMMENT '土地出让收入同比 (%)',
                    land_expenditure DECIMAL(18,2) COMMENT '土地出让收入相关支出 (亿元)',
                    land_expenditure_yoy DECIMAL(10,2) COMMENT '土地出让支出同比 (%)',
                    PRIMARY KEY (period)
                )
            """,
            'columns': [
                'period', 'report_date',
                'gov_fund_revenue', 'gov_fund_revenue_yoy',
                'land_revenue', 'land_revenue_yoy',
                'land_expenditure', 'land_expenditure_yoy',
            ],
            'strategy': 'truncate',
            'fetch_func': '_fetch_land_revenue',
        },
    ]

    def __init__(self):
        super().__init__(name='MOF')
        self._setup_cn_proxy()
        self.direct_session = self._create_session()
        self.direct_session.trust_env = False
        self._proxy_failed = False

    def _get(self, url, **kwargs):
        """GET with proxy first, then direct fallback.

        MOF pages are reachable directly from many hosts. A stale CN_PROXY
        should not block this source from updating.
        """
        if self._proxy_failed:
            return self.direct_session.get(url, **kwargs)
        try:
            return self.session.get(url, **kwargs)
        except Exception as exc:
            if not getattr(self, '_cn_proxies', None):
                raise
            self._proxy_failed = True
            self.logger.warning(
                f"  Proxy request failed for {url}: {exc}; retrying direct")
            return self.direct_session.get(url, **kwargs)

    def fetch_series(self, series_config):
        """Fetch and store one MOF series."""
        table = series_config['table']
        fetch_func = getattr(self, series_config['fetch_func'])

        conn = self.get_connection()
        try:
            self.ensure_table(conn, series_config['create_sql'])
            rows = fetch_func()
            self.logger.info(f"  Got {len(rows)} rows for {table}")
            self.truncate_and_insert(
                conn, table, series_config['columns'], rows
            )
        finally:
            if conn.is_connected():
                conn.close()

    # -----------------------------------------------------------------------
    # Listing page scraper
    # -----------------------------------------------------------------------
    def _get_report_links(self):
        """Scrape fiscal report links from all MOF listing pages.

        Auto-detects total page count from pagination on the first page.
        Returns list of (title, url, report_date_str) sorted newest first.
        """
        reports = []
        total_pages = 4  # fallback, updated from first page pagination

        for page_idx in range(100):  # safety cap
            if page_idx > 0 and page_idx >= total_pages:
                break

            suffix = '' if page_idx == 0 else f'index_{page_idx}.htm'
            page_url = MOF_BASE_URL + suffix
            try:
                resp = self.retry_call(
                    lambda u=page_url: self._get(
                        u, headers=MOF_HEADERS, timeout=30),
                    max_retries=3, backoff=3,
                    label=f'listing {suffix or "index"}',
                )
                resp.encoding = 'utf-8'
                if resp.status_code == 404:
                    self.logger.info(f"  Page {page_idx+1} returned 404, stopping")
                    break
                if resp.status_code != 200:
                    self.logger.warning(
                        f"  Listing page returned {resp.status_code}: {page_url}")
                    continue
            except Exception as e:
                self.logger.warning(f"  Failed to fetch listing page: {e}")
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Auto-detect total pages from pagination links on first page
            if page_idx == 0:
                for a in soup.find_all('a', href=True):
                    m = re.search(r'index_(\d+)\.htm', a['href'])
                    if m:
                        total_pages = max(total_pages, int(m.group(1)) + 1)
                self.logger.info(f"  Detected {total_pages} listing pages")

            page_count = 0
            for li in soup.find_all('li'):
                a = li.find('a')
                if not a:
                    continue
                title = (a.get('title', '') or a.get_text(strip=True))
                # Only keep "财政收支情况" reports
                if '财政收支' not in title:
                    continue

                href = a.get('href', '')
                if href.startswith('./'):
                    href = href[2:]
                full_url = MOF_BASE_URL + href

                # Extract publication date from trailing text in <li>
                li_text = li.get_text()
                date_m = re.search(r'(\d{4}-\d{2}-\d{2})', li_text)
                report_date = date_m.group(1) if date_m else None

                reports.append((title, full_url, report_date))
                page_count += 1

            self.logger.info(
                f"  Page {page_idx+1}/{total_pages}: {page_count} fiscal reports")
            self.rate_limit_pause(1.0)

        self.logger.info(f"  Found {len(reports)} fiscal reports on listing pages")
        return reports

    # -----------------------------------------------------------------------
    # Period parsing
    # -----------------------------------------------------------------------
    @staticmethod
    def _parse_period(title):
        """Parse report title → last day of the cumulative period end month.

        Examples:
            "2025年1-2月财政收支情况"   → date(2025, 2, 28)
            "2025年一季度财政收支情况"   → date(2025, 3, 31)
            "2025年1-5月财政收支情况"   → date(2025, 5, 31)
            "2025年上半年财政收支情况"   → date(2025, 6, 30)
            "2025年前三季度财政收支情况" → date(2025, 9, 30)
            "2024年10月财政收支情况"    → date(2024, 10, 31)
            "2025年财政收支情况"        → date(2025, 12, 31)
        """
        # Normalize dashes (em-dash, en-dash → hyphen)
        t = title.replace('\u2014', '-').replace('\u2013', '-')

        year_m = re.search(r'(\d{4})年', t)
        if not year_m:
            return None
        year = int(year_m.group(1))

        # "1-N月" explicit range
        m = re.search(r'1-(\d{1,2})月', t)
        if m:
            end_month = int(m.group(1))
            return date(year, end_month, monthrange(year, end_month)[1])

        # Quarterly / half-year keywords
        if '一季度' in t:
            return date(year, 3, 31)
        if '上半年' in t:
            return date(year, 6, 30)
        if '前三季度' in t:
            return date(year, 9, 30)

        # "N月财政收支" — single month label = cumulative through month N
        m = re.search(r'(\d{1,2})月财政收支', t)
        if m:
            end_month = int(m.group(1))
            return date(year, end_month, monthrange(year, end_month)[1])

        # Full year: "YYYY年财政收支情况" with no month
        if re.search(r'\d{4}年财政收支', t):
            return date(year, 12, 31)

        return None

    # -----------------------------------------------------------------------
    # Report page parser — regex extraction from prose text
    # -----------------------------------------------------------------------
    @staticmethod
    def _extract_amount_yoy(prefix, text):
        """Extract (amount, yoy%) for an indicator by prefix string.

        Handles: 增长/下降 X.X%, and 持平 (0% YoY).
        Returns (None, None) if not found.
        """
        # Standard: 增长/下降 + percentage
        m = re.search(
            prefix + r'(\d+)亿元[，,]\s*(?:同比|比上年)(增长|下降)([\d.]+)%', text)
        if m:
            amount = float(m.group(1))
            yoy = float(m.group(3))
            if m.group(2) == '下降':
                yoy = -yoy
            return amount, yoy
        # Flat: 持平 (0% YoY)
        m = re.search(prefix + r'(\d+)亿元[，,。].{0,20}持平', text)
        if m:
            return float(m.group(1)), 0.0
        return None, None

    def _parse_report(self, html):
        """Extract land revenue data from a fiscal report HTML page.

        Returns dict with keys: gov_fund_revenue, gov_fund_revenue_yoy,
        land_revenue, land_revenue_yoy, land_expenditure, land_expenditure_yoy.
        """
        soup = BeautifulSoup(html, 'html.parser')
        editor = soup.find(class_='TRS_Editor')
        text = editor.get_text() if editor else soup.get_text()

        result = {}
        result['gov_fund_revenue'], result['gov_fund_revenue_yoy'] = \
            self._extract_amount_yoy('全国政府性基金预算收入', text)
        result['land_revenue'], result['land_revenue_yoy'] = \
            self._extract_amount_yoy('国有土地使用权出让收入', text)
        result['land_expenditure'], result['land_expenditure_yoy'] = \
            self._extract_amount_yoy('国有土地使用权出让收入相关支出', text)
        return result

    # -----------------------------------------------------------------------
    # Single report fetch with robust retry (detects 502 in body too)
    # -----------------------------------------------------------------------
    def _fetch_one_report(self, url, period, max_retries=5, backoff=3):
        """Fetch and parse one report page with retry on server errors.

        Returns:
            dict with data if land revenue found,
            'NO_DATA' if page loaded OK but has no land revenue (don't retry),
            None if server error after all retries (retry-able).
        """
        for attempt in range(1, max_retries + 1):
            try:
                resp = self._get(url, headers=MOF_HEADERS, timeout=30)
                resp.encoding = 'utf-8'

                # Server error — retry
                if (resp.status_code in (502, 503, 504)
                        or '502 Bad Gateway' in resp.text[:500]):
                    self.logger.warning(
                        f"  {period} attempt {attempt}/{max_retries}: 502")
                    if attempt < max_retries:
                        time.sleep(backoff * attempt)
                    continue

                if resp.status_code != 200:
                    self.logger.warning(
                        f"  {period}: HTTP {resp.status_code}, skipping")
                    return 'NO_DATA'

                # Page loaded OK — parse
                data = self._parse_report(resp.text)
                if data.get('land_revenue'):
                    return data

                # Page loaded but no land revenue — report genuinely
                # doesn't contain this data, do NOT retry
                self.logger.info(
                    f"  {period}: report loaded OK but no land revenue data")
                return 'NO_DATA'

            except Exception as e:
                self.logger.warning(
                    f"  {period} attempt {attempt}/{max_retries}: {e}")
                if attempt < max_retries:
                    time.sleep(backoff * attempt)

        return None

    # -----------------------------------------------------------------------
    # Main fetch
    # -----------------------------------------------------------------------
    def _fetch_land_revenue(self):
        """Scrape all fiscal reports and extract land revenue data.

        Two-pass approach: first pass scrapes all, second pass retries failures.
        Returns list of tuples matching SERIES columns order.
        """
        reports = self._get_report_links()
        if not reports:
            raise RuntimeError("No MOF fiscal report links found")

        rows = []
        failed = []

        # --- Pass 1: parallel fetch with 5 threads ---
        valid_reports = []
        for title, url, report_date_str in reports:
            period = self._parse_period(title)
            if not period:
                self.logger.warning(f"  Skipping (cannot parse period): {title}")
                continue
            valid_reports.append((title, url, report_date_str, period))

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self._fetch_one_report, url, period): (title, url, report_date_str, period)
                for title, url, report_date_str, period in valid_reports
            }
            for future in as_completed(futures):
                title, url, report_date_str, period = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    self.logger.error(f"  {period} FAILED: {e}")
                    failed.append((title, url, report_date_str, period))
                    continue
                if isinstance(result, dict):
                    rows.append((
                        period, report_date_str,
                        result.get('gov_fund_revenue'), result.get('gov_fund_revenue_yoy'),
                        result.get('land_revenue'), result.get('land_revenue_yoy'),
                        result.get('land_expenditure'), result.get('land_expenditure_yoy'),
                    ))
                    self.logger.info(
                        f"  {period} | land={result['land_revenue']}亿元 "
                        f"yoy={result.get('land_revenue_yoy')}%")
                elif result is None:
                    # Server error — worth retrying later
                    failed.append((title, url, report_date_str, period))

        # --- Pass 2: retry server-error failures only ---
        if failed:
            self.logger.info(
                f"  Pass 2: retrying {len(failed)} server-error reports...")
            time.sleep(5)

            unrecovered = []
            for title, url, report_date_str, period in failed:
                self.logger.info(f"  Retrying: {period}")
                try:
                    result = self._fetch_one_report(
                        url, period, max_retries=10, backoff=5)
                except Exception as e:
                    self.logger.error(f"  {period} | retry raised: {e}")
                    unrecovered.append(period)
                    self.rate_limit_pause(1.0)
                    continue

                if isinstance(result, dict):
                    rows.append((
                        period, report_date_str,
                        result.get('gov_fund_revenue'),
                        result.get('gov_fund_revenue_yoy'),
                        result.get('land_revenue'),
                        result.get('land_revenue_yoy'),
                        result.get('land_expenditure'),
                        result.get('land_expenditure_yoy'),
                    ))
                    self.logger.info(
                        f"  {period} | RECOVERED land={result['land_revenue']}亿元")
                else:
                    self.logger.error(
                        f"  {period} | FAILED after all retries")
                    if result is None:
                        unrecovered.append(period)
                self.rate_limit_pause(1.0)

            if unrecovered:
                periods = ', '.join(str(p) for p in sorted(unrecovered))
                raise RuntimeError(
                    "MOF land revenue refresh is incomplete; refusing to "
                    f"replace existing table. Unrecovered periods: {periods}"
                )

        rows.sort(key=lambda r: r[0])
        if not rows:
            raise RuntimeError("No MOF land revenue rows parsed")
        return rows


if __name__ == '__main__':
    fetcher = MOFFetcher()
    fetcher.run()

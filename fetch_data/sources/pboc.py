"""
PBOCFetcher -- fetcher for People's Bank of China monetary data.

Series:
    pboc_lpr              LPR (Loan Prime Rate) — 1Y and 5Y+
    pboc_money_supply     M0 / M1 / M2 money supply
    pboc_social_financing_stock Social financing scale stock
    pboc_social_financing Social financing scale (社会融资规模增量)
    pboc_new_credit       New RMB loans (新增人民币贷款)
    pboc_rrr              Reserve requirement ratio (存款准备金率)

Requires: pip install akshare
"""

import os
import re
import sys
import math
import threading
from datetime import datetime

import akshare as ak
import pandas as pd
from akshare.utils import demjson

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from fetch_data.base import BaseFetcher


class PBOCFetcher(BaseFetcher):
    """Fetches PBOC monetary policy data from AKShare."""

    MAX_WORKERS = 5
    SINA_MACRO_URL = (
        'https://quotes.sina.cn/mac/api/jsonp_v3.php/'
        'SINAREMOTECALLCALLBACK1601651495761/MacPage_Service.get_pagedata'
    )
    _SINA_MACRO_LOCK = threading.Lock()

    def __init__(self):
        super().__init__()
        self._setup_cn_proxy()

    SERIES = [
        # ---- LPR (truncate + insert, full refresh) ----
        {
            'table': 'pboc_lpr',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS pboc_lpr (
                    trade_date DATE NOT NULL,
                    lpr_1y DECIMAL(10,4),
                    lpr_5y DECIMAL(10,4),
                    rate_1 DECIMAL(10,4),
                    rate_2 DECIMAL(10,4),
                    PRIMARY KEY (trade_date)
                )
            """,
            'columns': ['trade_date', 'lpr_1y', 'lpr_5y', 'rate_1', 'rate_2'],
            'strategy': 'truncate',
            'fetch_func': '_fetch_lpr',
        },
        # ---- Money Supply M0/M1/M2 (truncate + insert) ----
        {
            'table': 'pboc_money_supply',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS pboc_money_supply (
                    period VARCHAR(10) NOT NULL,
                    m2 DECIMAL(18,2),
                    m2_yoy DECIMAL(10,2),
                    m1 DECIMAL(18,2),
                    m1_yoy DECIMAL(10,2),
                    m0 DECIMAL(18,2),
                    m0_yoy DECIMAL(10,2),
                    PRIMARY KEY (period)
                )
            """,
            'columns': ['period', 'm2', 'm2_yoy', 'm1', 'm1_yoy', 'm0', 'm0_yoy'],
            'strategy': 'truncate',
            'fetch_func': '_fetch_money_supply',
        },
        # ---- Social Financing (truncate + insert) ----
        {
            'table': 'pboc_social_financing',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS pboc_social_financing (
                    period VARCHAR(10) NOT NULL,
                    total DECIMAL(18,2),
                    rmb_loans DECIMAL(18,2),
                    fx_loans DECIMAL(18,2),
                    entrusted_loans DECIMAL(18,2),
                    trust_loans DECIMAL(18,2),
                    undiscounted_bankers DECIMAL(18,2),
                    corporate_bonds DECIMAL(18,2),
                    equity_financing DECIMAL(18,2),
                    PRIMARY KEY (period)
                )
            """,
            'columns': ['period', 'total', 'rmb_loans', 'fx_loans',
                        'entrusted_loans', 'trust_loans', 'undiscounted_bankers',
                        'corporate_bonds', 'equity_financing'],
            'strategy': 'truncate',
            'fetch_func': '_fetch_social_financing',
        },
        # ---- Social Financing Stock (truncate + insert) ----
        {
            'table': 'pboc_social_financing_stock',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS pboc_social_financing_stock (
                    period VARCHAR(10) NOT NULL,
                    total DECIMAL(18,2),
                    total_yoy DECIMAL(10,4),
                    rmb_loans DECIMAL(18,2),
                    rmb_loans_yoy DECIMAL(10,4),
                    fx_loans DECIMAL(18,2),
                    fx_loans_yoy DECIMAL(10,4),
                    entrusted_loans DECIMAL(18,2),
                    entrusted_loans_yoy DECIMAL(10,4),
                    trust_loans DECIMAL(18,2),
                    trust_loans_yoy DECIMAL(10,4),
                    undiscounted_bankers DECIMAL(18,2),
                    undiscounted_bankers_yoy DECIMAL(10,4),
                    corporate_bonds DECIMAL(18,2),
                    corporate_bonds_yoy DECIMAL(10,4),
                    govt_bonds DECIMAL(18,2),
                    govt_bonds_yoy DECIMAL(10,4),
                    equity_financing DECIMAL(18,2),
                    equity_financing_yoy DECIMAL(10,4),
                    PRIMARY KEY (period)
                )
            """,
            'columns': ['period', 'total', 'total_yoy',
                        'rmb_loans', 'rmb_loans_yoy',
                        'fx_loans', 'fx_loans_yoy',
                        'entrusted_loans', 'entrusted_loans_yoy',
                        'trust_loans', 'trust_loans_yoy',
                        'undiscounted_bankers', 'undiscounted_bankers_yoy',
                        'corporate_bonds', 'corporate_bonds_yoy',
                        'govt_bonds', 'govt_bonds_yoy',
                        'equity_financing', 'equity_financing_yoy'],
            'strategy': 'truncate',
            'fetch_func': '_fetch_social_financing_stock',
        },
        # ---- New Credit / New RMB Loans (truncate + insert) ----
        {
            'table': 'pboc_new_credit',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS pboc_new_credit (
                    period VARCHAR(20) NOT NULL,
                    monthly DECIMAL(18,2),
                    monthly_yoy DECIMAL(10,4),
                    monthly_mom DECIMAL(10,4),
                    cumulative DECIMAL(18,2),
                    cumulative_yoy DECIMAL(10,4),
                    PRIMARY KEY (period)
                )
            """,
            'columns': ['period', 'monthly', 'monthly_yoy', 'monthly_mom',
                        'cumulative', 'cumulative_yoy'],
            'strategy': 'truncate',
            'fetch_func': '_fetch_new_credit',
        },
        # ---- Reserve Requirement Ratio (truncate + insert) ----
        {
            'table': 'pboc_rrr',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS pboc_rrr (
                    announce_date DATE NOT NULL,
                    effective_date DATE NOT NULL,
                    large_before DECIMAL(10,2),
                    large_after DECIMAL(10,2),
                    large_change DECIMAL(10,2),
                    small_before DECIMAL(10,2),
                    small_after DECIMAL(10,2),
                    small_change DECIMAL(10,2),
                    PRIMARY KEY (announce_date, effective_date)
                )
            """,
            'columns': ['announce_date', 'effective_date',
                        'large_before', 'large_after', 'large_change',
                        'small_before', 'small_after', 'small_change'],
            'strategy': 'truncate',
            'fetch_func': '_fetch_rrr',
        },
        # ---- SHIBOR (truncate + insert) ----
        {
            'table': 'pboc_shibor',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS pboc_shibor (
                    trade_date DATE NOT NULL,
                    on_rate DECIMAL(10,4),
                    on_change DECIMAL(10,4),
                    1w_rate DECIMAL(10,4),
                    1w_change DECIMAL(10,4),
                    2w_rate DECIMAL(10,4),
                    2w_change DECIMAL(10,4),
                    1m_rate DECIMAL(10,4),
                    1m_change DECIMAL(10,4),
                    3m_rate DECIMAL(10,4),
                    3m_change DECIMAL(10,4),
                    6m_rate DECIMAL(10,4),
                    6m_change DECIMAL(10,4),
                    9m_rate DECIMAL(10,4),
                    9m_change DECIMAL(10,4),
                    1y_rate DECIMAL(10,4),
                    1y_change DECIMAL(10,4),
                    PRIMARY KEY (trade_date)
                )
            """,
            'columns': ['trade_date',
                        'on_rate', 'on_change', '1w_rate', '1w_change',
                        '2w_rate', '2w_change', '1m_rate', '1m_change',
                        '3m_rate', '3m_change', '6m_rate', '6m_change',
                        '9m_rate', '9m_change', '1y_rate', '1y_change'],
            'strategy': 'truncate',
            'fetch_func': '_fetch_shibor',
        },
        # ---- Policy Rates: SLF 7-day (corridor ceiling) + IOER (corridor floor) ----
        {
            'table': 'pboc_policy_rates',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS pboc_policy_rates (
                    effective_date DATE NOT NULL,
                    rate_type VARCHAR(20) NOT NULL,
                    rate DECIMAL(10,4),
                    PRIMARY KEY (effective_date, rate_type)
                )
            """,
            'columns': ['effective_date', 'rate_type', 'rate'],
            'strategy': 'truncate',
            'fetch_func': '_fetch_policy_rates',
        },
        # ---- PBOC Balance Sheet (truncate + insert) ----
        {
            'table': 'pboc_balance_sheet',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS pboc_balance_sheet (
                    period VARCHAR(10) NOT NULL,
                    total_assets DECIMAL(18,2),
                    foreign_assets DECIMAL(18,2),
                    fx_reserves DECIMAL(18,2),
                    monetary_gold DECIMAL(18,2),
                    claims_on_govt DECIMAL(18,2),
                    claims_on_other_depository DECIMAL(18,2),
                    claims_on_other_financial DECIMAL(18,2),
                    other_assets DECIMAL(18,2),
                    total_liabilities DECIMAL(18,2),
                    reserve_money DECIMAL(18,2),
                    currency_in_circulation DECIMAL(18,2),
                    deposits_of_financial DECIMAL(18,2),
                    bond_issuance DECIMAL(18,2),
                    govt_deposits DECIMAL(18,2),
                    own_equity DECIMAL(18,2),
                    other_liabilities DECIMAL(18,2),
                    PRIMARY KEY (period)
                )
            """,
            'columns': ['period', 'total_assets', 'foreign_assets', 'fx_reserves',
                        'monetary_gold', 'claims_on_govt',
                        'claims_on_other_depository', 'claims_on_other_financial',
                        'other_assets', 'total_liabilities', 'reserve_money',
                        'currency_in_circulation', 'deposits_of_financial',
                        'bond_issuance', 'govt_deposits', 'own_equity',
                        'other_liabilities'],
            'strategy': 'truncate',
            'fetch_func': '_fetch_balance_sheet',
        },
        # ---- OMO: Open Market Operations / 7-day Reverse Repo (incremental) ----
        {
            'table': 'pboc_omo',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS pboc_omo (
                    trade_date DATE NOT NULL,
                    tenor INT NOT NULL,
                    rate DECIMAL(10,4),
                    volume DECIMAL(18,2),
                    PRIMARY KEY (trade_date, tenor)
                )
            """,
            'columns': ['trade_date', 'tenor', 'rate', 'volume'],
            'strategy': 'incremental',
            'date_column': 'trade_date',
            'fetch_func': '_fetch_omo',
        },
    ]

    # -- AKShare fetch functions ------------------------------------------------

    def _fetch_lpr(self):
        """Fetch LPR data via ak.macro_china_lpr()."""
        df = self.retry_call(ak.macro_china_lpr, label='macro_china_lpr')
        rows = []
        for _, r in df.iterrows():
            trade_date = pd.to_datetime(r.get('TRADE_DATE')).date() if pd.notna(r.get('TRADE_DATE')) else None
            if trade_date is None:
                continue
            rows.append((
                trade_date,
                self._to_float(r.get('LPR1Y')),
                self._to_float(r.get('LPR5Y')),
                self._to_float(r.get('RATE_1')),
                self._to_float(r.get('RATE_2')),
            ))
        return rows

    def _fetch_money_supply(self):
        """Fetch M0/M1/M2 from Sina's macro endpoint."""
        df = self._fetch_sina_macro_df(event='1', label='macro_china_supply_of_money')
        rows = []
        cols = df.columns.tolist()
        for _, r in df.iterrows():
            period = str(r.iloc[0]) if len(cols) > 0 else None
            if not period:
                continue
            rows.append((
                period,
                self._to_float(r.iloc[1] if len(cols) > 1 else None),   # M2
                self._to_float(r.iloc[2] if len(cols) > 2 else None),   # M2 YoY
                self._to_float(r.iloc[3] if len(cols) > 3 else None),   # M1
                self._to_float(r.iloc[4] if len(cols) > 4 else None),   # M1 YoY
                self._to_float(r.iloc[5] if len(cols) > 5 else None),   # M0
                self._to_float(r.iloc[6] if len(cols) > 6 else None),   # M0 YoY
            ))
        return rows

    def _fetch_social_financing(self):
        """Fetch social financing via AKShare, patched with PBOC official reports."""
        df = self.retry_call(ak.macro_china_shrzgm, label='macro_china_shrzgm')
        rows = []
        cols = df.columns.tolist()
        for _, r in df.iterrows():
            period = str(r.iloc[0]) if len(cols) > 0 else None
            if not period:
                continue
            rows.append((
                period,
                self._to_float(r.iloc[1] if len(cols) > 1 else None),   # total
                self._to_float(r.iloc[2] if len(cols) > 2 else None),   # rmb_loans
                self._to_float(r.iloc[3] if len(cols) > 3 else None),   # fx_loans
                self._to_float(r.iloc[4] if len(cols) > 4 else None),   # entrusted_loans
                self._to_float(r.iloc[5] if len(cols) > 5 else None),   # trust_loans
                self._to_float(r.iloc[6] if len(cols) > 6 else None),   # undiscounted_bankers
                self._to_float(r.iloc[7] if len(cols) > 7 else None),   # corporate_bonds
                self._to_float(r.iloc[8] if len(cols) > 8 else None),   # equity_financing
            ))
        latest_period = max((str(row[0]) for row in rows), default=None)
        pboc_rows = self._fetch_social_financing_from_pboc(min_period=latest_period)
        if pboc_rows:
            merged = {str(row[0]): row for row in rows}
            for row in pboc_rows:
                merged[str(row[0])] = row
            rows = [merged[period] for period in sorted(merged)]
            self.logger.info(
                f"  Added/updated {len(pboc_rows)} PBOC official social financing rows"
            )
        return rows

    def _fetch_social_financing_stock(self):
        """Fetch social financing stock data from PBOC official monthly reports."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            self.logger.warning('  BeautifulSoup not available; skipping PBOC social financing stock')
            return []

        pboc_base = 'https://www.pbc.gov.cn'
        stock_marker = '\u793e\u4f1a\u878d\u8d44\u89c4\u6a21\u5b58\u91cf\u7edf\u8ba1\u6570\u636e\u62a5\u544a'
        financial_marker = '\u91d1\u878d\u7edf\u8ba1\u6570\u636e\u62a5\u544a'
        stock_links = {}
        financial_links = {}

        for page_num in range(1, 81):
            if page_num == 1:
                list_url = f'{pboc_base}/goutongjiaoliu/113456/113469/index.html'
            else:
                list_url = f'{pboc_base}/goutongjiaoliu/113456/113469/11040-{page_num}.html'

            try:
                html = self._get_pboc_html(list_url)
            except Exception as e:
                self.logger.warning(f'  PBOC stock list page {page_num} failed: {e}')
                break

            soup = BeautifulSoup(html, 'html.parser')
            links = soup.select('#r_con a[title]')
            if not links:
                break

            found_on_page = False
            for link in links:
                title = link.get('title', '')
                is_stock_report = stock_marker in title
                is_financial_report = financial_marker in title
                if not is_stock_report and not is_financial_report:
                    continue
                report_period = self._pboc_report_period_from_title(title)
                if not report_period:
                    continue
                period, _month = report_period
                href = link.get('href', '')
                url = href if href.startswith('http') else f'{pboc_base}{href}'
                if is_stock_report:
                    stock_links.setdefault(period, url)
                else:
                    financial_links.setdefault(period, url)
                found_on_page = True

            if found_on_page:
                self.rate_limit_pause(0.1)

        report_links = dict(stock_links)
        latest_stock_period = max(stock_links, default=None)
        for period, url in financial_links.items():
            if latest_stock_period is None or period > latest_stock_period:
                report_links.setdefault(period, url)

        rows = []
        for period, url in sorted(report_links.items()):
            try:
                html = self._get_pboc_html(url)
                soup = BeautifulSoup(html, 'html.parser')
                content = (soup.find('div', id='zoom')
                           or soup.find('div', id='ewebeditor_content'))
                text = content.get_text('', strip=True) if content else soup.get_text('', strip=True)
                values = self._parse_pboc_social_financing_stock(text)
            except Exception as e:
                self.logger.warning(f'  PBOC social financing stock failed ({period}): {e}')
                continue

            if values:
                rows.append((period, *values))
            else:
                self.logger.warning(f'  PBOC social financing stock missing values ({period})')

            self.rate_limit_pause(0.1)

        return rows

    def _fetch_social_financing_from_pboc(self, min_period=None):
        """Fetch newer social financing flow data from PBOC financial reports.

        PBOC's monthly "financial statistics report" publishes year-to-date
        social financing flow details. Convert those cumulative values to
        monthly flows by differencing adjacent reports from the same year.
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            self.logger.warning('  BeautifulSoup not available; skipping PBOC social financing fallback')
            return []

        pboc_base = 'https://www.pbc.gov.cn'
        report_marker = '\u91d1\u878d\u7edf\u8ba1\u6570\u636e\u62a5\u544a'
        report_links = {}
        stop_scan = False

        for page_num in range(1, 31):
            if page_num == 1:
                list_url = f'{pboc_base}/goutongjiaoliu/113456/113469/index.html'
            else:
                list_url = f'{pboc_base}/goutongjiaoliu/113456/113469/11040-{page_num}.html'

            try:
                html = self._get_pboc_html(list_url)
            except Exception as e:
                self.logger.warning(f'  PBOC social financing list page {page_num} failed: {e}')
                break

            soup = BeautifulSoup(html, 'html.parser')
            links = soup.select('#r_con a[title]')
            if not links:
                break

            for link in links:
                title = link.get('title', '')
                if report_marker not in title:
                    continue
                report_period = self._pboc_report_period_from_title(title)
                if not report_period:
                    continue
                period, month = report_period
                href = link.get('href', '')
                url = href if href.startswith('http') else f'{pboc_base}{href}'
                report_links[period] = (month, url)
                if min_period and period <= min_period:
                    stop_scan = True

            if stop_scan:
                break

            self.rate_limit_pause(0.1)

        if not report_links:
            return []

        cumulative = {}
        for period, (month, url) in sorted(report_links.items()):
            try:
                html = self._get_pboc_html(url)
                soup = BeautifulSoup(html, 'html.parser')
                content = (soup.find('div', id='zoom')
                           or soup.find('div', id='ewebeditor_content'))
                text = content.get_text('', strip=True) if content else soup.get_text('', strip=True)
                values = self._parse_pboc_social_financing_cumulative(text)
            except Exception as e:
                self.logger.warning(f'  PBOC social financing report failed ({period}): {e}')
                continue

            if values:
                cumulative[period] = (month, values)
            else:
                self.logger.warning(f'  PBOC social financing report missing values ({period})')

            self.rate_limit_pause(0.1)

        rows = []
        previous_by_year = {}
        for period, (month, values) in sorted(cumulative.items()):
            year = period[:4]
            previous = previous_by_year.get(year)

            if month == 1:
                monthly_values = values
            elif previous and previous[0] == month - 1:
                monthly_values = tuple(
                    round(current - prior, 4)
                    for current, prior in zip(values, previous[1])
                )
            else:
                previous_by_year[year] = (month, values)
                continue

            previous_by_year[year] = (month, values)
            if min_period and period <= min_period:
                continue
            rows.append((period, *monthly_values))

        return rows

    def _get_pboc_html(self, url):
        headers = {'User-Agent': 'Mozilla/5.0'}
        if not hasattr(self, '_pboc_direct_session'):
            self._pboc_direct_session = self._create_session()
            self._pboc_direct_session.trust_env = False
            self._pboc_direct_session.proxies = {}
        try:
            resp = self._pboc_direct_session.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
        except Exception:
            resp = self.session.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
        resp.encoding = 'utf-8'
        return resp.text

    @staticmethod
    def _pboc_report_period_from_title(title):
        year_match = re.search(r'(\d{4})\u5e74', title)
        if not year_match:
            return None

        year = int(year_match.group(1))
        month_match = re.search(r'\u5e74(\d{1,2})\u6708', title)
        if month_match:
            month = int(month_match.group(1))
        elif '\u4e00\u5b63\u5ea6' in title:
            month = 3
        elif '\u4e0a\u534a\u5e74' in title:
            month = 6
        elif '\u524d\u4e09\u5b63\u5ea6' in title:
            month = 9
        else:
            month = 12

        if month < 1 or month > 12:
            return None
        return f'{year}{month:02d}', month

    @classmethod
    def _parse_pboc_social_financing_cumulative(cls, text):
        text = re.sub(r'\s+', '', str(text))

        yi = '\u4ebf\u5143'
        wan_yi = '\u4e07\u4ebf\u5143'
        inc = '\u589e\u52a0'
        dec = '\u51cf\u5c11'
        wei = '\u4e3a'
        amount_unit = rf'(-?\d+(?:\.\d+)?)({wan_yi}|{yi})'

        social_flow = '\u793e\u4f1a\u878d\u8d44\u89c4\u6a21\u589e\u91cf'
        total_match = re.search(rf'{social_flow}.{{0,20}}?{wei}{amount_unit}', text)
        if not total_match:
            return None

        terms = [
            '\u5bf9\u5b9e\u4f53\u7ecf\u6d4e\u53d1\u653e\u7684\u4eba\u6c11\u5e01\u8d37\u6b3e',
            '\u5bf9\u5b9e\u4f53\u7ecf\u6d4e\u53d1\u653e\u7684\u5916\u5e01\u8d37\u6b3e\u6298\u5408\u4eba\u6c11\u5e01',
            '\u59d4\u6258\u8d37\u6b3e',
            '\u4fe1\u6258\u8d37\u6b3e',
            '\u672a\u8d34\u73b0\u7684?\u94f6\u884c\u627f\u5151\u6c47\u7968',
            '\u4f01\u4e1a\u503a\u5238\u51c0\u878d\u8d44',
            '\u975e\u91d1\u878d\u4f01\u4e1a\u5883\u5185\u80a1\u7968\u878d\u8d44',
        ]

        values = [cls._pboc_amount_to_100m(total_match.group(1), total_match.group(2))]
        for term in terms:
            match = re.search(rf'{term}({inc}|{dec})?{amount_unit}', text)
            if not match:
                return None
            value = cls._pboc_amount_to_100m(match.group(2), match.group(3))
            if match.group(1) == dec:
                value = -value
            values.append(value)

        return tuple(values)

    @classmethod
    def _parse_pboc_social_financing_stock(cls, text):
        text = re.sub(r'\s+', '', str(text))

        amount_unit = r'(-?\d+(?:\.\d+)?)(\u4e07\u4ebf\u5143|\u4ebf\u5143)'
        yoy_part = (
            r'\u540c\u6bd4(?:'
            r'(\u589e\u957f|\u4e0b\u964d)(-?\d+(?:\.\d+)?)%|'
            r'(\u6301\u5e73)'
            r')'
        )

        total_term = '\u793e\u4f1a\u878d\u8d44\u89c4\u6a21\u5b58\u91cf'
        terms = [
            '\u5bf9\u5b9e\u4f53\u7ecf\u6d4e\u53d1\u653e\u7684\u4eba\u6c11\u5e01\u8d37\u6b3e\u4f59\u989d',
            '\u5bf9\u5b9e\u4f53\u7ecf\u6d4e\u53d1\u653e\u7684\u5916\u5e01\u8d37\u6b3e\u6298\u5408\u4eba\u6c11\u5e01\u4f59\u989d',
            '\u59d4\u6258\u8d37\u6b3e\u4f59\u989d',
            '\u4fe1\u6258\u8d37\u6b3e\u4f59\u989d',
            '\u672a\u8d34\u73b0\u7684?\u94f6\u884c\u627f\u5151\u6c47\u7968\u4f59\u989d',
            '\u4f01\u4e1a\u503a\u5238\u4f59\u989d',
            '\u653f\u5e9c\u503a\u5238\u4f59\u989d',
            '\u975e\u91d1\u878d\u4f01\u4e1a\u5883\u5185\u80a1\u7968\u4f59\u989d',
        ]

        def parse_term(term):
            match = re.search(
                rf'{term}.{{0,15}}?(?:\u4e3a)?{amount_unit}.{{0,15}}?{yoy_part}',
                text,
            )
            if not match:
                return None, None
            amount = cls._pboc_amount_to_100m(match.group(1), match.group(2))
            if match.group(5):
                yoy = 0.0
            else:
                yoy = float(match.group(4))
                if match.group(3) == '\u4e0b\u964d':
                    yoy = -abs(yoy)
            return amount, yoy

        total, total_yoy = parse_term(total_term)
        if total is None:
            return None

        values = [total, total_yoy]
        for term in terms:
            amount, yoy = parse_term(term)
            values.extend([amount, yoy])

        return tuple(values)

    @staticmethod
    def _pboc_amount_to_100m(amount, unit):
        value = float(amount)
        if unit == '\u4e07\u4ebf\u5143':
            value *= 10000
        return value

    def _fetch_new_credit(self):
        """Fetch new RMB loans via ak.macro_china_new_financial_credit()."""
        df = self.retry_call(ak.macro_china_new_financial_credit, label='macro_china_new_financial_credit')
        rows = []
        cols = df.columns.tolist()
        for _, r in df.iterrows():
            period = str(r.iloc[0]) if len(cols) > 0 else None
            if not period:
                continue
            rows.append((
                period,
                self._to_float(r.iloc[1] if len(cols) > 1 else None),   # monthly
                self._to_float(r.iloc[2] if len(cols) > 2 else None),   # monthly_yoy
                self._to_float(r.iloc[3] if len(cols) > 3 else None),   # monthly_mom
                self._to_float(r.iloc[4] if len(cols) > 4 else None),   # cumulative
                self._to_float(r.iloc[5] if len(cols) > 5 else None),   # cumulative_yoy
            ))
        return rows

    def _fetch_rrr(self):
        """Fetch reserve requirement ratio via ak.macro_china_reserve_requirement_ratio()."""
        df = self.retry_call(ak.macro_china_reserve_requirement_ratio, label='macro_china_rrr')
        rows = []
        cols = df.columns.tolist()
        for _, r in df.iterrows():
            announce_date = self._parse_cn_date(r.iloc[0]) if len(cols) > 0 else None
            effective_date = self._parse_cn_date(r.iloc[1]) if len(cols) > 1 else None
            if announce_date is None:
                continue
            rows.append((
                announce_date,
                effective_date,
                self._to_float(r.iloc[2] if len(cols) > 2 else None),   # large_before
                self._to_float(r.iloc[3] if len(cols) > 3 else None),   # large_after
                self._to_float(r.iloc[4] if len(cols) > 4 else None),   # large_change
                self._to_float(r.iloc[5] if len(cols) > 5 else None),   # small_before
                self._to_float(r.iloc[6] if len(cols) > 6 else None),   # small_after
                self._to_float(r.iloc[7] if len(cols) > 7 else None),   # small_change
            ))
        return rows

    # -- Helpers ----------------------------------------------------------------

    def _fetch_policy_rates(self):
        """Seed SLF 7-day and IOER historical adjustments (manual, rarely changed).
        Source: PBOC official rate tables. Update this list when PBOC announces changes.
        """
        # SLF 7-day rate history (corridor ceiling)
        # Source: http://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125440/125838/125885/125896/
        slf_7d = [
            ('2015-11-20', 'SLF_7D', 3.25),
            ('2017-02-03', 'SLF_7D', 3.35),
            ('2017-03-16', 'SLF_7D', 3.45),
            ('2017-12-14', 'SLF_7D', 3.50),
            ('2018-03-22', 'SLF_7D', 3.55),
            ('2019-11-18', 'SLF_7D', 3.50),
            ('2020-02-03', 'SLF_7D', 3.35),
            ('2020-04-10', 'SLF_7D', 3.20),
            ('2022-01-17', 'SLF_7D', 3.10),
            ('2022-08-15', 'SLF_7D', 3.00),
            ('2023-06-13', 'SLF_7D', 2.90),
            ('2023-08-15', 'SLF_7D', 2.80),
            ('2024-07-22', 'SLF_7D', 2.70),
            ('2024-09-27', 'SLF_7D', 2.50),
            ('2025-05-08', 'SLF_7D', 2.40),
        ]
        # IOER (Interest on Excess Reserves) history (corridor floor)
        ioer = [
            ('2002-02-21', 'IOER', 1.89),
            ('2003-12-21', 'IOER', 1.62),
            ('2005-03-17', 'IOER', 0.99),
            ('2008-11-27', 'IOER', 0.72),
            ('2020-04-07', 'IOER', 0.35),
        ]
        rows = []
        for d, t, r in slf_7d + ioer:
            rows.append((d, t, r))
        return rows

    def _fetch_omo(self, latest_date=None, _conn=None):
        """Fetch PBOC OMO (reverse repo) data from two sources:
        1. PBOC official website (pbc.gov.cn) — always up-to-date
        2. ChinaMoney notice API (channelId=2845) — historical backfill

        Supports two announcement formats:
          - New (2023.07+): 期限 | 操作利率 | 投标量 | 中标量
          - Old (pre-2023.07): 期限 | 中标量 | 中标利率

        When _conn is provided, rows are saved to DB per-page (for long backfills).
        """
        import re
        from bs4 import BeautifulSoup

        columns = ['trade_date', 'tenor', 'rate', 'volume']
        total_saved = 0
        latest_str = str(latest_date) if latest_date else None

        def _parse_omo_html(soup):
            """Parse an OMO announcement HTML, return list of (date, tenor, rate, vol)."""
            content = (soup.find('div', id='ewebeditor_content')
                       or soup.find('div', id='zoom'))
            if not content:
                return []
            text = content.get_text()
            if '逆回购' not in text:
                return []
            date_m = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', text)
            if not date_m:
                return []
            trade_date = (f'{date_m.group(1)}-'
                          f'{date_m.group(2).zfill(2)}-'
                          f'{date_m.group(3).zfill(2)}')
            table = content.find('table')
            if not table:
                return []
            trs = table.find_all('tr')
            if len(trs) < 2:
                return []
            header_cells = [c.get_text(strip=True) for c in trs[0].find_all(['td', 'th'])]
            new_fmt = '操作利率' in header_cells
            results = []
            for tr in trs[1:]:
                cells = [c.get_text(strip=True) for c in tr.find_all('td')]
                if len(cells) < 3:
                    continue
                tenor_m = re.search(r'(\d+)', cells[0])
                if not tenor_m:
                    continue
                tenor = int(tenor_m.group(1))
                if new_fmt:
                    rate_m = re.search(r'(\d+\.?\d*)', cells[1])
                    vol_m = re.search(r'(\d+\.?\d*)',
                                      cells[3] if len(cells) > 3 else cells[2])
                else:
                    vol_m = re.search(r'(\d+\.?\d*)', cells[1])
                    rate_m = re.search(r'(\d+\.?\d*)', cells[2])
                rate = float(rate_m.group(1)) if rate_m else None
                volume = float(vol_m.group(1)) if vol_m else None
                # Sanity: swap if rate/volume look reversed (old format variants)
                if rate is not None and volume is not None and rate > 10 and volume < 10:
                    rate, volume = volume, rate
                if rate is not None:
                    results.append((trade_date, tenor, rate, volume))
            return results

        def _save_batch(rows, source_label, page_num):
            nonlocal total_saved
            if rows and _conn:
                self.insert_ignore_rows(_conn, 'pboc_omo', columns, rows)
                total_saved += len(rows)
                date_range = f'{rows[-1][0]}~{rows[0][0]}'
                self.logger.info(f'  [{source_label}] page {page_num}: '
                                 f'+{len(rows)} rows ({date_range}), total={total_saved}')
            elif rows:
                total_saved += len(rows)

        # ── Source 1: PBOC official website (pbc.gov.cn) ──────────────
        self.logger.info('  Fetching from PBOC official website...')
        pboc_base = 'https://www.pbc.gov.cn'
        list_path = '/zhengcehuobisi/125207/125213/125431/125475/'
        pboc_page = 1
        pboc_stop = False

        while not pboc_stop:
            if pboc_page == 1:
                list_url = f'{pboc_base}{list_path}index.html'
            else:
                list_url = f'{pboc_base}{list_path}17081-{pboc_page}.html'

            try:
                html = self._get_pboc_html(list_url)
            except Exception as e:
                self.logger.warning(f'  PBOC list page {pboc_page} failed: {e}')
                break

            soup = BeautifulSoup(html, 'html.parser')
            links = soup.find_all('a', href=re.compile(r'/125475/\d+/index'))
            date_spans = soup.find_all('span', class_='hui12')

            if not links:
                break

            page_rows = []
            for i, link in enumerate(links):
                # Get date from corresponding span
                ann_date = date_spans[i].get_text(strip=True) if i < len(date_spans) else ''
                if latest_str and ann_date and ann_date <= latest_str:
                    pboc_stop = True
                    break

                href = link.get('href', '')
                ann_url = f'{pboc_base}{href}'
                try:
                    ann_html = self._get_pboc_html(ann_url)
                    parsed = _parse_omo_html(BeautifulSoup(ann_html, 'html.parser'))
                    page_rows.extend(parsed)
                    self.rate_limit_pause(0.2)
                except Exception as e:
                    self.logger.warning(f'  Failed to parse {ann_url}: {e}')

            _save_batch(page_rows, 'PBOC', pboc_page)
            pboc_page += 1
            self.rate_limit_pause(0.3)

        # ── Source 2: ChinaMoney API (historical backfill) ────────────
        # Check earliest date in DB to decide if backfill is needed
        earliest_date = None
        if _conn:
            cursor = _conn.cursor()
            try:
                cursor.execute('SELECT MIN(trade_date) FROM pboc_omo')
                result = cursor.fetchone()
                earliest_date = result[0] if result and result[0] else None
            except Exception:
                pass
            finally:
                cursor.close()

        # Backfill needed if earliest data is recent (haven't fetched old history yet)
        need_backfill = earliest_date is None or str(earliest_date) > '2014-01-01'
        if not need_backfill:
            self.logger.info(f'  ChinaMoney backfill complete (earliest={earliest_date})')
        else:
            self.logger.info(f'  ChinaMoney backfill needed (earliest={earliest_date})')
            self.logger.info('  Fetching from ChinaMoney API (historical)...')
            api_url = 'https://www.chinamoney.com.cn/ags/ms/cm-s-notice-query/contents'
            api_headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'X-Requested-With': 'XMLHttpRequest',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            }
            cm_page = 1
            cm_stop = False
            # Don't set a stop date — use insert_ignore to handle duplicates,
            # and fetch ALL historical pages to backfill completely
            cm_latest = None

            while not cm_stop:
                try:
                    resp = self.session.post(
                        api_url,
                        data={'pageNo': cm_page, 'pageSize': 20, 'channelId': '2845'},
                        headers=api_headers, timeout=30,
                    )
                    data = resp.json()
                except Exception as e:
                    self.logger.warning(f'  ChinaMoney page {cm_page} failed: {e}')
                    break
                records = data.get('records', [])
                if not records:
                    break

                page_rows = []
                for rec in records:
                    release_date = rec.get('releaseDate', '')
                    if cm_latest and release_date <= cm_latest:
                        cm_stop = True
                        break
                    draft_path = rec.get('draftPath', '')
                    url = f'https://www.chinamoney.com.cn{draft_path}'
                    try:
                        resp2 = self.session.get(
                            url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
                        resp2.encoding = 'utf-8'
                        parsed = _parse_omo_html(BeautifulSoup(resp2.text, 'html.parser'))
                        page_rows.extend(parsed)
                        self.rate_limit_pause(0.2)
                    except Exception as e:
                        self.logger.warning(f'  Failed to parse {url}: {e}')

                _save_batch(page_rows, 'ChinaMoney', cm_page)
                cm_page += 1
                self.rate_limit_pause(0.3)

        self.logger.info(f'  OMO fetch done: {total_saved} total entries')
        return []

    def _fetch_shibor(self):
        """Fetch SHIBOR rates via ak.macro_china_shibor_all()."""
        df = self.retry_call(ak.macro_china_shibor_all, label='macro_china_shibor_all')
        rows = []
        cols = df.columns.tolist()
        for _, r in df.iterrows():
            trade_date = pd.to_datetime(r.iloc[0], errors='coerce')
            if pd.isna(trade_date):
                continue
            rows.append((
                trade_date.date(),
                self._to_float(r.iloc[1] if len(cols) > 1 else None),    # O/N rate
                self._to_float(r.iloc[2] if len(cols) > 2 else None),    # O/N change
                self._to_float(r.iloc[3] if len(cols) > 3 else None),    # 1W rate
                self._to_float(r.iloc[4] if len(cols) > 4 else None),    # 1W change
                self._to_float(r.iloc[5] if len(cols) > 5 else None),    # 2W rate
                self._to_float(r.iloc[6] if len(cols) > 6 else None),    # 2W change
                self._to_float(r.iloc[7] if len(cols) > 7 else None),    # 1M rate
                self._to_float(r.iloc[8] if len(cols) > 8 else None),    # 1M change
                self._to_float(r.iloc[9] if len(cols) > 9 else None),    # 3M rate
                self._to_float(r.iloc[10] if len(cols) > 10 else None),  # 3M change
                self._to_float(r.iloc[11] if len(cols) > 11 else None),  # 6M rate
                self._to_float(r.iloc[12] if len(cols) > 12 else None),  # 6M change
                self._to_float(r.iloc[13] if len(cols) > 13 else None),  # 9M rate
                self._to_float(r.iloc[14] if len(cols) > 14 else None),  # 9M change
                self._to_float(r.iloc[15] if len(cols) > 15 else None),  # 1Y rate
                self._to_float(r.iloc[16] if len(cols) > 16 else None),  # 1Y change
            ))
        return rows

    def _fetch_balance_sheet(self):
        """Fetch PBOC balance sheet from Sina's macro endpoint."""
        df = self._fetch_sina_macro_df(event='8', label='macro_china_central_bank_balance')
        rows = []
        for _, r in df.iterrows():
            period = str(r.get('统计时间')) if '统计时间' in df.columns else None
            if not period:
                continue
            rows.append((
                period,
                self._to_float(r.get('总资产')),
                self._to_float(r.get('国外资产')),
                self._to_float(r.get('外汇')),
                self._to_float(r.get('货币黄金')),
                self._to_float(r.get('对政府债权')),
                self._to_float(r.get('对其他存款性公司债权')),
                self._to_float(r.get('对其他金融性公司债权')),
                self._to_float(r.get('其他资产')),
                self._to_float(r.get('总负债')),
                self._to_float(r.get('储备货币')),
                self._to_float(r.get('发行货币')),
                self._to_float(r.get('金融性公司存款')),
                self._to_float(r.get('债券')),
                self._to_float(r.get('政府存款')),
                self._to_float(r.get('自有资金')),
                self._to_float(r.get('其他负债')),
            ))
        return rows

    # -- Helpers ----------------------------------------------------------------

    def _sina_macro_sessions(self):
        """Candidate sessions for the Sina macro API, most promising first.

        Sina blocks some datacenter IPs (e.g. overseas servers) while the CN
        proxy can fail independently, so keep both transports available:
        direct connection first, CN-proxied session as fallback. The transport
        that succeeded last is remembered and tried first on later pages.
        """
        if not hasattr(self, '_sina_direct_session'):
            session = self._create_session()
            session.trust_env = False
            session.proxies = {}
            self._sina_direct_session = session
        sessions = [self._sina_direct_session]
        if getattr(self, '_cn_proxies', None):
            sessions.append(self.session)
        preferred = getattr(self, '_sina_session_ok', None)
        if preferred in sessions:
            sessions.remove(preferred)
            sessions.insert(0, preferred)
        return sessions

    def _fetch_sina_macro_df(self, event, label, page_size=31):
        """Fetch a Sina macro table with proxy-aware, page-level retries."""
        with self._SINA_MACRO_LOCK:
            first_page = self._fetch_sina_macro_page(event, 0, page_size, label)
            count = int(first_page.get('count') or 0)
            pages = max(1, math.ceil(count / page_size))
            frames = [pd.DataFrame(first_page.get('data', []))]
            columns = [item[1] for item in first_page.get('config', {}).get('all', [])]

            for page in range(1, pages):
                offset = page * page_size
                page_data = self._fetch_sina_macro_page(event, offset, page_size, label)
                frames.append(pd.DataFrame(page_data.get('data', [])))
                self.rate_limit_pause(0.2)

        if not frames:
            return pd.DataFrame(columns=columns)

        df = pd.concat(frames, ignore_index=True)
        if columns:
            if len(columns) != len(df.columns):
                raise ValueError(
                    f'{label} returned {len(df.columns)} columns, expected {len(columns)}'
                )
            df.columns = columns
        for col in df.columns[1:]:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def _fetch_sina_macro_page(self, event, offset, page_size, label):
        params = {
            'cate': 'fininfo',
            'event': event,
            'from': offset,
            'num': page_size,
            'condition': '',
        }
        headers = {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36'
            ),
            'Referer': 'https://finance.sina.com.cn/mac/',
        }

        def _fetch_page(session):
            resp = session.get(
                self.SINA_MACRO_URL,
                params=params,
                headers=headers,
                timeout=20,
            )
            resp.raise_for_status()
            text = resp.text
            start = text.find('{')
            end = text.rfind('}')
            if start < 0 or end <= start:
                snippet = re.sub(r'\s+', ' ', text[:160])
                raise ValueError(
                    f'unexpected Sina response for {label} offset {offset}: {snippet!r}'
                )
            return demjson.decode(text[start:end + 1])

        def _call():
            last_exc = None
            for session in self._sina_macro_sessions():
                try:
                    result = _fetch_page(session)
                    self._sina_session_ok = session
                    return result
                except Exception as exc:
                    last_exc = exc
            raise last_exc

        return self.retry_call(
            _call,
            max_retries=5,
            backoff=3,
            label=f'{label} offset {offset}',
        )

    @staticmethod
    def _to_float(val):
        """Convert a value to float, returning None for non-numeric."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_cn_date(val):
        """Parse Chinese-formatted dates like '2025年05月07日' or standard dates."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        s = str(val).strip()
        # Try Chinese date format: 2025年05月07日
        s_cleaned = s.replace('年', '-').replace('月', '-').replace('日', '')
        try:
            return pd.to_datetime(s_cleaned).date()
        except Exception:
            pass
        # Fallback
        try:
            return pd.to_datetime(s).date()
        except Exception:
            return None

    # -- Main fetch entry point -------------------------------------------------

    def fetch_series(self, series_config):
        """Fetch and store one PBOC series."""
        table = series_config['table']
        func_name = series_config['fetch_func']
        fetch_func = getattr(self, func_name)
        strategy = series_config.get('strategy')

        conn = self.get_connection()
        try:
            self.ensure_table(conn, series_config['create_sql'])

            if strategy == 'incremental':
                date_col = series_config.get('date_column', 'trade_date')
                latest_date = self.get_latest_date(conn, table, date_col)
                self.logger.info(f"  Fetching {table} (incremental, latest: {latest_date})...")
                rows = fetch_func(latest_date=latest_date, _conn=conn)
            else:
                self.logger.info(f"  Fetching {table} from AKShare...")
                rows = fetch_func()

            self.logger.info(f"  Got {len(rows)} rows for {table}")

            if strategy == 'truncate':
                self.truncate_and_insert(
                    conn, table, series_config['columns'], rows
                )
            elif strategy == 'incremental':
                self.insert_ignore_rows(
                    conn, table, series_config['columns'], rows
                )
            else:
                self.upsert_rows(
                    conn, table, series_config['columns'], rows,
                    on_duplicate_update=series_config.get('update_columns'),
                )
            self.rate_limit_pause(1.0)
        finally:
            if conn.is_connected():
                conn.close()

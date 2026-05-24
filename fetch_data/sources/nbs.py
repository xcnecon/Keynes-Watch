"""
NBSFetcher -- fetcher for National Bureau of Statistics real estate data.

Series:
    nbs_real_estate_climate    国房景气指数 (via AKShare)
    nbs_house_price            70城住宅价格指数 — 汇总 + 分面积 (via NBS easyquery API, dbcode=csyd)
    nbs_real_estate_macro      全国房地产月度宏观数据 — 9大类 (via NBS easyquery API, dbcode=hgyd)

Requires: pip install akshare lxml
"""

import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from io import StringIO
from urllib.parse import urljoin

import akshare as ak
import pandas as pd
import requests
from bs4 import BeautifulSoup

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from fetch_data.base import BaseFetcher

# ---------------------------------------------------------------------------
# NBS easyquery API indicator codes for 70-city house price index
# Parent: A0108 = "70个大中城市住宅销售价格指数"
# ---------------------------------------------------------------------------
HOUSE_PRICE_INDICATORS = {
    # (db_column_prefix, mom_code, yoy_code)
    'new':           ('A010804', 'A010805'),   # 新建商品住宅
    'used':          ('A010807', 'A010808'),   # 二手住宅
    'new_90below':   ('A01080A', 'A01080B'),   # 新建 ≤90m²
    'new_90to144':   ('A01080D', 'A01080E'),   # 新建 90-144m²
    'new_144above':  ('A01080G', 'A01080H'),   # 新建 >144m²
    'used_90below':  ('A01080J', 'A01080K'),   # 二手 ≤90m²
    'used_90to144':  ('A01080M', 'A01080N'),   # 二手 90-144m²
    'used_144above': ('A01080P', 'A01080Q'),   # 二手 >144m²
}

NBS_API_URL = 'https://data.stats.gov.cn/easyquery.htm'
NBS_RELEASE_LIST_URL = 'https://www.stats.gov.cn/sj/zxfb/'
NBS_RELEASE_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/131.0.0.0 Safari/537.36'),
}

# ---------------------------------------------------------------------------
# NBS easyquery API — 全国房地产月度宏观指标分类 (dbcode=hgyd)
# Parent: A06 = "房地产"
# ---------------------------------------------------------------------------
NBS_MACRO_CATEGORIES = {
    'A0601': '房地产开发投资情况',
    'A0602': '房地产开发投资实际到位资金',
    'A0603': '房地产土地开发与销售情况',
    'A0604': '房地产施工、竣工面积',
    'A0605': '商品住宅施工、竣工面积',
    'A0608': '商品房销售面积',
    'A0609': '商品房销售额',
    'A060A': '商品住宅销售面积',
    'A060B': '商品住宅销售额',
}


class NBSFetcher(BaseFetcher):
    """Fetches NBS real estate data."""

    def __init__(self):
        super().__init__()
        self._setup_cn_proxy()

    SERIES = [
        {
            'table': 'nbs_real_estate_climate',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS nbs_real_estate_climate (
                    record_date DATE NOT NULL,
                    value DECIMAL(10,2),
                    change_1m DECIMAL(10,6),
                    change_3m DECIMAL(10,6),
                    change_6m DECIMAL(10,6),
                    change_1y DECIMAL(10,6),
                    change_2y DECIMAL(10,6),
                    change_3y DECIMAL(10,6),
                    PRIMARY KEY (record_date)
                )
            """,
            'columns': ['record_date', 'value', 'change_1m', 'change_3m',
                        'change_6m', 'change_1y', 'change_2y', 'change_3y'],
            'strategy': 'truncate',
            'fetch_func': '_fetch_climate',
        },
        {
            'table': 'nbs_house_price',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS nbs_house_price (
                    record_date DATE NOT NULL,
                    city VARCHAR(20) NOT NULL,
                    new_mom DECIMAL(10,2),
                    new_yoy DECIMAL(10,2),
                    used_mom DECIMAL(10,2),
                    used_yoy DECIMAL(10,2),
                    new_90below_mom DECIMAL(10,2),
                    new_90below_yoy DECIMAL(10,2),
                    new_90to144_mom DECIMAL(10,2),
                    new_90to144_yoy DECIMAL(10,2),
                    new_144above_mom DECIMAL(10,2),
                    new_144above_yoy DECIMAL(10,2),
                    used_90below_mom DECIMAL(10,2),
                    used_90below_yoy DECIMAL(10,2),
                    used_90to144_mom DECIMAL(10,2),
                    used_90to144_yoy DECIMAL(10,2),
                    used_144above_mom DECIMAL(10,2),
                    used_144above_yoy DECIMAL(10,2),
                    PRIMARY KEY (record_date, city)
                )
            """,
            'columns': ['record_date', 'city',
                        'new_mom', 'new_yoy', 'used_mom', 'used_yoy',
                        'new_90below_mom', 'new_90below_yoy',
                        'new_90to144_mom', 'new_90to144_yoy',
                        'new_144above_mom', 'new_144above_yoy',
                        'used_90below_mom', 'used_90below_yoy',
                        'used_90to144_mom', 'used_90to144_yoy',
                        'used_144above_mom', 'used_144above_yoy'],
            'strategy': 'upsert',
            'update_columns': ['new_mom', 'new_yoy', 'used_mom', 'used_yoy',
                               'new_90below_mom', 'new_90below_yoy',
                               'new_90to144_mom', 'new_90to144_yoy',
                               'new_144above_mom', 'new_144above_yoy',
                               'used_90below_mom', 'used_90below_yoy',
                               'used_90to144_mom', 'used_90to144_yoy',
                               'used_144above_mom', 'used_144above_yoy'],
            'fetch_func': '_fetch_house_price',
        },
        {
            'table': 'nbs_real_estate_macro',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS nbs_real_estate_macro (
                    record_date DATE NOT NULL,
                    indicator_code VARCHAR(20) NOT NULL,
                    indicator_name VARCHAR(100),
                    value DECIMAL(18,4),
                    PRIMARY KEY (record_date, indicator_code)
                )
            """,
            'columns': ['record_date', 'indicator_code', 'indicator_name', 'value'],
            'strategy': 'truncate',
            'update_columns': ['indicator_name', 'value'],
            'fetch_func': '_fetch_real_estate_macro',
        },
    ]

    @staticmethod
    def _to_float(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        if isinstance(val, str):
            val = (val.replace(',', '')
                      .replace('\xa0', '')
                      .replace('\u3000', '')
                      .strip())
            if val in ('', '-', '--', '\u2014', 'nan', 'NaN'):
                return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _clean_cn_text(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ''
        return re.sub(r'[\s\u3000\xa0]+', '', str(val)).strip()

    @staticmethod
    def _nbs_json(resp):
        if resp.status_code != 200:
            snippet = resp.text[:200].replace('\n', ' ')
            raise ValueError(f"NBS API HTTP {resp.status_code}: {snippet}")
        try:
            return resp.json()
        except ValueError as exc:
            snippet = resp.text[:200].replace('\n', ' ')
            raise ValueError(f"NBS API non-JSON response: {snippet}") from exc

    def _easyquery_available(self, probe):
        attr = f'_easyquery_{probe}_disabled'
        if getattr(self, attr, False):
            return False
        try:
            if probe == 'house':
                self._nbs_query('A010804', period='LAST1')
            else:
                self._nbs_macro_query('A0601', period='LAST1')
            return True
        except Exception as exc:
            setattr(self, attr, True)
            self.logger.warning(
                f"  NBS easyquery unavailable for {probe}; using official releases: {exc}")
            return False

    def _fetch_release_links(self, keyword, max_pages=8):
        """Return unique NBS news-release links whose title contains keyword."""
        links = []
        seen = set()
        for page_idx in range(max_pages):
            suffix = '' if page_idx == 0 else f'index_{page_idx}.html'
            page_url = urljoin(NBS_RELEASE_LIST_URL, suffix)
            resp = self._get_release_page(page_url, timeout=30)
            resp.raise_for_status()
            resp.encoding = 'utf-8'
            soup = BeautifulSoup(resp.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                title = (a.get('title') or a.get_text(' ', strip=True) or '').strip()
                if keyword not in title:
                    continue
                full_url = urljoin(page_url, a['href'])
                if full_url in seen:
                    continue
                seen.add(full_url)
                links.append((title, full_url))
            self.rate_limit_pause(0.2)
        return links

    def _get_release_page(self, url, timeout=60):
        """Fetch an official NBS release page through the configured session.

        China data sources may require CN_PROXY; BaseFetcher wires that proxy
        into self.session, so release fallback scraping must not bypass it.
        """
        return self.session.get(
            url,
            headers=NBS_RELEASE_HEADERS,
            timeout=timeout,
        )

    @staticmethod
    def _parse_house_release_period(title):
        m = re.search(r'(\d{4})\u5e74(\d{1,2})\u6708(?:\u4efd)?70\u4e2a', title)
        if not m:
            return None
        return date(int(m.group(1)), int(m.group(2)), 1)

    @staticmethod
    def _parse_macro_release_period(title):
        year_m = re.search(r'(\d{4})\u5e74', title)
        if not year_m:
            return None
        year = int(year_m.group(1))
        m = re.search(r'1[\u2014\u2013-](\d{1,2})\u6708', title)
        if m:
            return date(year, int(m.group(1)), 1)
        if '\u4e00\u5b63\u5ea6' in title:
            return date(year, 3, 1)
        if '\u4e0a\u534a\u5e74' in title:
            return date(year, 6, 1)
        if '\u524d\u4e09\u5b63\u5ea6' in title:
            return date(year, 9, 1)
        if re.search(r'\d{4}\u5e74\u5168\u56fd\u623f\u5730\u4ea7', title):
            return date(year, 12, 1)
        return None

    # -- Climate index (AKShare) --------------------------------------------

    def _fetch_climate(self):
        """Fetch real estate climate index via ak.macro_china_real_estate()."""
        df = self.retry_call(ak.macro_china_real_estate, label='macro_china_real_estate')
        rows = []
        for _, r in df.iterrows():
            d = pd.to_datetime(r.iloc[0], errors='coerce')
            if pd.isna(d):
                continue
            rows.append((
                d.date(),
                self._to_float(r.iloc[1]),
                self._to_float(r.iloc[2]),
                self._to_float(r.iloc[3]),
                self._to_float(r.iloc[4]),
                self._to_float(r.iloc[5]),
                self._to_float(r.iloc[6]),
                self._to_float(r.iloc[7]),
            ))
        return rows

    # -- 70-city house price (NBS easyquery API) ----------------------------

    def _nbs_query(self, indicator_code, period='LAST180'):
        """Query NBS easyquery API for one indicator across all 70 cities.
        Returns dict: { (YYYYMM, region_code): value }
        """
        params = {
            'm': 'QueryData',
            'dbcode': 'csyd',
            'rowcode': 'reg',
            'colcode': 'sj',
            'wds': json.dumps([{'wdcode': 'zb', 'valuecode': indicator_code}]),
            'dfwds': json.dumps([{'wdcode': 'sj', 'valuecode': period}]),
            'k1': str(int(time.time() * 1000)),
        }
        r = requests.get(NBS_API_URL, params=params, timeout=120,
                         headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'},
                         proxies=self._cn_proxies)
        data = self._nbs_json(r)
        if data.get('returncode') != 200:
            raise ValueError(f"NBS API error: {data.get('returncode')}")

        nodes = data['returndata']['datanodes']
        result = {}
        for n in nodes:
            wds_map = {wd['wdcode']: wd['valuecode'] for wd in n['wds']}
            reg = wds_map['reg']
            sj = wds_map['sj']
            val = n['data']['data'] if n['data']['hasdata'] else None
            result[(sj, reg)] = val

        # Extract region names
        region_names = {}
        for w in data['returndata']['wdnodes']:
            if w.get('wdcode') == 'reg':
                for node in w['nodes']:
                    region_names[node['code']] = node['cname']

        return result, region_names

    def _overall_house_block_starts(self, table):
        headers = [self._clean_cn_text(v) for v in table.iloc[0]]
        starts = [i for i, h in enumerate(headers) if h == '\u57ce\u5e02']
        if len(starts) >= 2:
            return starts[:2]

        # Older/no-average releases have 6 columns; current YTD-average
        # releases have 8. If the header is malformed, split the table in two.
        half_width = len(headers) // 2
        if half_width >= 3:
            return [0, half_width]
        return [0]

    def _add_overall_house_table(self, table, rows_by_city, mom_col, yoy_col):
        block_starts = self._overall_house_block_starts(table)
        for _, r in table.iterrows():
            for base in block_starts:
                if base + 2 >= len(r):
                    continue
                city = self._clean_cn_text(r.iloc[base])
                if not city or city == '\u57ce\u5e02':
                    continue
                mom = self._to_float(r.iloc[base + 1])
                yoy = self._to_float(r.iloc[base + 2])
                if mom is None and yoy is None:
                    continue
                rows_by_city.setdefault(city, {})[mom_col] = mom
                rows_by_city.setdefault(city, {})[yoy_col] = yoy

    def _area_house_group_starts(self, table):
        headers = [self._clean_cn_text(v) for v in table.iloc[0]]
        starts = []
        prev = None
        for i, header in enumerate(headers[1:], start=1):
            if not header or header == prev:
                continue
            starts.append(i)
            prev = header
        if len(starts) >= 3:
            return starts[:3]

        metric_width = (len(headers) - 1) // 3
        if metric_width >= 2:
            return [1, 1 + metric_width, 1 + metric_width * 2]
        return []

    def _add_area_house_table(self, table, rows_by_city, prefix):
        group_starts = self._area_house_group_starts(table)
        if len(group_starts) < 3:
            raise ValueError(f"Cannot detect house-price area table layout: {table.shape}")
        suffixes = ['90below', '90to144', '144above']

        for _, r in table.iterrows():
            city = self._clean_cn_text(r.iloc[0])
            if not city or city == '\u57ce\u5e02':
                continue
            vals = rows_by_city.setdefault(city, {})
            for suffix, start in zip(suffixes, group_starts):
                if start + 1 >= len(r):
                    continue
                mom = self._to_float(r.iloc[start])
                yoy = self._to_float(r.iloc[start + 1])
                if mom is None and yoy is None:
                    continue
                vals[f'{prefix}_{suffix}_mom'] = mom
                vals[f'{prefix}_{suffix}_yoy'] = yoy

    def _fetch_house_price_release(self, title, url):
        record_date = self._parse_house_release_period(title)
        if not record_date:
            return []
        resp = self._get_release_page(url, timeout=60)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
        tables = pd.read_html(StringIO(resp.text))
        if len(tables) < 6:
            raise ValueError(f"House price release has {len(tables)} tables: {url}")

        rows_by_city = {}
        self._add_overall_house_table(tables[0], rows_by_city, 'new_mom', 'new_yoy')
        self._add_overall_house_table(tables[1], rows_by_city, 'used_mom', 'used_yoy')
        self._add_area_house_table(tables[2], rows_by_city, 'new')
        self._add_area_house_table(tables[3], rows_by_city, 'new')
        self._add_area_house_table(tables[4], rows_by_city, 'used')
        self._add_area_house_table(tables[5], rows_by_city, 'used')

        col_order = [
            'new_mom', 'new_yoy', 'used_mom', 'used_yoy',
            'new_90below_mom', 'new_90below_yoy',
            'new_90to144_mom', 'new_90to144_yoy',
            'new_144above_mom', 'new_144above_yoy',
            'used_90below_mom', 'used_90below_yoy',
            'used_90to144_mom', 'used_90to144_yoy',
            'used_144above_mom', 'used_144above_yoy',
        ]
        rows = []
        for city, vals in rows_by_city.items():
            row = [record_date, city]
            row.extend(vals.get(col) for col in col_order)
            if any(v is not None for v in row[2:]):
                rows.append(tuple(row))
        if len(rows) < 60:
            raise ValueError(f"Only parsed {len(rows)} house price city rows from {url}")
        self.logger.info(f"  Official release {record_date}: {len(rows)} house-price rows")
        return rows

    def _fetch_house_price_releases(self, after_date=None):
        keyword = '70\u4e2a\u5927\u4e2d\u57ce\u5e02\u5546\u54c1\u4f4f\u5b85\u9500\u552e\u4ef7\u683c\u53d8\u52a8\u60c5\u51b5'
        releases = []
        for title, url in self._fetch_release_links(keyword, max_pages=8):
            record_date = self._parse_house_release_period(title)
            if not record_date:
                continue
            if after_date and record_date <= after_date:
                continue
            releases.append((record_date, title, url))
        rows = []
        for _, title, url in sorted(releases):
            rows.extend(self._fetch_house_price_release(title, url))
            self.rate_limit_pause(0.5)
        return rows

    def _fetch_house_price(self, period='LAST180', latest_date=None):
        """Fetch all 70-city house price indicators via NBS easyquery API.
        Makes 16 API calls (8 indicators × mom/yoy) in parallel (5 threads).
        Period: LAST13 for incremental, LAST180 for full refresh.
        """

        # Collect all data: key = (period, region_code), value = dict of column values
        all_data = {}
        region_names = {}
        errors = []

        # Build task list: (column_name, indicator_code)
        tasks = []
        for prefix, (mom_code, yoy_code) in HOUSE_PRICE_INDICATORS.items():
            tasks.append((f'{prefix}_mom', mom_code))
            tasks.append((f'{prefix}_yoy', yoy_code))

        # Fetch all 16 indicators in parallel (5 threads)
        if self._easyquery_available('house'):
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(
                        self.retry_call,
                        lambda ic=code, p=period: self._nbs_query(ic, period=p),
                        max_retries=2,
                        backoff=2,
                        label=f'nbs_{code}',
                    ): (col, code)
                    for col, code in tasks
                }
                for future in as_completed(futures):
                    col_name, indicator_code = futures[future]
                    try:
                        result, rnames = future.result()
                        region_names.update(rnames)
                        for (sj, reg), val in result.items():
                            key = (sj, reg)
                            if key not in all_data:
                                all_data[key] = {}
                            all_data[key][col_name] = val
                        self.logger.info(f"  {indicator_code} ({col_name}): {len(result)} values")
                    except Exception as e:
                        self.logger.error(f"  {indicator_code} ({col_name}) FAILED: {e}")
                        errors.append((indicator_code, col_name, e))

        if errors:
            failed = ', '.join(
                f'{code}/{col}' for code, col, _exc in errors
            )
            raise RuntimeError(
                f"Partial NBS house-price fetch failed for "
                f"{len(errors)} indicator(s): {failed}"
            )

        # Convert to rows
        col_order = [
            'new_mom', 'new_yoy', 'used_mom', 'used_yoy',
            'new_90below_mom', 'new_90below_yoy',
            'new_90to144_mom', 'new_90to144_yoy',
            'new_144above_mom', 'new_144above_yoy',
            'used_90below_mom', 'used_90below_yoy',
            'used_90to144_mom', 'used_90to144_yoy',
            'used_144above_mom', 'used_144above_yoy',
        ]
        rows = []
        unmapped_codes = set()
        for (sj, reg), vals in all_data.items():
            try:
                record_date = pd.to_datetime(sj, format='%Y%m').date()
            except Exception:
                continue
            city = region_names.get(reg)
            if city is None:
                unmapped_codes.add(reg)
                city = reg
            row = [record_date, city]
            for col in col_order:
                row.append(self._to_float(vals.get(col)))
            rows.append(tuple(row))

        if unmapped_codes:
            self.logger.warning(f"  Unmapped region codes (stored as raw code): {unmapped_codes}")
        self.logger.info(f"  Total: {len(rows)} rows across {len(region_names)} cities")
        max_date = max((r[0] for r in rows), default=None)
        release_after = max_date or (latest_date - timedelta(days=1) if latest_date else None)
        try:
            release_rows = self._fetch_house_price_releases(after_date=release_after)
        except Exception as exc:
            if not rows:
                raise
            self.logger.warning(f"  Official NBS release fallback failed: {exc}")
            release_rows = []
        if release_rows:
            rows.extend(release_rows)
            self.logger.info(f"  Added {len(release_rows)} rows from official NBS releases")
        if not rows:
            raise RuntimeError("No NBS house price rows fetched")
        return rows

    # -- National macro real estate data (NBS easyquery API, hgyd) -----------

    def _nbs_macro_query(self, zb_code, period='LAST20'):
        """Query NBS easyquery API for national macro indicators (dbcode=hgyd).
        Returns list of (period_str, indicator_code, indicator_name, value).
        """
        params = {
            'm': 'QueryData',
            'dbcode': 'hgyd',
            'rowcode': 'zb',
            'colcode': 'sj',
            'wds': '[]',
            'dfwds': json.dumps([
                {'wdcode': 'zb', 'valuecode': zb_code},
                {'wdcode': 'sj', 'valuecode': period},
            ]),
            'k1': str(int(time.time() * 1000)),
        }
        r = requests.get(NBS_API_URL, params=params, timeout=120,
                         headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'},
                         proxies=self._cn_proxies)
        data = self._nbs_json(r)
        if data.get('returncode') != 200:
            raise ValueError(f"NBS API error: {data.get('returncode')}")

        # Build indicator name mapping
        zb_names = {}
        for w in data['returndata']['wdnodes']:
            if w.get('wdcode') == 'zb':
                for node in w['nodes']:
                    zb_names[node['code']] = node['cname']

        results = []
        for n in data['returndata']['datanodes']:
            wds_map = {wd['wdcode']: wd['valuecode'] for wd in n['wds']}
            zb = wds_map.get('zb', '')
            sj = wds_map.get('sj', '')
            val = n['data']['data'] if n['data']['hasdata'] else None
            results.append((sj, zb, zb_names.get(zb, zb), val))

        return results

    def _macro_release_codes(self, label, parent):
        if label.startswith('\u623f\u5730\u4ea7\u5f00\u53d1\u6295\u8d44'):
            return 'investment', ('A060101', 'A060102')
        if label.startswith('\u529e\u516c\u697c') and parent == 'investment':
            return parent, ('A06010D', 'A06010E')
        if label.startswith('\u5546\u4e1a\u8425\u4e1a\u7528\u623f') and parent == 'investment':
            return parent, ('A06010F', 'A06010G')
        if label.startswith('\u623f\u5c4b\u65bd\u5de5\u9762\u79ef'):
            return 'construction', ('A060401', 'A060402')
        if label.startswith('\u623f\u5c4b\u65b0\u5f00\u5de5\u9762\u79ef'):
            return 'new_starts', ('A060403', 'A060404')
        if label.startswith('\u623f\u5c4b\u7ae3\u5de5\u9762\u79ef'):
            return 'completions', ('A060405', 'A060406')
        if label.startswith('\u65b0\u5efa\u5546\u54c1\u623f\u9500\u552e\u9762\u79ef'):
            return 'sales_area', ('A060801', 'A060802')
        if label.startswith('\u65b0\u5efa\u5546\u54c1\u623f\u9500\u552e\u989d'):
            return 'sales_value', ('A060901', 'A060902')
        if label.startswith('\u623f\u5730\u4ea7\u5f00\u53d1\u4f01\u4e1a\u672c\u5e74\u5230\u4f4d\u8d44\u91d1'):
            return 'funds', ('A060205', 'A060206')
        if label.startswith('\u5546\u54c1\u623f\u5f85\u552e\u9762\u79ef'):
            return 'inventory', None
        if label.startswith('\u5176\u4e2d\uff1a\u56fd\u5185\u8d37\u6b3e') and parent == 'funds':
            return parent, ('A060207', 'A060208')
        if label.startswith('\u56fd\u5185\u8d37\u6b3e') and parent == 'funds':
            return parent, ('A060207', 'A060208')
        if label.startswith('\u81ea\u7b79\u8d44\u91d1') and parent == 'funds':
            return parent, ('A06020B', 'A06020C')
        if label in ('\u5176\u4e2d\uff1a\u4f4f\u5b85', '\u4f4f\u5b85'):
            res_codes = {
                'investment': ('A060105', 'A060106'),
                'construction': ('A060501', 'A060502'),
                'new_starts': ('A060503', 'A060504'),
                'completions': ('A060505', 'A060506'),
                'sales_area': ('A060A01', 'A060A02'),
                'sales_value': ('A060B01', 'A060B02'),
            }
            return parent, res_codes.get(parent)
        return parent, None

    def _fetch_macro_release(self, title, url):
        record_date = self._parse_macro_release_period(title)
        if not record_date:
            return []
        resp = self._get_release_page(url, timeout=60)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
        tables = pd.read_html(StringIO(resp.text))
        source_table = None
        for table in tables:
            header = self._clean_cn_text(table.iloc[0, 0]) if table.shape[1] else ''
            if table.shape[1] >= 3 and '\u6307\u6807' in header:
                source_table = table
                break
        if source_table is None:
            raise ValueError(f"Macro release table not found: {url}")

        rows = []
        parent = None
        for _, r in source_table.iloc[1:].iterrows():
            label = self._clean_cn_text(r.iloc[0])
            if not label:
                continue
            parent, codes = self._macro_release_codes(label, parent)
            if not codes:
                continue
            value = self._to_float(r.iloc[1])
            yoy = self._to_float(r.iloc[2])
            level_code, yoy_code = codes
            rows.append((record_date, level_code, label, value))
            rows.append((record_date, yoy_code, f'{label}_\u7d2f\u8ba1\u589e\u957f', yoy))
        if len(rows) < 20:
            raise ValueError(f"Only parsed {len(rows)} macro rows from {url}")
        self.logger.info(f"  Official release {record_date}: {len(rows)} macro rows")
        return rows

    def _fetch_macro_releases(self, after_date=None):
        keyword = '\u5168\u56fd\u623f\u5730\u4ea7\u5e02\u573a\u57fa\u672c\u60c5\u51b5'
        releases = []
        for title, url in self._fetch_release_links(keyword, max_pages=8):
            record_date = self._parse_macro_release_period(title)
            if not record_date:
                continue
            if after_date and record_date <= after_date:
                continue
            releases.append((record_date, title, url))
        rows = []
        for _, title, url in sorted(releases):
            rows.extend(self._fetch_macro_release(title, url))
            self.rate_limit_pause(0.5)
        return rows

    def _fetch_one_macro_category(self, zb_code, category_name):
        """Fetch one macro category. Called in parallel by _fetch_real_estate_macro."""
        results = self.retry_call(
            lambda code=zb_code: self._nbs_macro_query(code, period='LAST180'),
            max_retries=2,
            backoff=2,
            label=f'nbs_macro_{zb_code}',
        )
        rows = []
        for sj, indicator_code, indicator_name, val in results:
            try:
                record_date = pd.to_datetime(sj, format='%Y%m').date()
            except Exception:
                continue
            rows.append((record_date, indicator_code, indicator_name,
                         self._to_float(val)))
        self.logger.info(f"  {zb_code} ({category_name}): {len(rows)} values")
        return rows

    def _fetch_real_estate_macro(self, latest_date=None):
        """Fetch all national macro real estate indicators (9 categories) in parallel (5 threads)."""
        rows = []
        errors = []
        items = list(NBS_MACRO_CATEGORIES.items())
        if self._easyquery_available('macro'):
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(self._fetch_one_macro_category, zb_code, cat_name): (zb_code, cat_name)
                    for zb_code, cat_name in items
                }
                for future in as_completed(futures):
                    zb_code, cat_name = futures[future]
                    try:
                        cat_rows = future.result()
                        rows.extend(cat_rows)
                    except Exception as e:
                        self.logger.error(f"  {zb_code} ({cat_name}) FAILED: {e}")
                        errors.append((zb_code, e))

        if errors:
            failed = ', '.join(code for code, _exc in errors)
            raise RuntimeError(
                f"Partial NBS macro fetch failed for "
                f"{len(errors)} category/categories: {failed}"
            )

        self.logger.info(f"  Total: {len(rows)} macro indicator rows")
        max_date = max((r[0] for r in rows), default=None)
        release_after = max_date or (latest_date - timedelta(days=1) if latest_date else None)
        try:
            release_rows = self._fetch_macro_releases(after_date=release_after)
        except Exception as exc:
            if not rows:
                raise
            self.logger.warning(f"  Official NBS release fallback failed: {exc}")
            release_rows = []
        if release_rows:
            rows.extend(release_rows)
            self.logger.info(f"  Added {len(release_rows)} rows from official NBS releases")
        if not rows:
            if errors:
                raise RuntimeError(f"No NBS macro rows fetched; {len(errors)} category fetches failed")
            raise RuntimeError("No NBS macro rows fetched")
        if not max_date and release_rows:
            self._last_fetch_partial = True
        return rows

    # -- Main entry ---------------------------------------------------------

    def fetch_series(self, series_config):
        """Fetch and store one NBS series."""
        table = series_config['table']
        func_name = series_config['fetch_func']
        fetch_func = getattr(self, func_name)

        conn = self.get_connection()
        try:
            self.ensure_table(conn, series_config['create_sql'])
            self._last_fetch_partial = False

            # Incremental: LAST13 if table has data, LAST180 if empty
            if func_name == '_fetch_house_price':
                latest = self.get_latest_date(conn, table)
                if latest:
                    period = 'LAST13'
                    self.logger.info(f"  Fetching {table} (incremental LAST13, latest: {latest})...")
                else:
                    period = 'LAST180'
                    self.logger.info(f"  Fetching {table} (full LAST180, table empty)...")
                rows = fetch_func(period=period, latest_date=latest)
            elif func_name == '_fetch_real_estate_macro':
                latest = self.get_latest_date(conn, table)
                self.logger.info(f"  Fetching {table} (latest: {latest})...")
                rows = fetch_func(latest_date=latest)
            else:
                self.logger.info(f"  Fetching {table}...")
                rows = fetch_func()
            self.logger.info(f"  Got {len(rows)} rows for {table}")

            if series_config.get('strategy') == 'truncate' and not self._last_fetch_partial:
                self.truncate_and_insert(
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

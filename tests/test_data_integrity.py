"""Regressions for the data errors observed in the 2026-09-07 page audit.

Only public source fixtures and isolated database substitutes are used.
"""

import copy
import json
import random
import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pytest
from bs4 import BeautifulSoup

from fetch_data.sources.fiscal import (
    FiscalDataFetcher, SERIES_AVERAGE_MATURITY, SERIES_MTS, parse_mts_rows,
)
from fetch_data.sources.fred import FREDFetcher
from fetch_data.sources.nbs import NBSFetcher, RETAIL_INDICATORS
from fetch_data.sources.nyfed import NYFedFetcher, SERIES_REPO
from fetch_data.sources.pboc import PBOCFetcher

FIXTURES = Path(__file__).parent / 'fixtures'


def mts_records():
    return json.loads((FIXTURES / 'mts_202607.json').read_text(encoding='utf-8'))


def test_mts_resolves_report_sections_and_calendar_dates():
    records = mts_records()
    random.Random(19).shuffle(records)
    rows = {r[0]: r[1:] for r in parse_mts_rows(records)}
    # The old fields request auto-summed the two July rows to $723.450535bn.
    assert rows[date(2026, 7, 31)] == (
        Decimal('334009.87555579'), Decimal('766317.75017727'),
        Decimal('432307.87462148'),
    )
    assert rows[date(2025, 7, 31)][2] == Decimal('291142.66028767')
    assert rows[date(2025, 10, 31)][2] == Decimal('284332.74399837')
    assert rows[date(2026, 4, 30)][2] == Decimal('-215024.13519777')
    for receipts, outlays, deficit in rows.values():
        assert outlays - receipts == deficit
    assert len(rows) == 22  # 12 prior-FY months + 10 current-FY months; no YTD


def test_mts_prefers_newest_vintage_independently_of_api_order():
    records = mts_records()
    earlier = [copy.deepcopy(r) for r in records
               if r['classification_desc'] in ('FY 2026', 'June')
               and (r['classification_desc'] == 'FY 2026'
                    or r['parent_id'] == '59083765')]
    for r in earlier:
        r['record_date'] = '2026-06-30'
        if r['classification_desc'] == 'June':
            r['current_month_gross_outly_amt'] = '600000000000.00'
            r['current_month_dfct_sur_amt'] = '104238518431.30'
    rows = {r[0]: r for r in parse_mts_rows(records + earlier)}
    assert rows[date(2026, 6, 30)][3] == Decimal('120305.27558637')


@pytest.mark.parametrize('field,value', [
    ('current_month_gross_rcpt_amt', 'null'),
    ('current_month_gross_outly_amt', 'NaN'),
    ('current_month_dfct_sur_amt', '1'),
    ('parent_id', 'missing'),
])
def test_mts_rejects_unreconciled_or_ambiguous_rows(field, value):
    records = mts_records()
    row = next(r for r in records if r['classification_id'] == '59083804')
    row[field] = value
    with pytest.raises(ValueError):
        parse_mts_rows(records)


def test_mts_missing_deficit_is_derived_without_inventing_receipts():
    records = mts_records()
    row = next(r for r in records if r['classification_id'] == '59083804')
    row['current_month_dfct_sur_amt'] = 'null'
    assert parse_mts_rows(records)[-1][3] == Decimal('432307.87462148')


def test_mts_repairs_existing_months_and_preserves_pre_api_history():
    fetcher = FiscalDataFetcher()
    conn = Mock()
    fetcher.get_connection = Mock(return_value=conn)
    fetcher.ensure_table = Mock()
    fetcher.get_latest_date = Mock(return_value=date(2026, 7, 31))
    fetcher._fetch_fiscal_data = Mock(return_value=mts_records())
    fetcher.upsert_rows = Mock()
    fetcher._handle_mts(SERIES_MTS)
    assert 'classification_id' in SERIES_MTS['fields'].split(',')
    assert 'parent_id' in SERIES_MTS['fields'].split(',')
    fetcher._fetch_fiscal_data.assert_called_once_with(SERIES_MTS['endpoint'], SERIES_MTS['fields'])
    rows = fetcher.upsert_rows.call_args.args[3]
    assert rows[-1][3] == Decimal('432307.87462148')
    assert any(r[0] == date(2025, 7, 31) for r in rows)
    assert not any('DELETE' in str(c) or 'TRUNCATE' in str(c)
                   for c in conn.cursor.return_value.execute.call_args_list)


class SQLCursor:
    """Execute the maturity handler's actual predicates on an isolated DB."""
    def __init__(self, conn):
        self.cursor = conn.cursor()

    def execute(self, sql, args=()):
        return self.cursor.execute(sql.replace('%s', '?'), args)

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()


def test_maturity_repair_excludes_summaries_and_is_repeatable_without_new_data():
    db = sqlite3.connect(':memory:')
    db.execute('''CREATE TABLE treasury_average_maturity (
        id INTEGER PRIMARY KEY, record_date TEXT, security_type_desc TEXT,
        security_class1_desc TEXT, security_class2_desc TEXT, maturity_date TEXT,
        amount REAL, maturity REAL, weight REAL, weight_by_type REAL)''')
    rows = [
        ('Notes', None, '2027-07-31', 100, 1),
        ('Bonds', None, '2036-07-31', 900, 10),
        ('Notes', 'Total Treasury Notes', None, 100, 99),
        ('Bonds', 'Total Treasury Bonds', None, 900, 99),
        ('Total Marketable', None, None, 1000, 99),
    ]
    db.executemany('''INSERT INTO treasury_average_maturity
        (record_date, security_class1_desc, security_class2_desc, maturity_date,
         amount, maturity, weight) VALUES ('2026-07-31', ?, ?, ?, ?, ?, 1)''', rows)
    conn = Mock()
    conn.cursor.side_effect = lambda: SQLCursor(db)
    conn.commit.side_effect = db.commit
    fetcher = FiscalDataFetcher()
    fetcher.get_connection = Mock(return_value=conn)
    fetcher.ensure_table = Mock()
    fetcher.get_latest_date = Mock(return_value=date(2026, 7, 31))
    fetcher._fetch_fiscal_data = Mock(return_value=[])
    for _ in range(2):
        fetcher._handle_average_maturity(SERIES_AVERAGE_MATURITY)
        result = dict(db.execute('''SELECT security_class2_desc, maturity
            FROM treasury_average_maturity WHERE maturity_date IS NULL'''))
        assert result == {'Total Treasury Notes': 1, 'Total Treasury Bonds': 10,
                          'Total Marketable': 9.1}
        assert db.execute('''SELECT COUNT(*) FROM treasury_average_maturity
            WHERE maturity_date IS NULL AND weight IS NOT NULL''').fetchone()[0] == 0


@pytest.mark.parametrize('table', ['fred_gdp', 'fred_cpiaucl', 'fred_wresbal'])
def test_revised_fred_observation_updates_even_without_new_date(table):
    fetcher = FREDFetcher()
    config = next(c for c in fetcher.SERIES if c['table'] == table)
    fetcher.get_connection = Mock(return_value=Mock())
    fetcher.ensure_table = Mock()
    fetcher.get_latest_date = Mock(return_value=date(2026, 4, 1))
    fetcher._fetch_fred_observations = Mock(return_value=[
        {'date': '2026-04-01', 'value': '32824.7'},
    ])
    fetcher.upsert_rows = Mock()
    fetcher.fetch_series(config)
    fetcher._fetch_fred_observations.assert_called_once_with(config['series_ids'][0], start_date=None)
    assert fetcher.upsert_rows.call_args.args[3] == [(date(2026, 4, 1), 32824.7)]


def test_repo_refills_lagging_type_and_sums_both_completed_daily_auctions():
    fetcher = NYFedFetcher()
    conn = Mock()
    conn.cursor.return_value.fetchall.return_value = [
        ('Repo', date(2026, 9, 4)), ('Reverse Repo', date(2026, 7, 2)),
    ]
    fetcher.get_connection = Mock(return_value=conn)
    fetcher.ensure_table = Mock()
    def op(id_, kind, amount, status='Results'):
        return {'operationId': id_, 'operationDate': '2026-07-06',
                'operationType': kind, 'totalAmtAccepted': amount, 'auctionStatus': status}
    fetcher.session = Mock()
    fetcher.session.get.return_value.json.return_value = {'repo': {'operations': [
        op('am', 'Repo', 1e9), op('pm', 'Repo', 2e9),
        op('rrp', 'Reverse Repo', 675e6), op('am', 'Repo', 1e9),
        op('pending', 'Repo', None, 'Announced'),
    ]}}
    fetcher._handle_repo(SERIES_REPO)
    assert 'startDate=2026-06-25' in fetcher.session.get.call_args.args[0]
    rows = conn.cursor.return_value.executemany.call_args.args[1]
    assert rows == [('2026-07-06', 'Repo', 3.0), ('2026-07-06', 'Reverse Repo', 0.675)]


def test_omo_official_zero_allotment_has_no_rate():
    soup = BeautifulSoup((FIXTURES / 'omo_no_operation.html').read_text(encoding='utf-8'), 'html.parser')
    assert PBOCFetcher._parse_omo_html(soup) == [('2026-09-04', 7, None, 0.0)]


def test_omo_official_header_and_decimal_whitespace():
    soup = BeautifulSoup((FIXTURES / 'omo_operation.html').read_text(encoding='utf-8'), 'html.parser')
    assert PBOCFetcher._parse_omo_html(soup) == [('2026-09-07', 7, 1.4, 5.0)]


def test_omo_update_revisits_old_zero_and_overwrites_it_with_null():
    fetcher = PBOCFetcher()
    conn = Mock()
    conn.cursor.return_value.fetchone.side_effect = [
        (date(2026, 6, 3),),  # older than the normal month of overlap
        (date(2013, 1, 1),),  # historical backfill already exists
    ]
    listing = '''<a href="/125475/20260904/index.html">September</a>
        <a href="/125475/20260603/index.html">June</a>
        <span class="hui12">2026-09-04</span><span class="hui12">2026-06-03</span>'''
    september = (FIXTURES / 'omo_no_operation.html').read_text(encoding='utf-8')
    june = september.replace('2026年9月4日', '2026年6月3日')
    fetcher._get_pboc_html = Mock(side_effect=[listing, september, june, ''])
    fetcher.rate_limit_pause = Mock()
    fetcher.upsert_rows = Mock()
    fetcher.insert_ignore_rows = Mock()
    fetcher._fetch_omo(latest_date=date(2026, 9, 4), _conn=conn)
    rows = fetcher.upsert_rows.call_args.args[3]
    assert rows == [('2026-09-04', 7, None, 0.0), ('2026-06-03', 7, None, 0.0)]
    assert fetcher.upsert_rows.call_args.kwargs == {'on_duplicate_update': ['rate', 'volume']}
    fetcher.insert_ignore_rows.assert_not_called()


@pytest.mark.parametrize('header,row,expected', [
    ('期限|中标量|中标利率', '7天|1,000亿元|2.00%', (2.0, 1000.0)),
    ('期限|操作利率（%）|投标量|中标量', '7天|1.40|20亿元|10亿元', (1.4, 10.0)),
])
def test_omo_preserves_old_and_new_named_column_formats(header, row, expected):
    def tr(values):
        return '<tr>' + ''.join(f'<td>{v}</td>' for v in values.split('|')) + '</tr>'
    html = '<div id="zoom">2026年9月1日逆回购<table>' + tr(header) + tr(row) + '</table></div>'
    assert PBOCFetcher._parse_omo_html(BeautifulSoup(html, 'html.parser'))[0][2:] == expected


@pytest.mark.parametrize('title,month', [
    ('2026年上半年社会消费品零售总额增长1.3%', 6),
    ('2026年前三季度社会消费品零售总额增长1.3%', 9),
    ('2026年一季度社会消费品零售总额增长1.3%', 3),
    ('2026年全年社会消费品零售总额增长1.3%', 12),
    ('2026年1—2月份社会消费品零售总额增长1.3%', 2),
    ('2026年7月份社会消费品零售总额增长1.3%', 7),
])
def test_retail_named_and_numeric_periods(title, month):
    assert NBSFetcher._parse_retail_release_period(title) == date(2026, month, 1)


def test_retail_half_year_release_populates_june_without_ytd_double_count():
    fetcher = NBSFetcher()
    fetcher.session = Mock()
    fetcher.session.get.return_value.text = (FIXTURES / 'retail_june.html').read_text(encoding='utf-8')
    cells = fetcher._fetch_retail_release('2026年上半年社会消费品零售总额增长1.3%', 'fixture')
    assert cells[(date(2026, 6, 1), 'total')] == {
        'monthly_value': 42691.0, 'monthly_yoy': 1.0,
        'ytd_value': 248722.0, 'ytd_yoy': 1.3,
    }


def test_retail_fills_june_even_when_api_and_database_have_july():
    fetcher = NBSFetcher()
    total = next(v for v in RETAIL_INDICATORS if v[0] == 'total')
    def api(cid, ids, dts, **kwargs):
        if cid == total[2]:
            return [{'code': '202607MM', 'values': [
                {'_id': total[3]['ytd_value'], 'value': '287744'},
            ]}]
        return []
    fetcher._nbs_v2_esdata = Mock(side_effect=api)
    fetcher.rate_limit_pause = Mock()
    def releases(after_date, max_pages):
        assert after_date < date(2026, 6, 1)
        return {(date(2026, 6, 1), 'total'): {'ytd_value': 248722.0}}
    fetcher._fetch_retail_releases = Mock(side_effect=releases)
    rows = fetcher._fetch_retail_sales(latest_date=date(2026, 7, 1))
    assert {r[0] for r in rows} == {date(2026, 6, 1), date(2026, 7, 1)}

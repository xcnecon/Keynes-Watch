"""TreasuryFetcher -- fetches Treasury yield curve XML data.

Handles both nominal and real (TIPS) yield curves from the U.S. Treasury
XML feed, using a unified approach driven by SERIES configuration.
"""

import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime

import pandas as pd

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from fetch_data.base import BaseFetcher

# XML namespace map used by Treasury XML feeds
_NS = {
    'atom': 'http://www.w3.org/2005/Atom',
    'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
    'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices',
}

# Maturity suffixes used in both nominal and real XML fields
_MATURITY_SUFFIXES = [
    '1MONTH', '2MONTH', '3MONTH', '4MONTH', '6MONTH',
    '1YEAR', '2YEAR', '3YEAR', '5YEAR', '7YEAR',
    '10YEAR', '20YEAR', '30YEAR',
]

# Column names in the database table (order matches _MATURITY_SUFFIXES)
_DB_COLUMNS = [
    '1_Mo', '2_Mo', '3_Mo', '4_Mo', '6_Mo',
    '1_Yr', '2_Yr', '3_Yr', '5_Yr', '7_Yr',
    '10_Yr', '20_Yr', '30_Yr',
]

_ALL_COLUMNS = ['record_date', 'type'] + [f'`{c}`' for c in _DB_COLUMNS]
_ALL_COLUMNS_PLAIN = ['record_date', 'type'] + _DB_COLUMNS


class TreasuryFetcher(BaseFetcher):
    """Fetches nominal and real Treasury yield curve data from XML feeds."""

    SERIES = [
        {
            'table': 'yield_curve',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS yield_curve (
                    record_date DATE PRIMARY KEY,
                    type VARCHAR(100),
                    `1_Mo` FLOAT,
                    `2_Mo` FLOAT,
                    `3_Mo` FLOAT,
                    `4_Mo` FLOAT,
                    `6_Mo` FLOAT,
                    `1_Yr` FLOAT,
                    `2_Yr` FLOAT,
                    `3_Yr` FLOAT,
                    `5_Yr` FLOAT,
                    `7_Yr` FLOAT,
                    `10_Yr` FLOAT,
                    `20_Yr` FLOAT,
                    `30_Yr` FLOAT
                )
            """,
            'url': (
                'https://home.treasury.gov/resource-center/data-chart-center/'
                'interest-rates/pages/xmlview?data=daily_treasury_yield_curve'
                '&field_tdr_date_value={year}'
            ),
            'prefix': 'BC_',
            'type_label': 'Nominal Treasury',
            'date_column': 'record_date',
        },
        {
            'table': 'real_yield_curve',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS real_yield_curve (
                    record_date DATE PRIMARY KEY,
                    type VARCHAR(100),
                    `1_Mo` FLOAT,
                    `2_Mo` FLOAT,
                    `3_Mo` FLOAT,
                    `4_Mo` FLOAT,
                    `6_Mo` FLOAT,
                    `1_Yr` FLOAT,
                    `2_Yr` FLOAT,
                    `3_Yr` FLOAT,
                    `5_Yr` FLOAT,
                    `7_Yr` FLOAT,
                    `10_Yr` FLOAT,
                    `20_Yr` FLOAT,
                    `30_Yr` FLOAT
                )
            """,
            'url': (
                'https://home.treasury.gov/resource-center/data-chart-center/'
                'interest-rates/pages/xmlview?data=daily_treasury_real_yield_curve'
                '&field_tdr_date_value={year}'
            ),
            'prefix': 'TC_',
            'type_label': 'Real Treasury',
            'date_column': 'record_date',
        },
    ]

    # ------------------------------------------------------------------
    # XML parsing
    # ------------------------------------------------------------------

    def _parse_xml(self, xml_bytes, prefix):
        """Parse Treasury XML feed and return list of row dicts.

        Args:
            xml_bytes: raw XML content (bytes).
            prefix: XML field prefix, e.g. ``'BC_'`` or ``'TC_'``.

        Returns:
            List of dicts with keys ``'Date'`` and each maturity field.
        """
        root = ET.fromstring(xml_bytes)
        xml_fields = [f'{prefix}{suffix}' for suffix in _MATURITY_SUFFIXES]
        data = []

        for entry in root.findall('.//atom:entry', _NS):
            content = entry.find('atom:content', _NS)
            if content is None:
                continue
            properties = content.find('m:properties', _NS)
            if properties is None:
                continue

            date_elem = properties.find('d:NEW_DATE', _NS)
            date_str = date_elem.text[:10] if date_elem is not None else None

            row = {'Date': date_str}
            for field in xml_fields:
                elem = properties.find(f'd:{field}', _NS)
                if elem is not None and elem.text:
                    try:
                        row[field] = float(elem.text)
                    except (ValueError, TypeError):
                        row[field] = None
                else:
                    row[field] = None
            data.append(row)

        return data

    def _to_db_rows(self, parsed_data, prefix, type_label):
        """Convert parsed XML data into DB-ready tuples.

        Returns:
            List of tuples matching (_ALL_COLUMNS_PLAIN order).
        """
        xml_fields = [f'{prefix}{suffix}' for suffix in _MATURITY_SUFFIXES]
        rows = []
        for item in parsed_data:
            values = [item['Date'], type_label]
            for field in xml_fields:
                v = item.get(field)
                # Ensure None for missing values (same behaviour as clean_value)
                if v is not None:
                    try:
                        values.append(float(v))
                    except (ValueError, TypeError):
                        values.append(None)
                else:
                    values.append(None)
            rows.append(tuple(values))
        return rows

    # ------------------------------------------------------------------
    # BaseFetcher interface
    # ------------------------------------------------------------------

    def fetch_series(self, series_config):
        table = series_config['table']
        create_sql = series_config['create_sql']
        url_template = series_config['url']
        prefix = series_config['prefix']
        type_label = series_config['type_label']
        date_column = series_config.get('date_column', 'record_date')

        conn = self.get_connection()
        try:
            self.ensure_table(conn, create_sql)
            latest_date = self.get_latest_date(conn, table, date_column)

            # Determine year range: if table is empty, backfill from 1990
            current_year = datetime.today().year
            if latest_date:
                start_year = pd.to_datetime(latest_date).year
            else:
                start_year = 1990

            all_parsed = []
            for year in range(start_year, current_year + 1):
                url = url_template.format(year=year)
                self.logger.info(f"  Requesting XML for year={year}: {url}")
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                parsed = self._parse_xml(resp.content, prefix)
                if parsed:
                    all_parsed.extend(parsed)
                self.rate_limit_pause(0.3)

            if not all_parsed:
                self.logger.warning(f"  No entries parsed from XML for {table}")
                return

            # Filter to only new rows
            if latest_date:
                latest_dt = pd.to_datetime(latest_date)
                all_parsed = [
                    r for r in all_parsed
                    if r['Date'] and pd.to_datetime(r['Date']) > latest_dt
                ]

            if not all_parsed:
                self.logger.info(f"  No new data for {table}")
                return

            parsed = all_parsed

            rows = self._to_db_rows(parsed, prefix, type_label)
            self.insert_ignore_rows(conn, table, _ALL_COLUMNS_PLAIN, rows)
        finally:
            conn.close()


if __name__ == '__main__':
    fetcher = TreasuryFetcher()
    fetcher.run()

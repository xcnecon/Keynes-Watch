"""BEAFetcher -- fetches NIPA data from the Bureau of Economic Analysis API.

Replicates the logic of ``nipa/kalecki_equation.py`` using the BaseFetcher
framework.  Instead of DROP TABLE, uses ``truncate_and_insert()`` for a
safe full-refresh strategy.
"""

import os
import sys

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from fetch_data.base import BaseFetcher

# BEA API endpoint
_BASE_URL = 'https://apps.bea.gov/api/data/'

# All NIPA queries needed for the Kalecki equation / three-sector balance
_QUERIES = [
    {'variable': 'Personal saving',
     'TableName': 'T50100', 'LineNumber': '9'},
    {'variable': 'Net government saving',
     'TableName': 'T50100', 'LineNumber': '10'},
    {'variable': 'Gross investment',
     'TableName': 'T50100', 'LineNumber': '21'},
    {'variable': 'Capital consumption',
     'TableName': 'T50100', 'LineNumber': '13'},
    {'variable': 'Foreign saving',
     'TableName': 'T40100', 'LineNumber': '33'},
    {'variable': 'Net dividend',
     'TableName': 'T11200', 'LineNumber': '16'},
    {'variable': 'Corporate net profit (with inventory/depreciation adjustment)',
     'TableName': 'T11200', 'LineNumber': '15'},
    {'variable': 'Statistical discrepancy',
     'TableName': 'T50100', 'LineNumber': '42'},
    {'variable': 'Nominal GDP',
     'TableName': 'T10105', 'LineNumber': '1'},
    {'variable': 'Private net lending',
     'TableName': 'T50100', 'LineNumber': '36'},
    {'variable': 'Government net lending',
     'TableName': 'T50100', 'LineNumber': '39'},
    {'variable': 'Foreign net lending',
     'TableName': 'T40100', 'LineNumber': '34'},
]

_KALECKI_REQUIRED_VARIABLES = [
    'Personal saving',
    'Corporate net profit (with inventory/depreciation adjustment)',
    'Net government saving',
    'Gross investment',
    'Foreign saving',
    'Capital consumption',
    'Statistical discrepancy',
    'Net dividend',
    'Nominal GDP',
]

_CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS kalecki_equation (
        id INT AUTO_INCREMENT PRIMARY KEY,
        variable VARCHAR(255) NOT NULL,
        year VARCHAR(10) NOT NULL,
        value DECIMAL(20,2) NOT NULL,
        is_annual BOOLEAN NOT NULL
    )
"""

_COLUMNS = ['variable', 'year', 'value', 'is_annual']


class BEAFetcher(BaseFetcher):
    """Fetches NIPA data from the BEA API for the Kalecki equation."""

    SERIES = [
        {
            'table': 'kalecki_equation',
            'create_sql': _CREATE_TABLE_SQL,
        },
    ]

    def __init__(self, name=None):
        super().__init__(name)
        self.api_key = os.getenv('BEA_API_KEY')
        if not self.api_key:
            self.logger.warning(
                'BEA_API_KEY not found in environment; BEA fetches will fail'
            )

    # ------------------------------------------------------------------
    # BEA API helpers
    # ------------------------------------------------------------------

    def _fetch_bea_table(self, table_name):
        """Fetch a single NIPA table from the BEA API.

        Returns:
            List of raw data entries for *table_name*.
        """
        params = {
            'UserID': self.api_key,
            'method': 'GetData',
            'DataSetName': 'NIPA',
            'TableName': table_name,
            'Frequency': 'A,Q',
            'Year': 'ALL',
            'ResultFormat': 'JSON',
        }
        headers = {
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'User-Agent': 'KeynesWatch data updater',
        }

        def _call():
            resp = self.session.get(
                _BASE_URL,
                params=params,
                headers=headers,
                verify=False,
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            beaapi = data.get('BEAAPI', {})
            error = beaapi.get('Error') or beaapi.get('Results', {}).get('Error')
            if error:
                raise RuntimeError(f'BEA API error for {table_name}: {error}')

            entries = (
                beaapi
                .get('Results', {})
                .get('Data', [])
            )
            if not entries:
                raise ValueError(f'BEA API returned no data for {table_name}')
            return entries

        return self.retry_call(
            _call,
            max_retries=3,
            backoff=3,
            label=f'BEA {table_name}',
        )

    # ------------------------------------------------------------------
    # BaseFetcher interface
    # ------------------------------------------------------------------

    def fetch_series(self, series_config):
        table = series_config['table']
        create_sql = series_config['create_sql']

        conn = self.get_connection()
        try:
            self.ensure_table(conn, create_sql)

            existing_latest_q = self._latest_complete_period_from_db(
                conn, is_annual=False
            )
            existing_latest_a = self._latest_complete_period_from_db(
                conn, is_annual=True
            )

            table_data = {}
            for table_name in sorted({query['TableName'] for query in _QUERIES}):
                self.logger.info(f'  Fetching BEA table {table_name} ...')
                table_data[table_name] = self._fetch_bea_table(table_name)
                self.rate_limit_pause(0.3)

            all_rows = []
            missing_lines = []
            for query in _QUERIES:
                variable = query['variable']
                raw = [
                    entry for entry in table_data[query['TableName']]
                    if entry.get('LineNumber') == str(query['LineNumber'])
                ]
                if not raw:
                    missing_lines.append(
                        f"{variable} ({query['TableName']} line {query['LineNumber']})"
                    )
                    continue

                for entry in raw:
                    time_period = entry['TimePeriod']
                    raw_value = entry['DataValue'].replace(',', '')
                    unit_mult = int(entry.get('UNIT_MULT', 0))

                    try:
                        value_adjusted = float(raw_value) * (10 ** unit_mult)
                    except ValueError:
                        self.logger.warning(
                            f'  Invalid value for {variable} in '
                            f'{time_period}: {raw_value}'
                        )
                        continue

                    is_annual = len(time_period) == 4
                    all_rows.append((
                        variable,
                        time_period,
                        value_adjusted,
                        is_annual,
                    ))

            if missing_lines:
                raise RuntimeError(
                    'BEA response missing required lines; refusing to overwrite '
                    f'{table}: ' + '; '.join(missing_lines)
                )

            if not all_rows:
                self.logger.warning('  No BEA data collected; skipping insert')
                return

            latest_q = self._latest_complete_period(
                all_rows,
                _KALECKI_REQUIRED_VARIABLES,
                is_annual=False,
            )
            latest_a = self._latest_complete_period(
                all_rows,
                _KALECKI_REQUIRED_VARIABLES,
                is_annual=True,
            )
            self._raise_if_period_regressed(
                'quarterly Kalecki',
                latest_q,
                existing_latest_q,
            )
            self._raise_if_period_regressed(
                'annual Kalecki',
                latest_a,
                existing_latest_a,
            )

            self.logger.info(f'  Latest complete Kalecki quarter: {latest_q}')
            self.logger.info(f'  Latest complete Kalecki year: {latest_a}')

            # Full refresh: truncate existing data, then insert all rows
            self.truncate_and_insert(conn, table, _COLUMNS, all_rows)
        finally:
            conn.close()

    def _latest_complete_period_from_db(self, conn, is_annual):
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT variable, `year`, is_annual
                FROM kalecki_equation
                WHERE is_annual = %s
                """,
                (is_annual,),
            )
            rows = [
                (variable, year, None, bool(row_is_annual))
                for variable, year, row_is_annual in cursor.fetchall()
            ]
            return self._latest_complete_period(
                rows,
                _KALECKI_REQUIRED_VARIABLES,
                is_annual=is_annual,
            )
        finally:
            cursor.close()

    @classmethod
    def _latest_complete_period(cls, rows, variables, is_annual):
        periods_by_variable = {variable: set() for variable in variables}
        for variable, period, _value, row_is_annual in rows:
            if bool(row_is_annual) != bool(is_annual):
                continue
            if variable in periods_by_variable:
                periods_by_variable[variable].add(str(period))

        if not periods_by_variable:
            return None
        common_periods = None
        for periods in periods_by_variable.values():
            if not periods:
                return None
            common_periods = periods if common_periods is None else common_periods & periods
        if not common_periods:
            return None
        return max(common_periods, key=cls._period_sort_key)

    @staticmethod
    def _period_sort_key(period):
        period = str(period)
        if 'Q' in period:
            year, quarter = period.split('Q', 1)
            return int(year), int(quarter)
        return int(period), 0

    @classmethod
    def _raise_if_period_regressed(cls, label, new_period, existing_period):
        if not new_period or not existing_period:
            return
        if cls._period_sort_key(new_period) < cls._period_sort_key(existing_period):
            raise RuntimeError(
                f'Latest complete {label} period would regress from '
                f'{existing_period} to {new_period}; refusing to overwrite table'
            )


if __name__ == '__main__':
    fetcher = BEAFetcher()
    fetcher.run()

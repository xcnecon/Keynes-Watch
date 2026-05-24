"""
FiscalDataFetcher -- unified fetcher for US Treasury Fiscal Data API series.

Replaces the following standalone scripts:
  - fiscal_data/tga_balance.py
  - fiscal_data/debt_limit.py
  - fiscal_data/total_treasury_outstanding.py
  - fiscal_data/average_treasury_maturity.py
  - fiscal_data/average_treasury_yields.py
  - fiscal_data/mts.py
  - fiscal_data/withheld_tax.py
"""

import os
import sys
import argparse
from datetime import datetime

import pandas as pd

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from fetch_data.base import BaseFetcher


# ---------------------------------------------------------------------------
# Value-cleaning helpers (inlined from common/functions.py)
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


def clean_value_zero(value, data_type="text"):
    """Like clean_value but returns 0 for missing numeric values."""
    if pd.isna(value) if not isinstance(value, str) else False:
        return 0 if data_type == "numeric" else None
    if value in (None, 'null', '', ' ', '*', 'nan', 'NaN'):
        return 0 if data_type == "numeric" else None
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

SERIES_TGA_BALANCE = {
    'table': 'tga_balance',
    'create_sql': """
        CREATE TABLE IF NOT EXISTS tga_balance (
            record_date DATE,
            account_type VARCHAR(100),
            open_today_bal DECIMAL(18,2)
        )
    """,
    'endpoint': '/v1/accounting/dts/operating_cash_balance',
    'fields': 'record_date,account_type,open_today_bal',
    'columns': ['record_date', 'account_type', 'open_today_bal'],
    'column_types': {
        'record_date': 'date',
        'account_type': 'text',
        'open_today_bal': 'text',
    },
    'strategy': 'incremental',
    'date_column': 'record_date',
}

SERIES_DEBT_LIMIT = {
    'table': 'debt_limit',
    'create_sql': """
        CREATE TABLE IF NOT EXISTS debt_limit (
            record_date DATE,
            debt_limit_desc VARCHAR(100),
            debt_limit_class1_desc VARCHAR(100),
            debt_held_public_mil_amt DECIMAL(18,3),
            intragov_hold_mil_amt DECIMAL(18,3),
            total_mil_amt DECIMAL(18,3)
        )
    """,
    'endpoint': '/v1/debt/mspd/mspd_table_2',
    'fields': 'record_date,debt_limit_desc,debt_limit_class1_desc,debt_held_public_mil_amt,intragov_hold_mil_amt,total_mil_amt',
    'columns': ['record_date', 'debt_limit_desc', 'debt_limit_class1_desc',
                'debt_held_public_mil_amt', 'intragov_hold_mil_amt', 'total_mil_amt'],
    'column_types': {
        'record_date': 'date',
        'debt_limit_desc': 'text',
        'debt_limit_class1_desc': 'text',
        'debt_held_public_mil_amt': 'numeric',
        'intragov_hold_mil_amt': 'numeric',
        'total_mil_amt': 'numeric',
    },
    'strategy': 'incremental',
    'date_column': 'record_date',
}

SERIES_TOTAL_DEBT_OUTSTANDING = {
    'table': 'total_debt_outstanding',
    'create_sql': """
        CREATE TABLE IF NOT EXISTS total_debt_outstanding (
            id INT AUTO_INCREMENT PRIMARY KEY,
            record_date DATE,
            security_type_desc VARCHAR(255),
            security_class_desc VARCHAR(255),
            debt_held_public_mil_amt DECIMAL(15, 5),
            intragov_hold_mil_amt DECIMAL(15, 5),
            total_mil_amt DECIMAL(15, 5)
        )
    """,
    'endpoint': '/v1/debt/mspd/mspd_table_1',
    'fields': 'record_date,security_type_desc,security_class_desc,debt_held_public_mil_amt,intragov_hold_mil_amt,total_mil_amt',
    'columns': ['record_date', 'security_type_desc', 'security_class_desc',
                'debt_held_public_mil_amt', 'intragov_hold_mil_amt', 'total_mil_amt'],
    'strategy': 'incremental',
    'date_column': 'record_date',
    'handler': '_handle_total_debt_outstanding',
}

SERIES_AVERAGE_MATURITY = {
    'table': 'treasury_average_maturity',
    'create_sql': """
        CREATE TABLE IF NOT EXISTS treasury_average_maturity (
            id INT AUTO_INCREMENT PRIMARY KEY,
            record_date DATE,
            security_type_desc VARCHAR(255),
            security_class1_desc VARCHAR(255),
            security_class2_desc VARCHAR(255),
            maturity_date DATE,
            amount DECIMAL(20,2),
            maturity DECIMAL(20,2),
            weight DECIMAL(20,8),
            weight_by_type DECIMAL(20,8)
        )
    """,
    'endpoint': '/v1/debt/mspd/mspd_table_3_market',
    'fields': 'record_date,security_type_desc,security_class1_desc,security_class2_desc,maturity_date,issued_amt,inflation_adj_amt,redeemed_amt',
    'strategy': 'incremental',
    'date_column': 'record_date',
    'handler': '_handle_average_maturity',
}

SERIES_AVERAGE_YIELDS = {
    'table': 'treasuries_average_yields',
    'create_sql': """
        CREATE TABLE IF NOT EXISTS treasuries_average_yields (
            id INT AUTO_INCREMENT PRIMARY KEY,
            record_date DATE,
            security_type_desc VARCHAR(255),
            security_desc VARCHAR(255),
            avg_interest_rate_amt DECIMAL(10, 5)
        )
    """,
    'endpoint': '/v2/accounting/od/avg_interest_rates',
    'fields': 'record_date,security_type_desc,security_desc,avg_interest_rate_amt',
    'columns': ['record_date', 'security_type_desc', 'security_desc', 'avg_interest_rate_amt'],
    'column_types': {
        'record_date': 'date',
        'security_type_desc': 'text',
        'security_desc': 'text',
        'avg_interest_rate_amt': 'numeric',
    },
    'strategy': 'incremental',
    'date_column': 'record_date',
}

SERIES_MTS = {
    'table': 'mts',
    'create_sql': """
        CREATE TABLE IF NOT EXISTS mts (
            record_date DATE PRIMARY KEY,
            receipts DECIMAL(20, 5),
            outlays DECIMAL(20, 5),
            deficit DECIMAL(20, 5)
        )
    """,
    'endpoint': '/v1/accounting/mts/mts_table_1',
    'fields': 'record_date,classification_desc,record_calendar_year,current_month_gross_rcpt_amt,current_month_gross_outly_amt,current_month_dfct_sur_amt',
    'strategy': 'incremental',
    'date_column': 'record_date',
    'handler': '_handle_mts',
}

SERIES_WITHHELD_TAX = {
    'table': 'withheld_tax',
    'create_sql': """
        CREATE TABLE IF NOT EXISTS withheld_tax (
            record_date DATE PRIMARY KEY,
            amount DECIMAL(15, 5),
            t2 DECIMAL(15, 5),
            t5 DECIMAL(15, 5)
        )
    """,
    'endpoint_t2': '/v1/accounting/dts/deposits_withdrawals_operating_cash',
    'fields_t2': 'record_date,transaction_catg,transaction_today_amt',
    'endpoint_t5': '/v1/accounting/dts/inter_agency_tax_transfers',
    'fields_t5': 'record_date,classification,today_amt',
    'strategy': 'incremental',
    'date_column': 'record_date',
    'handler': '_handle_withheld_tax',
}


# ---------------------------------------------------------------------------
# FiscalDataFetcher
# ---------------------------------------------------------------------------

class FiscalDataFetcher(BaseFetcher):
    """Fetcher for the US Treasury Fiscal Data API."""

    BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"

    SERIES = [
        SERIES_TGA_BALANCE,
        SERIES_DEBT_LIMIT,
        SERIES_TOTAL_DEBT_OUTSTANDING,
        SERIES_AVERAGE_MATURITY,
        SERIES_AVERAGE_YIELDS,
        SERIES_MTS,
        SERIES_WITHHELD_TAX,
    ]

    # -- Fiscal Data API pagination -----------------------------------------

    def _fetch_fiscal_data(self, endpoint, fields, since=None):
        """Fetch all pages from a Fiscal Data API endpoint.

        Returns a list of record dicts.
        """
        url = self.BASE_URL + endpoint
        all_data = []
        page = 1

        params = {
            'page[number]': page,
            'page[size]': 5000,
            'fields': fields,
        }
        if since:
            params['filter'] = f"record_date:gt:{since}"

        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        body = resp.json()

        if not body.get('data'):
            self.logger.info("  No new data from %s", endpoint)
            return []

        total_pages = body.get('meta', {}).get('total-pages', 1)
        self.logger.info("  %s: %d page(s) to fetch", endpoint, total_pages)
        all_data.extend(body['data'])
        page += 1

        while page <= total_pages:
            params['page[number]'] = page
            resp = self.session.get(url, params=params)
            resp.raise_for_status()
            body = resp.json()
            all_data.extend(body['data'])
            self.logger.info("  Fetched page %d/%d", page, total_pages)
            page += 1

        self.logger.info("  Total records fetched: %d", len(all_data))
        return all_data

    # -- Main dispatch ------------------------------------------------------

    def fetch_series(self, series_config):
        handler_name = series_config.get('handler')
        if handler_name:
            getattr(self, handler_name)(series_config)
        else:
            self._handle_generic(series_config)

    # -- Generic handler (tga_balance, debt_limit, average_yields) ----------

    def _handle_generic(self, config):
        conn = self.get_connection()
        try:
            self.ensure_table(conn, config['create_sql'])
            latest_date = self.get_latest_date(conn, config['table'],
                                               config.get('date_column', 'record_date'))

            data = self._fetch_fiscal_data(config['endpoint'], config['fields'],
                                           since=latest_date)
            if not data:
                return

            col_types = config.get('column_types', {})
            columns = config['columns']
            rows = []
            for record in data:
                row = []
                for col in columns:
                    dtype = col_types.get(col, 'text')
                    row.append(clean_value(record.get(col), data_type=dtype))
                rows.append(tuple(row))

            self.insert_ignore_rows(conn, config['table'], columns, rows)
        finally:
            conn.close()

    # -- total_debt_outstanding handler -------------------------------------

    def _handle_total_debt_outstanding(self, config):
        conn = self.get_connection()
        try:
            self.ensure_table(conn, config['create_sql'])
            latest_date = self.get_latest_date(conn, config['table'],
                                               config.get('date_column', 'record_date'))

            data = self._fetch_fiscal_data(config['endpoint'], config['fields'],
                                           since=latest_date)
            if not data:
                return

            columns = ['record_date', 'security_type_desc', 'security_class_desc',
                       'debt_held_public_mil_amt', 'intragov_hold_mil_amt', 'total_mil_amt']
            rows = []
            for record in data:
                record_date = clean_value(record.get("record_date"), data_type='date')
                security_type_desc = clean_value(record.get("security_type_desc"), data_type='text')
                security_class_desc = clean_value(record.get("security_class_desc"), data_type='text')
                debt_held = clean_value(record.get("debt_held_public_mil_amt"), data_type='numeric')
                intragov = clean_value(record.get("intragov_hold_mil_amt"), data_type='numeric')
                total = clean_value(record.get("total_mil_amt"), data_type='numeric')

                # Filter: keep only specific marketable categories and total
                if (security_type_desc == "Marketable" and
                        security_class_desc in ["Bills", "Notes", "Bonds",
                                                "Treasury Inflation-Protected Securities"]):
                    pass
                elif security_type_desc == "Total Public Debt Outstanding":
                    security_class_desc = security_type_desc
                else:
                    continue

                rows.append((record_date, security_type_desc, security_class_desc,
                             debt_held, intragov, total))

            self.insert_ignore_rows(conn, config['table'], columns, rows)
        finally:
            conn.close()

    # -- treasury_average_maturity handler ----------------------------------

    def _handle_average_maturity(self, config):
        conn = self.get_connection()
        try:
            self.ensure_table(conn, config['create_sql'])
            latest_date = self.get_latest_date(conn, config['table'],
                                               config.get('date_column', 'record_date'))

            data = self._fetch_fiscal_data(config['endpoint'], config['fields'],
                                           since=latest_date)
            if not data:
                return

            cursor = conn.cursor()
            try:
                insert_sql = """
                    INSERT INTO treasury_average_maturity (
                        record_date, security_type_desc, security_class1_desc,
                        security_class2_desc, maturity_date, amount, maturity,
                        weight, weight_by_type
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, NULL)
                """

                tips_sum_by_date = {}

                for record in data:
                    record_date = clean_value_zero(record.get("record_date"), data_type="date")
                    security_type_desc = clean_value_zero(record.get("security_type_desc"), data_type="text")
                    security_class1_desc = clean_value_zero(record.get("security_class1_desc"), data_type="text")
                    security_class2_desc = clean_value_zero(record.get("security_class2_desc"), data_type="text")
                    maturity_date = clean_value_zero(record.get("maturity_date"), data_type="date")
                    issued_amt = clean_value_zero(record.get("issued_amt"), data_type="numeric")
                    inflation_adj_amt = clean_value_zero(record.get("inflation_adj_amt"), data_type="numeric")
                    redeemed_amt = clean_value_zero(record.get("redeemed_amt"), data_type="numeric")

                    # Skip records with "Unmatured" or "Matured"
                    if security_class2_desc and any(
                        kw in security_class2_desc for kw in ["Unmatured", "Matured"]
                    ):
                        continue

                    # Skip Federal Financing Bank
                    if security_class1_desc and "Federal Financing Bank" in security_class1_desc:
                        continue

                    # Skip specific TIPS total entries
                    if security_class2_desc and any(
                        kw in security_class2_desc for kw in [
                            "Total Treasury Inflation-Indexed Notes",
                            "Total Treasury Inflation-Indexed Bonds",
                            "Total Treasury Inflation-Protected Securities",
                            "Total Treasury TIPS",
                            "Total Treasury Floating Rate Notes",
                        ]
                    ):
                        continue

                    # Merge Inflation-Indexed Notes/Bonds into Inflation-Protected Securities
                    if security_class1_desc in [
                        "Inflation-Indexed Notes", "Inflation-Indexed Bonds",
                        "Inflation-Protected Securities",
                    ]:
                        security_class1_desc = "Inflation-Protected Securities"
                        security_class2_desc = None
                        tips_sum_by_date[record_date] = tips_sum_by_date.get(record_date, 0) + (
                            issued_amt + inflation_adj_amt + redeemed_amt
                        )

                    amount = issued_amt + inflation_adj_amt + redeemed_amt

                    # Calculate maturity in years
                    if maturity_date and record_date:
                        maturity = (
                            datetime.strptime(maturity_date, "%Y-%m-%d").date()
                            - datetime.strptime(record_date, "%Y-%m-%d").date()
                        ).days / 365
                    else:
                        maturity = None

                    cursor.execute(insert_sql, (
                        record_date, security_type_desc, security_class1_desc,
                        security_class2_desc, maturity_date, amount, maturity,
                    ))

                # Insert TIPS summary rows
                for rd, tips_total in tips_sum_by_date.items():
                    cursor.execute(insert_sql, (
                        rd, "Marketable", "Inflation-Protected Securities",
                        "Total Treasury Inflation-Protected Securities",
                        None, tips_total, None,
                    ))

                conn.commit()
                self.logger.info("  Inserted raw maturity data")

                # Update security_class2_desc for Total Marketable rows
                cursor.execute("""
                    UPDATE treasury_average_maturity
                    SET security_class2_desc = 'Total Marketable'
                    WHERE security_class1_desc = 'Total Marketable'
                """)
                conn.commit()

                # --- Calculate weights ---
                self.logger.info("  Calculating weights...")
                cursor.execute("SELECT DISTINCT record_date FROM treasury_average_maturity")
                record_dates = cursor.fetchall()

                for (rd_val,) in record_dates:
                    cursor.execute("""
                        SELECT amount FROM treasury_average_maturity
                        WHERE record_date = %s AND security_class1_desc = 'Total Marketable'
                    """, (rd_val,))
                    result = cursor.fetchone()
                    if not result:
                        continue
                    total_marketable = result[0]

                    # Totals by type
                    cursor.execute("""
                        SELECT security_class2_desc, amount FROM treasury_average_maturity
                        WHERE record_date = %s AND security_class2_desc IN (
                            'Total Treasury Bonds',
                            'Total Treasury Inflation-Protected Securities',
                            'Total Treasury Notes',
                            'Total Treasury Bills'
                        )
                    """, (rd_val,))
                    totals_by_type = {row[0]: row[1] for row in cursor.fetchall()}

                    # Individual records
                    cursor.execute("""
                        SELECT id, amount, security_class1_desc FROM treasury_average_maturity
                        WHERE record_date = %s AND security_class1_desc NOT LIKE 'Total%%'
                    """, (rd_val,))
                    ind_rows = cursor.fetchall()

                    type_map = {
                        'Bonds': 'Total Treasury Bonds',
                        'Notes': 'Total Treasury Notes',
                        'Bills Maturity Value': 'Total Treasury Bills',
                        'Inflation-Protected Securities': 'Total Treasury Inflation-Protected Securities',
                    }

                    for row_id, amount, sc1 in ind_rows:
                        total_type_key = type_map.get(sc1)
                        weight = amount / total_marketable if total_marketable else None
                        weight_by_type = (
                            amount / totals_by_type.get(total_type_key, 0)
                            if total_type_key and total_type_key in totals_by_type else None
                        )
                        cursor.execute("""
                            UPDATE treasury_average_maturity
                            SET weight = %s, weight_by_type = %s
                            WHERE id = %s
                        """, (weight, weight_by_type, row_id))

                conn.commit()
                self.logger.info("  Weights calculated")

                # --- Calculate weighted-average maturities ---
                self.logger.info("  Calculating weighted-average maturities...")
                for (rd_val,) in record_dates:
                    cursor.execute("""
                        SELECT amount, maturity FROM treasury_average_maturity
                        WHERE record_date = %s AND maturity IS NOT NULL
                    """, (rd_val,))
                    mat_rows = cursor.fetchall()
                    total_amount = sum(r[0] for r in mat_rows)
                    weighted_maturity = (
                        sum((r[0] / total_amount) * r[1] for r in mat_rows)
                        if total_amount else None
                    )
                    cursor.execute("""
                        UPDATE treasury_average_maturity
                        SET maturity = %s
                        WHERE record_date = %s AND security_class1_desc = 'Total Marketable'
                    """, (weighted_maturity, rd_val))

                    type_map_mat = {
                        'Bonds': 'Total Treasury Bonds',
                        'Notes': 'Total Treasury Notes',
                        'Bills Maturity Value': 'Total Treasury Bills',
                        'Inflation-Protected Securities': 'Total Treasury Inflation-Protected Securities',
                    }
                    for sec_type, total_desc in type_map_mat.items():
                        cursor.execute("""
                            SELECT amount, maturity FROM treasury_average_maturity
                            WHERE record_date = %s AND maturity IS NOT NULL
                              AND security_class1_desc = %s
                        """, (rd_val, sec_type))
                        type_rows = cursor.fetchall()
                        total_by_type = sum(r[0] for r in type_rows)
                        wm_by_type = (
                            sum((r[0] / total_by_type) * r[1] for r in type_rows)
                            if total_by_type else None
                        )
                        cursor.execute("""
                            UPDATE treasury_average_maturity
                            SET maturity = %s
                            WHERE record_date = %s AND security_class2_desc = %s
                        """, (wm_by_type, rd_val, total_desc))

                conn.commit()
                self.logger.info("  Weighted-average maturities calculated")
            finally:
                cursor.close()
        finally:
            conn.close()

    # -- mts handler --------------------------------------------------------

    def _handle_mts(self, config):
        conn = self.get_connection()
        try:
            self.ensure_table(conn, config['create_sql'])

            # Ensure column widths
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    ALTER TABLE mts
                    MODIFY COLUMN receipts DECIMAL(20,5),
                    MODIFY COLUMN outlays DECIMAL(20,5),
                    MODIFY COLUMN deficit DECIMAL(20,5)
                """)
                conn.commit()
            except Exception:
                pass
            finally:
                cursor.close()

            latest_date = self.get_latest_date(conn, config['table'],
                                               config.get('date_column', 'record_date'))

            # Seed from CSV if empty
            if latest_date is None:
                latest_date = self._seed_mts_from_csv(conn, config['table'])

            data = self._fetch_fiscal_data(config['endpoint'], config['fields'],
                                           since=latest_date)
            if not data:
                return

            df = pd.DataFrame(data)
            if df.empty:
                return

            df['record_date'] = pd.to_datetime(df['record_date'], errors='coerce')
            df = df.dropna(subset=['record_date'])
            df['record_year'] = df['record_date'].dt.year
            df['month_name'] = df['record_date'].dt.strftime('%B')

            df['record_calendar_year'] = pd.to_numeric(
                df.get('record_calendar_year'), errors='coerce'
            )
            df = df[
                (df['classification_desc'] == df['month_name'])
                & (df['record_calendar_year'] == df['record_year'])
            ]

            df['receipts'] = pd.to_numeric(
                df.get('current_month_gross_rcpt_amt'), errors='coerce'
            ).fillna(0.0)
            df['outlays'] = pd.to_numeric(
                df.get('current_month_gross_outly_amt'), errors='coerce'
            ).fillna(0.0)
            df['deficit'] = pd.to_numeric(
                df.get('current_month_dfct_sur_amt'), errors='coerce'
            )
            df['deficit'] = df['deficit'].where(
                df['deficit'].notna(), df['outlays'] - df['receipts']
            )

            # Scale to millions
            scale = 1_000_000.0
            df['receipts'] = df['receipts'] / scale
            df['outlays'] = df['outlays'] / scale
            df['deficit'] = df['deficit'] / scale

            df['record_date'] = df['record_date'].dt.date
            final_df = df[['record_date', 'receipts', 'outlays', 'deficit']].drop_duplicates(
                subset=['record_date'], keep='last'
            )

            rows = list(final_df.itertuples(index=False, name=None))
            columns = ['record_date', 'receipts', 'outlays', 'deficit']
            self.upsert_rows(conn, config['table'], columns, rows,
                             on_duplicate_update=['receipts', 'outlays', 'deficit'])
        finally:
            conn.close()

    def _seed_mts_from_csv(self, conn, table_name):
        """Seed mts table from local CSV file. Returns latest_date or None."""
        csv_path = os.path.join(os.path.dirname(__file__), '..', 'fiscal_data', 'mts.csv')
        if not os.path.exists(csv_path):
            self.logger.warning("CSV file not found at %s; skipping seed.", csv_path)
            return None

        try:
            seed_df = pd.read_csv(csv_path)
            seed_df['receipts'] = pd.to_numeric(seed_df.get('receipts'), errors='coerce').fillna(0.0)
            seed_df['outlays'] = pd.to_numeric(seed_df.get('outlays'), errors='coerce').fillna(0.0)
            if 'deficit' in seed_df.columns:
                seed_df['deficit'] = pd.to_numeric(seed_df['deficit'], errors='coerce')
            else:
                seed_df['deficit'] = None
            seed_df['deficit'] = seed_df['deficit'].fillna(seed_df['outlays'] - seed_df['receipts'])

            seed_df['record_date'] = pd.to_datetime(
                seed_df['record_date'], errors='coerce'
            ).dt.date
            seed_df = seed_df.dropna(subset=['record_date'])
            seed_df = seed_df[['record_date', 'receipts', 'outlays', 'deficit']]

            rows = list(seed_df.itertuples(index=False, name=None))
            if rows:
                columns = ['record_date', 'receipts', 'outlays', 'deficit']
                self.upsert_rows(conn, table_name, columns, rows,
                                 on_duplicate_update=['receipts', 'outlays', 'deficit'])
                self.logger.info("Seeded %d rows from CSV into %s", len(rows), table_name)

            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT MAX(record_date) FROM {table_name}")
                result = cursor.fetchone()
                return result[0] if result else None
            finally:
                cursor.close()
        except Exception as e:
            self.logger.error("CSV seed failed: %s", e)
            conn.rollback()
            return None

    # -- withheld_tax handler -----------------------------------------------

    def _handle_withheld_tax(self, config):
        conn = self.get_connection()
        try:
            self.ensure_table(conn, config['create_sql'])
            latest_date = self.get_latest_date(conn, config['table'],
                                               config.get('date_column', 'record_date'))

            # Seed from CSV if empty
            if latest_date is None:
                latest_date = self._seed_withheld_tax_from_csv(conn, config['table'])

            date_filter_val = latest_date

            # Fetch from two endpoints
            t2_data = self._fetch_fiscal_data(config['endpoint_t2'], config['fields_t2'],
                                              since=date_filter_val)
            t5_data = self._fetch_fiscal_data(config['endpoint_t5'], config['fields_t5'],
                                              since=date_filter_val)

            # Process t2 (deposits/withdrawals)
            t2_df = pd.DataFrame(t2_data) if t2_data else pd.DataFrame()
            if not t2_df.empty and 'transaction_catg' in t2_df.columns:
                t2_df = t2_df[t2_df['transaction_catg'] == 'Taxes - Withheld Individual/FICA']
                if 'transaction_today_amt' in t2_df.columns:
                    t2_df = t2_df.rename(columns={'transaction_today_amt': 't2'})
            else:
                t2_df = pd.DataFrame(columns=['record_date', 't2'])

            # Process t5 (inter-agency)
            t5_df = pd.DataFrame(t5_data) if t5_data else pd.DataFrame()
            if not t5_df.empty and 'classification' in t5_df.columns:
                t5_df = t5_df[t5_df['classification'] == 'Taxes - Withheld Individual/FICA']
                if 'today_amt' in t5_df.columns:
                    t5_df = t5_df.rename(columns={'today_amt': 't5'})
            else:
                t5_df = pd.DataFrame(columns=['record_date', 't5'])

            if t2_df.empty or t5_df.empty:
                self.logger.info("  No withheld_tax data to merge")
                return

            df = t2_df.merge(t5_df, on='record_date', how='inner')
            df = df[['record_date', 't2', 't5']].copy()
            df['t2'] = pd.to_numeric(df['t2'], errors='coerce').fillna(0.0)
            df['t5'] = pd.to_numeric(df['t5'], errors='coerce').fillna(0.0)
            df['amount'] = df['t2'] + df['t5']
            df['record_date'] = pd.to_datetime(df['record_date']).dt.date

            rows = list(df[['record_date', 'amount', 't2', 't5']].itertuples(
                index=False, name=None
            ))
            columns = ['record_date', 'amount', 't2', 't5']
            self.upsert_rows(conn, config['table'], columns, rows,
                             on_duplicate_update=['amount', 't2', 't5'])
        finally:
            conn.close()

    def _seed_withheld_tax_from_csv(self, conn, table_name):
        """Seed withheld_tax table from local CSV. Returns latest_date or None."""
        csv_path = os.path.join(os.path.dirname(__file__), '..', 'fiscal_data', 'withheld_tax.csv')
        if not os.path.exists(csv_path):
            self.logger.warning("CSV file not found at %s; skipping seed.", csv_path)
            return None

        try:
            seed_df = pd.read_csv(csv_path)
            col_map = {'tax': 't2', 'interagency': 't5'}
            seed_df = seed_df.rename(columns=col_map)

            if 't2' not in seed_df.columns and 'tax' in seed_df.columns:
                seed_df['t2'] = seed_df['tax']
            if 't5' not in seed_df.columns and 'interagency' in seed_df.columns:
                seed_df['t5'] = seed_df['interagency']

            seed_df['t2'] = pd.to_numeric(seed_df.get('t2'), errors='coerce').fillna(0.0)
            seed_df['t5'] = pd.to_numeric(seed_df.get('t5'), errors='coerce').fillna(0.0)
            if 'amount' not in seed_df.columns:
                seed_df['amount'] = seed_df['t2'] + seed_df['t5']
            else:
                seed_df['amount'] = pd.to_numeric(
                    seed_df['amount'], errors='coerce'
                ).fillna(seed_df['t2'] + seed_df['t5'])

            seed_df['record_date'] = pd.to_datetime(
                seed_df['record_date'], errors='coerce'
            ).dt.date
            seed_df = seed_df.dropna(subset=['record_date'])
            seed_df = seed_df[['record_date', 'amount', 't2', 't5']]

            rows = list(seed_df.itertuples(index=False, name=None))
            if rows:
                columns = ['record_date', 'amount', 't2', 't5']
                self.upsert_rows(conn, table_name, columns, rows,
                                 on_duplicate_update=['amount', 't2', 't5'])
                self.logger.info("Seeded %d rows from CSV into %s", len(rows), table_name)

            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT MAX(record_date) FROM {table_name}")
                result = cursor.fetchone()
                return result[0] if result else None
            finally:
                cursor.close()
        except Exception as e:
            self.logger.error("CSV seed failed: %s", e)
            conn.rollback()
            return None


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Fetch Fiscal Data API series")
    parser.add_argument('--series', type=str, default=None,
                        help="Only fetch series whose table name contains this string")
    args = parser.parse_args()

    fetcher = FiscalDataFetcher()
    success = fetcher.run(series_filter=args.series)
    raise SystemExit(0 if success else 1)

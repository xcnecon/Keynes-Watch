"""
Base fetcher class for KeynesWatch data collection.

Provides: DB connection pooling, HTTP retry with backoff, unified logging,
incremental fetch detection, and upsert/insert helpers.

Usage:
    class MyFetcher(BaseFetcher):
        SERIES = [{'table': '...', ...}]
        def fetch_series(self, series_config):
            ...
"""

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool

load_dotenv()

_db_pool = None
_db_pool_lock = threading.Lock()


def _quote_identifier(identifier):
    """Quote a MySQL identifier used by internal fetcher configs."""
    return f"`{str(identifier).replace('`', '``')}`"


def _transaction_connection(conn):
    """Return the object that owns transaction state for a DB connection.

    PooledMySQLConnection proxies attribute *reads* to the underlying
    connection, but assigning ``conn.autocommit = ...`` on the pooled wrapper
    creates an instance attribute instead of toggling the real connection.
    """
    return getattr(conn, '_cnx', conn)


def _db_config_from_env():
    """Build a MySQL config from environment variables."""
    required = {
        'DB_HOST': 'host',
        'DB_USER': 'user',
        'DB_PASSWORD': 'password',
        'DB_NAME': 'database',
    }
    missing = [env_name for env_name in required if not os.getenv(env_name)]
    if missing:
        raise RuntimeError(
            'Missing required database environment variables: '
            + ', '.join(missing)
        )

    config = {
        option_name: os.getenv(env_name)
        for env_name, option_name in required.items()
    }
    if os.getenv('DB_PORT'):
        config['port'] = int(os.getenv('DB_PORT'))
    return config


# ---------------------------------------------------------------------------
# Shared database pool (lazy-initialized, shared across all fetchers)
# ---------------------------------------------------------------------------
def get_db_pool():
    """Get or create the shared connection pool."""
    global _db_pool
    if _db_pool is None:
        with _db_pool_lock:
            if _db_pool is None:
                _db_pool = MySQLConnectionPool(
                    pool_name="fetcher_pool",
                    pool_size=5,
                    **_db_config_from_env(),
                )
    return _db_pool


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )


# ---------------------------------------------------------------------------
# BaseFetcher
# ---------------------------------------------------------------------------
class BaseFetcher:
    """Base class for all data fetchers.

    Subclasses must:
    1. Define SERIES (list of dicts, each describing one table/series).
    2. Override fetch_series(series_config) with source-specific logic.
    """

    # Subclass should override with list of series config dicts
    SERIES = []
    MAX_WORKERS = 1  # Override in subclass for parallel series execution

    def __init__(self, name=None):
        self.name = name or self.__class__.__name__
        self.logger = logging.getLogger(self.name)
        self.session = self._create_session()

    # -- HTTP session with automatic retry ---------------------------------

    def _create_session(self):
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _setup_cn_proxy(self):
        """Configure CN proxy from CN_PROXY env var.

        Call in __init__ for fetchers that access Chinese government websites.
        Sets self.session.proxies and self._cn_proxies (for direct requests.get calls).
        """
        proxy = os.getenv('CN_PROXY', '')
        if proxy:
            self._cn_proxies = {'https': proxy, 'http': proxy}
            self.session.proxies.update(self._cn_proxies)
            self.logger.info("Using CN proxy from CN_PROXY")
        else:
            self._cn_proxies = None

    # -- Database helpers ---------------------------------------------------

    def get_connection(self):
        """Get a connection from the shared pool."""
        return get_db_pool().get_connection()

    def ensure_table(self, conn, create_sql):
        """Execute a CREATE TABLE IF NOT EXISTS statement."""
        cursor = conn.cursor()
        try:
            cursor.execute(create_sql)
            conn.commit()
        finally:
            cursor.close()

    def get_latest_date(self, conn, table_name, date_column='record_date'):
        """Get the most recent date in a table (for incremental fetch)."""
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT MAX({date_column}) FROM {table_name}")
            result = cursor.fetchone()
            return result[0] if result and result[0] else None
        except mysql.connector.Error:
            return None
        finally:
            cursor.close()

    def upsert_rows(self, conn, table_name, columns, rows,
                     on_duplicate_update=None, coalesce=False):
        """INSERT ... ON DUPLICATE KEY UPDATE.

        Args:
            on_duplicate_update: list of column names to update on conflict.
                If None, does a plain INSERT (may fail on duplicates).
            coalesce: if True, update columns with COALESCE(VALUES(col), col)
                so an incoming NULL never wipes an existing value (protects
                against transient upstream blanks on re-fetched rows).
        """
        if not rows:
            return 0
        placeholders = ', '.join(['%s'] * len(columns))
        col_names = ', '.join(columns)
        sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"
        if on_duplicate_update:
            tpl = ("{col} = COALESCE(VALUES({col}), {col})" if coalesce
                   else "{col} = VALUES({col})")
            updates = ', '.join(
                tpl.format(col=col) for col in on_duplicate_update
            )
            sql += f" ON DUPLICATE KEY UPDATE {updates}"
        cursor = conn.cursor()
        try:
            cursor.executemany(sql, rows)
            conn.commit()
            count = cursor.rowcount
            self.logger.info(f"  Upserted {count} rows into {table_name}")
            return count
        finally:
            cursor.close()

    def insert_ignore_rows(self, conn, table_name, columns, rows):
        """INSERT IGNORE — silently skip duplicate rows."""
        if not rows:
            return 0
        placeholders = ', '.join(['%s'] * len(columns))
        col_names = ', '.join(columns)
        sql = f"INSERT IGNORE INTO {table_name} ({col_names}) VALUES ({placeholders})"
        cursor = conn.cursor()
        try:
            cursor.executemany(sql, rows)
            conn.commit()
            count = cursor.rowcount
            self.logger.info(f"  Inserted {count} rows into {table_name}")
            return count
        finally:
            cursor.close()

    def truncate_and_insert(self, conn, table_name, columns, rows):
        """Replace a table's rows without risking data loss on insert failure.

        Rows are staged into a temporary table first. The real table is only
        emptied after the replacement rows have passed schema and constraint
        checks, and the final delete/insert runs in one transaction.
        """
        if not rows:
            self.logger.warning(
                f"  No data to insert into {table_name}, keeping existing rows"
            )
            return 0

        table_sql = _quote_identifier(table_name)
        temp_table_name = (
            f"{table_name}_refresh_{os.getpid()}_{int(time.time() * 1000)}"
        )
        temp_table_sql = _quote_identifier(temp_table_name)
        col_names = ', '.join(_quote_identifier(col) for col in columns)
        placeholders = ', '.join(['%s'] * len(columns))
        tx_conn = _transaction_connection(conn)
        old_autocommit = getattr(tx_conn, 'autocommit', False)

        cursor = conn.cursor()
        try:
            if old_autocommit:
                tx_conn.autocommit = False

            cursor.execute(
                f"CREATE TEMPORARY TABLE {temp_table_sql} LIKE {table_sql}"
            )
            cursor.executemany(
                f"INSERT INTO {temp_table_sql} ({col_names}) "
                f"VALUES ({placeholders})",
                rows,
            )
            staged_count = cursor.rowcount
            conn.commit()

            cursor.execute(f"DELETE FROM {table_sql}")
            cursor.execute(
                f"INSERT INTO {table_sql} ({col_names}) "
                f"SELECT {col_names} FROM {temp_table_sql}"
            )
            conn.commit()
            self.logger.info(
                f"  Replaced {table_name} with {staged_count} staged rows"
            )
            return staged_count
        except Exception:
            conn.rollback()
            raise
        finally:
            try:
                cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS {temp_table_sql}")
            except Exception as exc:
                self.logger.warning(
                    f"  Could not drop temporary table for {table_name}: {exc}"
                )
            if old_autocommit:
                tx_conn.autocommit = True
            cursor.close()

    # -- Rate limiting helper -----------------------------------------------

    def rate_limit_pause(self, seconds=0.5):
        """Simple pause between API calls to avoid rate limits."""
        time.sleep(seconds)

    def retry_call(self, func, max_retries=10, backoff=5, label=None):
        """Call func() with retry on failure. Returns the result or re-raises."""
        tag = label or func.__name__
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                self.logger.warning(f"  {tag} attempt {attempt+1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(backoff * (attempt + 1))
                else:
                    raise

    # -- Main entry points --------------------------------------------------

    def fetch_series(self, series_config):
        """Fetch and store data for one series. Override in subclasses."""
        raise NotImplementedError

    def run(self, series_filter=None):
        """Fetch all series (or a filtered subset).

        Args:
            series_filter: optional string — if given, only series whose
                'table' contains this string will be fetched.
        """
        setup_logging()
        self.logger.info(f"=== Starting {self.name} ===")
        start = datetime.now()
        success = 0
        failed = 0

        series_list = self.SERIES
        if series_filter:
            series_list = [
                s for s in series_list
                if series_filter in s.get('table', '')
            ]
            self.logger.info(f"Filtered to {len(series_list)} series matching '{series_filter}'")

        if self.MAX_WORKERS > 1 and len(series_list) > 1:
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                futures = {}
                for sc in series_list:
                    table = sc.get('table', 'unknown')
                    self.logger.info(f"Fetching: {table}")
                    futures[executor.submit(self.fetch_series, sc)] = table
                for future in as_completed(futures):
                    table = futures[future]
                    try:
                        future.result()
                        success += 1
                    except Exception as e:
                        self.logger.exception(f"FAILED: {table} — {e}")
                        failed += 1
        else:
            for sc in series_list:
                table = sc.get('table', 'unknown')
                try:
                    self.logger.info(f"Fetching: {table}")
                    self.fetch_series(sc)
                    success += 1
                except Exception as e:
                    self.logger.exception(f"FAILED: {table} — {e}")
                    failed += 1

        elapsed = datetime.now() - start
        self.logger.info(
            f"=== {self.name} done: {success} OK, {failed} failed, "
            f"{elapsed.total_seconds():.1f}s ==="
        )
        return failed == 0

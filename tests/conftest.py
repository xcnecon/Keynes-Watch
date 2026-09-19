"""Tests must not load a checkout's deployment .env file."""
from unittest.mock import patch

import pytest

_dotenv = patch('dotenv.load_dotenv', return_value=False)


def pytest_configure(config):
    _dotenv.start()


def pytest_unconfigure(config):
    _dotenv.stop()


@pytest.fixture(autouse=True)
def isolate_external_services(monkeypatch):
    """Fail closed if a regression test accidentally reaches live services."""
    for name in ('DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_NAME',
                 'FRED_API_KEY', 'BEA_API_KEY', 'CN_PROXY',
                 'NBS_HOUSE_FULL', 'NBS_RETAIL_FULL'):
        monkeypatch.delenv(name, raising=False)

    def forbidden(*args, **kwargs):
        pytest.fail('Tests must substitute live HTTP and MySQL connections')

    monkeypatch.setattr('requests.sessions.Session.request', forbidden)
    monkeypatch.setattr('fetch_data.base.MySQLConnectionPool', forbidden)

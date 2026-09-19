"""Regression coverage for the runtime and China-source additions."""
import json
from datetime import date
from unittest.mock import Mock

import pandas as pd
import pytest
import requests

from fetch_data import base, run
from fetch_data.base import AbortFetch, BaseFetcher
from fetch_data.sources.nbs import NBSFetcher, NBSThrottledError, HP_V2_INDICATORS
from fetch_data.sources.pboc import PBOCFetcher


def test_abort_fetch_does_not_retry_or_sleep(monkeypatch):
    fetch = Mock(side_effect=AbortFetch('wait for the next run'))
    sleep = Mock()
    monkeypatch.setattr(base.time, 'sleep', sleep)
    with pytest.raises(AbortFetch):
        BaseFetcher().retry_call(fetch, max_retries=5, label='fixture')
    fetch.assert_called_once()
    sleep.assert_not_called()


@pytest.mark.parametrize('workers', [1, 2])
def test_failed_series_status_survives_filtered_rerun(tmp_path, monkeypatch, workers):
    status_path = tmp_path / 'logs' / 'fetch_status.json'
    monkeypatch.setattr(base, 'LOG_DIR', str(status_path.parent))
    monkeypatch.setattr(base, 'STATUS_FILE', str(status_path))

    class SampleFetcher(BaseFetcher):
        MAX_WORKERS = workers
        SERIES = [{'table': 'good'}, {'table': 'bad'}]

        def fetch_series(self, config):
            if config['table'] == 'bad':
                raise ValueError('invalid source data')

    fetcher = SampleFetcher()
    assert not fetcher.run(source_name='sample')
    first = json.loads(status_path.read_text())['sample']
    assert (first['ok'], first['failed']) == (1, 1)
    assert first['series']['good']['ok'] is True
    assert first['series']['bad']['ok'] is False
    assert first['series']['bad']['error'] == 'ValueError: invalid source data'

    assert fetcher.run(series_filter='good', source_name='sample')
    second = json.loads(status_path.read_text())['sample']
    assert (second['ok'], second['failed']) == (1, 0)
    assert second['series_filter'] == 'good'
    assert second['series']['bad'] == first['series']['bad']
    assert not status_path.with_suffix('.json.tmp').exists()


def test_status_file_failure_does_not_fail_collection(tmp_path, monkeypatch):
    monkeypatch.setattr(base, 'LOG_DIR', str(tmp_path))
    monkeypatch.setattr(base, 'STATUS_FILE', str(tmp_path / 'fetch_status.json'))
    monkeypatch.setattr(base.os, 'replace', Mock(side_effect=OSError('read-only status')))
    fetcher = BaseFetcher()
    fetcher.SERIES = [{'table': 'sample'}]
    fetcher.fetch_series = Mock()
    assert fetcher.run(source_name='sample')
    fetcher.fetch_series.assert_called_once()


@pytest.mark.parametrize('failure', ['403', '429', 'empty', 'connection'])
def test_nbs_throttle_stops_further_network_calls(failure):
    fetcher = NBSFetcher()
    fetcher.session = Mock()
    response = fetcher.session.post.return_value
    response.status_code = int(failure) if failure.isdigit() else 200
    if failure == 'empty':
        response.json.side_effect = ValueError('not JSON')
    elif failure == 'connection':
        fetcher.session.post.side_effect = requests.exceptions.ConnectionError('reset')

    for _ in range(fetcher.WAF_STRIKE_LIMIT):
        with pytest.raises((ValueError, requests.exceptions.ConnectionError)):
            fetcher._v2_post_json({}, 'fixture')
    with pytest.raises(NBSThrottledError):
        fetcher._v2_post_json({}, 'next city')
    assert fetcher.session.post.call_count == fetcher.WAF_STRIKE_LIMIT


def test_nbs_success_resets_consecutive_throttle_count():
    fetcher = NBSFetcher()
    fetcher.session = Mock()
    response = fetcher.session.post.return_value
    response.status_code = 403
    for _ in range(fetcher.WAF_STRIKE_LIMIT - 1):
        with pytest.raises(ValueError):
            fetcher._v2_post_json({}, 'fixture')
    response.status_code = 200
    response.json.return_value = {'state': 20000, 'data': []}
    assert fetcher._v2_post_json({}, 'fixture')['state'] == 20000
    response.status_code = 429
    with pytest.raises(ValueError):
        fetcher._v2_post_json({}, 'fixture')
    assert fetcher._waf_strikes == 1
    assert not fetcher._waf_tripped


@pytest.mark.parametrize('city,returned,accepted', [
    ('上海', '北京市', False),
    ('上海', '', False),
    ('上海', '上海市', True),
    ('大理', '大理白族自治州', True),
])
def test_nbs_city_response_cannot_silently_default_to_beijing(city, returned, accepted):
    fetcher = NBSFetcher()
    fetcher._v2_post_json = Mock(return_value={
        'state': 20000, 'data': [{'code': '202607MM', 'values': [
            {'da_name': returned, '_id': HP_V2_INDICATORS['new_mom'], 'value': '99.3'},
        ]}],
    })
    if accepted:
        result = fetcher._nbs_v2_house_city(city, '310000', '202607MM-202607MM')
        assert result == {date(2026, 7, 1): {'new_mom': 99.3}}
    else:
        with pytest.raises(ValueError, match='returned region'):
            fetcher._nbs_v2_house_city(city, '310000', '202607MM-202607MM')


def test_repo_fixing_saves_bounded_windows_and_preserves_missing_fdr(monkeypatch):
    today = pd.Timestamp.today().normalize()
    latest = today - pd.Timedelta(days=300)
    windows = []

    def history(start_date, end_date):
        windows.append((pd.Timestamp(start_date), pd.Timestamp(end_date)))
        return pd.DataFrame([
            {'date': end_date, 'FR001': '1.25', 'FR007': '1.50'},
            {'date': 'invalid', 'FR001': '1.25'},
            {'date': start_date},
        ])

    monkeypatch.setattr('fetch_data.sources.pboc.ak.repo_rate_hist', history)
    fetcher = PBOCFetcher()
    fetcher.rate_limit_pause = Mock()
    fetcher.insert_ignore_rows = Mock()
    conn = Mock()
    fetcher._fetch_repo_fixing(latest_date=latest.date(), _conn=conn)
    assert len(windows) == 3
    assert windows[0][0] == latest - pd.Timedelta(days=5)
    assert windows[-1][1] == today
    for i, (start, end) in enumerate(windows):
        assert (end - start).days <= 149
        if i:
            assert start == windows[i - 1][1] + pd.Timedelta(days=1)
        call = fetcher.insert_ignore_rows.call_args_list[i]
        assert call.args[0] is conn
        assert call.args[1] == 'pboc_repo_fixing'
        assert call.args[3] == [(end.date(), 1.25, 1.50, None, None, None, None)]


def test_repo_fixing_keeps_saved_window_when_later_download_aborts(monkeypatch):
    today = pd.Timestamp.today().normalize()
    latest = today - pd.Timedelta(days=300)
    first_end = latest - pd.Timedelta(days=5) + pd.Timedelta(days=149)
    history = Mock(side_effect=[
        pd.DataFrame([{'date': first_end, 'FR007': 1.5}]),
        AbortFetch('upstream unavailable'),
    ])
    monkeypatch.setattr('fetch_data.sources.pboc.ak.repo_rate_hist', history)
    fetcher = PBOCFetcher()
    fetcher.rate_limit_pause = Mock()
    fetcher.insert_ignore_rows = Mock()
    with pytest.raises(AbortFetch):
        fetcher._fetch_repo_fixing(latest_date=latest.date(), _conn=Mock())
    fetcher.insert_ignore_rows.assert_called_once()
    assert fetcher.insert_ignore_rows.call_args.args[3][0][0] == first_end.date()


def test_cli_lists_all_sources_without_credentials(capsys):
    run.list_sources()
    output = capsys.readouterr().out
    assert 'import error' not in output
    for source in run.ALL_SOURCES:
        assert f'\n{source}:\n' in output
    assert 'pboc_repo_fixing' in output


def test_cli_failure_propagates_exit_code_and_source_name(monkeypatch):
    fetcher = Mock()
    fetcher.run.return_value = False
    monkeypatch.setattr(run, '_import_fetcher', Mock(return_value=lambda: fetcher))
    monkeypatch.setattr(run.sys, 'argv', ['fetch_data.run', '--source', 'fred', '--series', 'gdp'])
    with pytest.raises(SystemExit) as result:
        run.main()
    assert result.value.code == 1
    fetcher.run.assert_called_once_with(series_filter='gdp', source_name='fred')

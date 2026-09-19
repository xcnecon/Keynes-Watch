# Keynes Watch — Data Pipeline

Keynes Watch collects U.S. and Chinese macroeconomic data into MySQL tables
and local CSV files. This is the standalone data pipeline originally used by
[keyneswatch.com](https://keyneswatch.com/), released under the [MIT License](LICENSE).
It contains no Flask application, charts, or website deployment configuration.

**项目安排：** 网站后续不再持续维护更新；本仓库保留为可独立运行的数据采集项目。
需要最新数据的使用者，请配置自己的数据库、API 密钥并运行或定时执行采集程序。
本次整理仅更新采集代码与文档，不执行线上停更、关站或部署操作。

The hosted website is planned to leave regular maintenance. This repository
lets you run the pipeline yourself; it does not provide a hosted data feed or
promise continued updates to upstream integrations. Updates run only when you
invoke the fetchers or configure your own scheduler. Historical coverage and
availability depend on each source.

The project was created by [Chenning Xu](https://www.linkedin.com/in/chenning-xu/).
The September 2026 synchronization is described in
[the comparison and migration notes](docs/sync-2026-09-19.md).

## Data sources

| Source | Main tables / files |
| --- | --- |
| `fred` | Claims, payrolls, unemployment, CPI, GDP, reserve balances, Fedwire monthly statistics |
| `bea` | BEA NIPA data for the Kalecki equation and three-sector balances |
| `fiscal` | TGA balance, debt limit, Treasury outstanding, average maturity, average yields, Monthly Treasury Statement, withheld tax |
| `nyfed` | New York Fed repo / reverse-repo operations and overnight rates |
| `treasury` | Nominal and real Treasury yield curves |
| `indeed` | Indeed Hiring Lab wage and job-posting CSV snapshots |
| `pboc` | LPR, money supply, social financing, credit, reserve ratios, SHIBOR, policy rates, balance sheet, OMO, FR/FDR repo fixing rates |
| `nbs` | Real estate climate, 70-city house prices, macro real estate indicators, annual flow-of-funds accounts, monthly retail sales |
| `mof` | Land transfer revenue from monthly fiscal reports |

The fetchers use official APIs and releases, AKShare adapters, and Indeed's
published datasets. Some policy-rate history is maintained as seed values in
the code. See the source modules for endpoints, units, and transformation rules.

Most sources create their own tables in an **existing** MySQL database. The
`indeed` source writes files under `fetch_data/github/` and does not need MySQL
or an API key. Downloaded data is excluded from git; the small public examples
under `tests/fixtures/` are regression inputs, not a distributable dataset.

## Setup

Use Python 3.11 (the tested version) and MySQL. From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Create your database using a MySQL administrator account, for example:

```sql
CREATE DATABASE keyneswatch CHARACTER SET utf8mb4;
```

Configure your own database user with permission to read/write data and create
or alter tables and indexes in that database. Populate `.env` with its connection
details. Fetchers do not create the database or database user.

| Variable | Required for | Description |
| --- | --- | --- |
| `DB_HOST` | Database-backed sources | MySQL host |
| `DB_PORT` | Optional | MySQL port; defaults to `3306` |
| `DB_USER` | Database-backed sources | MySQL user |
| `DB_PASSWORD` | Database-backed sources | MySQL password |
| `DB_NAME` | Database-backed sources | Existing database name |
| `FRED_API_KEY` | `fred` | Your FRED API key |
| `BEA_API_KEY` | `bea` | Your BEA API key |
| `CN_PROXY` | Optional | HTTP/SOCKS proxy URL for supported China fetchers |

`.env` is ignored by git. No production credentials, certificates, database
exports, or private server settings are needed.

## Usage

List source groups and target tables without fetching data or connecting to MySQL:

```bash
python -m fetch_data.run --list
```

Run all sources, one source, or a matching table group:

```bash
python -m fetch_data.run
python -m fetch_data.run --source fred
python -m fetch_data.run --source fred --series claims
python -m fetch_data.run --source pboc --series repo_fixing
```

The first run can take considerably longer than subsequent updates. Run only
the sources you need. A failed series causes the runner to return a nonzero
exit code; other series still run. Re-running accepts revisions or resumes
incremental downloads according to each source's strategy.

The optional Bash helper creates a virtual environment if needed and installs
requirements when they change. It resolves paths from the checkout, so it can
be run from any working directory:

```bash
bash scripts/update_all.sh
FETCH_SOURCES="fred bea fiscal" bash scripts/update_all.sh
```

It supports `PROJECT_DIR`, `VENV_DIR`, `LOG_FILE`, and `FETCH_SOURCES` overrides.
Its console output is also saved to `update.log` by default. On Windows, use
the Python commands above, or run the helper through Git Bash.

For unattended updates, configure your own cron job or Windows Task Scheduler
task. For example, a daily cron entry (in the machine's local timezone):

```cron
0 9 * * * /bin/bash /path/to/Keynes-Watch/scripts/update_all.sh
```

Each source run records series outcomes, durations, and errors in
`logs/fetch_status.json`. A filtered run preserves the previous results for
untouched series; check each series' `finished` timestamp when assessing
freshness. This file is local runtime state and is ignored by git. Schedule
runs sequentially within a checkout; status-file locking is process-local.

## Updating an existing database

Ordinary runs now re-read revised FRED observations, repair MTS fiscal-year
mapping and Treasury maturity summaries, refill lagging NY Fed operation types,
and correct PBOC zero-allotment notices to a NULL rate with zero volume.
See [migration notes](docs/sync-2026-09-19.md) for targeted commands.

NBS house prices use the newer city-price API with response-city validation;
retail sales and flow-of-funds also use the newer data-release API. Official
press releases provide fallbacks where implemented. NBS throttling can stop
API requests for the remainder of a run; retry later instead of increasing
parallelism. The national real-estate macro fetcher still uses the legacy
easyquery endpoint with a press-release fallback.

Optional NBS full-history refreshes, in Bash:

```bash
NBS_HOUSE_FULL=1 python -m fetch_data.run --source nbs --series house_price
NBS_RETAIL_FULL=1 python -m fetch_data.run --source nbs --series retail_sales
```

In PowerShell, set `$env:NBS_HOUSE_FULL = '1'` (or `NBS_RETAIL_FULL`), run the
corresponding Python command, then remove the temporary setting with
`Remove-Item Env:NBS_HOUSE_FULL` (or `Env:NBS_RETAIL_FULL`).

## Tests

```bash
python -m pip install -r requirements-test.txt
python -m pytest -q
python -m compileall -q fetch_data
```

Tests use curated public fixtures and isolated database/network substitutes;
they do not load `.env` or update external data. CI runs these checks and CLI
discovery without API keys or a live database. It does not deploy the website
or schedule data collection. Tests verify parsing and update behavior, not
the continued availability of every external endpoint.

## License and repository scope

[MIT](LICENSE) applies to this project's code. Upstream data remains subject
to its providers' terms. Keep local secrets, downloaded datasets, logs,
certificates, and database dumps out of commits. The website code and its
deployment history are maintained separately from this data-only repository.

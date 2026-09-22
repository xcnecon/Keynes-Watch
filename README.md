# Keynes Watch — Data Pipeline

Keynes Watch collects U.S. and Chinese macroeconomic data into MySQL tables
and local CSV files. This is the standalone data pipeline that powered
[keyneswatch.com](https://keyneswatch.com/), released under the [MIT License](LICENSE).
It contains no Flask application, charts, or website deployment configuration;
the [gallery](#what-the-data-looked-like-on-keyneswatchcom) below shows what the
site built from these tables.

**项目安排：** keyneswatch.com 网站已下线；本仓库保留为可独立运行的数据采集项目。
需要最新数据的使用者，请配置自己的数据库、API 密钥并运行或定时执行采集程序。
网站各页面的截图见下文画廊。

The hosted website has been taken offline. This repository lets you run the
pipeline yourself; it does not provide a hosted data feed or promise continued
updates to upstream integrations. Updates run only when you
invoke the fetchers or configure your own scheduler. Historical coverage and
availability depend on each source.

The project was created by [Chenning Xu](https://www.linkedin.com/in/chenning-xu/).
The September 2026 synchronization is described in
[the comparison and migration notes](docs/sync-2026-09-19.md).

## What the data looked like on keyneswatch.com

The website rendered these tables as interactive Plotly pages behind a Flask
app. The screenshots below come from a local run of that site in September
2026 against a database filled by this pipeline. Each caption names the
table(s) the chart reads, so you can find the matching fetcher below.

<p align="center">
  <img src="docs/screenshots/home.png" alt="Keynes Watch home page" width="900">
</p>

### United States

<table>
<tr>
<td width="50%" valign="top">
<b>Monthly Treasury Statement</b><br>
Calendar year-to-date deficit, one line per year. Table <code>mts</code>.<br><br>
<img src="docs/screenshots/us_mts.png" alt="Monthly Treasury Statement, deficit YTD by year">
</td>
<td width="50%" valign="top">
<b>Weighted average maturity</b><br>
Face-value-weighted remaining maturity of marketable Treasury debt, computed by this pipeline from security-level MSPD records. Table <code>treasury_average_maturity</code>.<br><br>
<img src="docs/screenshots/us_average_treasury_maturities.png" alt="Treasury weighted average maturity since 2001">
</td>
</tr>
<tr>
<td width="50%" valign="top">
<b>Kalecki profits equation</b><br>
Corporate profits decomposed quarterly from NIPA into investment, government saving, foreign saving, dividends and personal saving. Table <code>kalecki_equation</code>.<br><br>
<img src="docs/screenshots/us_kalecki_equation.png" alt="Kalecki profits equation, quarterly">
</td>
<td width="50%" valign="top">
<b>Sectoral financial balances</b><br>
Private, government and foreign net lending as a share of GDP; the three sum to zero by identity. Table <code>kalecki_equation</code>.<br><br>
<img src="docs/screenshots/us_three_sector_balance.png" alt="Three-sector financial balances">
</td>
</tr>
<tr>
<td width="50%" valign="top">
<b>Overnight rates: EFFR and SOFR</b><br>
The SOFR percentile distribution and the effective funds rate inside the FOMC target range. Table <code>onrates</code>.<br><br>
<img src="docs/screenshots/us_stir.png" alt="EFFR and SOFR percentiles within the target range">
</td>
<td width="50%" valign="top">
<b>Reserve balances scaled by GDP</b><br>
Reserve balances at the Fed divided by nominal GDP. Tables <code>fred_wresbal</code> and <code>fred_gdp</code>.<br><br>
<img src="docs/screenshots/us_reserve.png" alt="Reserve balances relative to nominal GDP">
</td>
</tr>
<tr>
<td width="50%" valign="top">
<b>Treasury General Account</b><br>
Daily closing balance, including the 2023 debt-limit drawdown and rebuild. Table <code>tga_balance</code>.<br><br>
<img src="docs/screenshots/us_tga_balance.png" alt="Daily TGA balance">
</td>
<td width="50%" valign="top">
<b>Withheld taxes</b><br>
Daily withheld income and payroll taxes, cumulative year-to-date and aligned by calendar position. Table <code>withheld_tax</code>.<br><br>
<img src="docs/screenshots/us_withheld_tax.png" alt="Withheld taxes cumulative YTD by year">
</td>
</tr>
<tr>
<td width="50%" valign="top">
<b>Treasury debt held by the public, share of GDP</b><br>
Total public debt held by the public, from the MSPD, divided by nominal GDP. Tables <code>total_debt_outstanding</code> and <code>fred_gdp</code>.<br><br>
<img src="docs/screenshots/us_treasuries_outstanding.png" alt="Treasury debt held by the public as a share of GDP">
</td>
<td width="50%" valign="top">
<b>Yield curve on two dates</b><br>
Nominal constant-maturity curves overlaid for shape comparison. Table <code>yield_curve</code>.<br><br>
<img src="docs/screenshots/us_yield_curve.png" alt="Nominal yield curve on two dates">
</td>
</tr>
<tr>
<td width="50%" valign="top">
<b>Statutory debt limit</b><br>
Gaps in the line mark suspension periods. Table <code>debt_limit</code>.<br><br>
<img src="docs/screenshots/us_debt_limit.png" alt="Statutory debt limit with suspension gaps">
</td>
<td width="50%" valign="top">
<b>Average interest rate on Treasury debt</b><br>
The rate the Treasury pays on outstanding marketable securities. Table <code>treasuries_average_yields</code>.<br><br>
<img src="docs/screenshots/us_treasuries_average_interest_rates.png" alt="Average interest rate on marketable Treasury debt">
</td>
</tr>
</table>

### China

<table>
<tr>
<td width="50%" valign="top">
<b>Interest rate corridor</b><br>
SLF 7-day ceiling, IOER floor, 7-day reverse repo rate and the FDR007 fixing. Tables <code>pboc_repo_fixing</code>, <code>pboc_omo</code> and <code>pboc_policy_rates</code>.<br><br>
<img src="docs/screenshots/cn_shibor.png" alt="China interest rate corridor">
</td>
<td width="50%" valign="top">
<b>PBOC balance sheet</b><br>
Asset side: FX reserves, claims on government and claims on depository institutions. Table <code>pboc_balance_sheet</code>.<br><br>
<img src="docs/screenshots/cn_balance_sheet.png" alt="PBOC balance sheet, asset side">
</td>
</tr>
<tr>
<td width="50%" valign="top">
<b>Kalecki profits equation, China</b><br>
The same identity rebuilt annually from the NBS flow-of-funds accounts. Table <code>cn_flow_of_funds</code>.<br><br>
<img src="docs/screenshots/cn_kalecki_equation.png" alt="Kalecki profits equation for China">
</td>
<td width="50%" valign="top">
<b>Sectoral financial balances, China</b><br>
Private, government and foreign net lending as a share of GDP. Table <code>cn_flow_of_funds</code>.<br><br>
<img src="docs/screenshots/cn_three_sector_balance.png" alt="Three-sector financial balances for China">
</td>
</tr>
<tr>
<td width="50%" valign="top">
<b>Money supply</b><br>
M1 and M2 growth with the M1 minus M2 gap shaded. Table <code>pboc_money_supply</code>.<br><br>
<img src="docs/screenshots/cn_money_supply.png" alt="M1 and M2 growth with the gap shaded">
</td>
<td width="50%" valign="top">
<b>Total social financing by component</b><br>
Monthly flow, stacked by instrument. Table <code>pboc_social_financing</code>.<br><br>
<img src="docs/screenshots/cn_social_financing.png" alt="Total social financing monthly flow by component">
</td>
</tr>
<tr>
<td width="50%" valign="top">
<b>70-city house prices</b><br>
New-home index year on year, averaged by city tier. Table <code>nbs_house_price</code>.<br><br>
<img src="docs/screenshots/cn_house_price.png" alt="70-city house price index by tier">
</td>
<td width="50%" valign="top">
<b>Land transfer revenue</b><br>
Cumulative year-to-date proceeds from land-use-right sales, by year. Table <code>mof_land_revenue</code>.<br><br>
<img src="docs/screenshots/cn_land_revenue.png" alt="Land transfer revenue cumulative YTD by year">
</td>
</tr>
<tr>
<td width="50%" valign="top">
<b>Loan Prime Rate</b><br>
1-year and 5-year-plus LPR. Table <code>pboc_lpr</code>.<br><br>
<img src="docs/screenshots/cn_lpr.png" alt="Loan Prime Rate, 1-year and 5-year-plus">
</td>
<td width="50%" valign="top">
<b>Required reserve ratio</b><br>
Large and small-to-medium institutions, with the spread shaded. Table <code>pboc_rrr</code>.<br><br>
<img src="docs/screenshots/cn_rrr.png" alt="Required reserve ratios for large and small institutions">
</td>
</tr>
</table>

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

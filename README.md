# Keynes Watch — Data Pipeline

This is the open-source ETL behind [keyneswatch.com](https://keyneswatch.com/),
a free, auto-updating library of the macro data that rates and macro investors
actually trade on:

- **U.S. — Fed & money markets**: EFFR vs the target range, SOFR percentiles and
  volume, NY Fed repo/reverse-repo operations, reserve balances scaled by GDP and
  Fedwire volume, nominal and TIPS yield curves
- **U.S. — Treasury supply & fiscal flows**: outstanding debt by instrument,
  weighted average maturity and interest rates, the daily TGA balance, the debt
  limit, and the Monthly Treasury Statement
- **U.S. — Labor, in real time**: daily withheld income/payroll taxes (a
  census-like wage-bill proxy weeks ahead of payrolls), CES payrolls, UI claims,
  unemployment detail, Indeed posted wages and job postings
- **U.S. — Profits & sectoral balances**: the Kalecki–Levy profits equation and
  Godley-style three-sector balances, built quarterly from BEA NIPA
- **China — PBOC & credit**: the rate corridor (SHIBOR/SLF/IOER/OMO), LPR, RRR,
  money supply, the PBOC balance sheet, total social financing and new loans
- **China — Economic activity**: total retail sales of consumer goods (monthly
  since 1984) with urban/rural, catering/goods, 16 above-quota category and
  online-retail splits
- **China — Property & land finance**: NBS real estate macro, 70-city house
  prices, and MOF land transfer revenue
- **China — Profits & sectoral balances**: the same Kalecki-equation and
  three-sector-balance identities rebuilt annually (1992+) from the NBS
  flow-of-funds accounts (non-financial transactions)

Every series is pulled programmatically from primary sources — the NY Fed
Markets API, Treasury Fiscal Data, BEA, FRED, PBOC, NBS, and MOF — never rekeyed
from secondary aggregators, and refreshes automatically as new data are
released.

The site is built and maintained by [Chenning Xu](https://www.linkedin.com/in/chenning-xu/),
a Hong Kong-based hedge fund research analyst covering global macro with a focus
on rates ([email](mailto:chenningxuecon@gmail.com)).

This repository contains the fetcher code and safe configuration examples;
it does not include production secrets, logs, certificates, downloaded datasets,
or server deployment files.

## Data Sources

The unified runner in `fetch_data/run.py` can update these source groups:

| Source | Main tables / files |
| --- | --- |
| `fred` | FRED claims, payrolls, unemployment, CPI, GDP, reserve balances, Fedwire monthly stats |
| `bea` | BEA NIPA data for the Kalecki equation and three-sector balances |
| `fiscal` | Treasury Fiscal Data API tables: TGA balance, debt limit, Treasury outstanding, average maturity, average yields, MTS, withheld tax |
| `nyfed` | New York Fed repo operations and overnight rates |
| `treasury` | Nominal and real Treasury yield curves |
| `indeed` | Indeed Hiring Lab wage and job-posting CSV snapshots |
| `pboc` | PBOC LPR, money supply, social financing, credit, reserve ratios, SHIBOR, policy rates, balance sheet, OMO |
| `nbs` | NBS China real estate climate, house prices, macro real estate indicators, annual flow-of-funds accounts, and monthly retail sales of consumer goods (via the NBS data-release-library API launched June 2026, with official press-release fallback) |
| `mof` | China Ministry of Finance land transfer revenue from monthly fiscal reports |

Most sources write into MySQL tables and create those tables if they do not
exist. The `indeed` source writes CSV files under `fetch_data/github/`; generated
CSV and metadata files are intentionally ignored by git.

## Setup

Requirements:

- Python 3.11 or newer
- MySQL-compatible database
- FRED API key for `fred`
- BEA API key for `bea`
- Optional proxy for China data sources if your network needs one

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create local configuration:

```bash
cp .env.example .env
```

Then edit `.env` with your local database and API credentials. `.env` is ignored
by git and should not be committed.

## Configuration

Required environment variables:

| Variable | Description |
| --- | --- |
| `DB_HOST` | MySQL host |
| `DB_PORT` | MySQL port, usually `3306` |
| `DB_USER` | MySQL user |
| `DB_PASSWORD` | MySQL password |
| `DB_NAME` | MySQL database name |
| `FRED_API_KEY` | FRED API key |
| `BEA_API_KEY` | BEA API key |

Optional:

| Variable | Description |
| --- | --- |
| `CN_PROXY` | HTTP/SOCKS proxy URL used by China data fetchers |

The code never needs production server paths. If `CN_PROXY` is set, logs only
state that a proxy is configured; the proxy value is not printed.

## Usage

List available source groups and target tables:

```bash
python -m fetch_data.run --list
```

Run every source:

```bash
python -m fetch_data.run
```

Run one source:

```bash
python -m fetch_data.run --source fred
```

Run one table group by substring:

```bash
python -m fetch_data.run --source fred --series claims
```

Run the generic update script:

```bash
bash scripts/update_all.sh
```

Run a subset with the script:

```bash
FETCH_SOURCES="fred bea fiscal" bash scripts/update_all.sh
```

## Data integrity and tests

The [2026-09-07 page audit](docs/data-audit-2026-09-07.md) records the checks
of all 32 data pages, confirmed errors, fixes, and limitations arising from
the web application and production database being outside this repository.

Run the offline regression suite:

```bash
python -m pip install -r requirements-test.txt
python -m pytest -q
```

Normal source updates also repair the affected stored history:

- MTS reads all available Table 1 reports and upserts each month's latest
  revision, resolving the fiscal-year parent before converting USD to millions.
  Existing observations before the API coverage are preserved.
- Treasury maturity updates recalculate summaries exclusively from securities
  with a contractual maturity date, even when no new month is available.
- Repo updates start from the earlier of the Repo and Reverse Repo watermarks,
  with a seven-day overlap; only completed auction results are summed.
- PBOC OMO revisits the recent month and any earlier stored zero-rate error;
  zero-volume notices without a rate are stored with a NULL rate. The website
  must preserve that missing value rather than render it as a 0% policy rate.
- NBS retail revisits recent official releases to fill missed periods, including
  first-half reports named “上半年”.
- GDP, CPI and reserve balances refresh existing observations to accept revisions.

After deploying this branch's fetchers in the production update environment,
the targeted updates are:

```bash
python -m fetch_data.run --source fiscal --series mts
python -m fetch_data.run --source fiscal --series treasury_average_maturity
python -m fetch_data.run --source nyfed --series repo
python -m fetch_data.run --source pboc --series pboc_omo
python -m fetch_data.run --source nbs --series nbs_retail_sales
python -m fetch_data.run --source fred
```

The tests use public source fixtures and isolated database substitutes, without
API keys or production access. GitHub Actions runs the same suite. Pushing or
merging this repository alone does not establish that the live database or web
application has been refreshed.

## Privacy Notes

The public repository intentionally excludes:

- `.env` and other environment files containing local secrets
- TLS certificates and private keys
- server logs and update logs
- virtual environments and bytecode caches
- downloaded Indeed CSV snapshots and metadata
- production startup scripts tied to a specific host

Before publishing, run:

```bash
rg -n "(BEGIN .*PRIVATE|password=|token=|api_key=|/root/|C:\\\\Users|production-domain\\.com)" .
```

Review any matches manually. Environment variable names such as
`DB_PASSWORD`, `FRED_API_KEY`, and `BEA_API_KEY` are expected; actual secret
values should never appear in the repository.

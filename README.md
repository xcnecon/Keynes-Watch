# KeynesWatch Data Fetchers

Open-source data collection programs for KeynesWatch macroeconomic dashboards at
https://keyneswatch.com/.
This repository contains only the fetcher code and safe configuration examples;
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
| `nbs` | NBS China real estate climate, house prices, and macro real estate indicators |
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

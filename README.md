# Dome Open Data

*A private, unattended pipeline that collects open statistics into Parquet on Cloudflare R2.*

Internal DOME project (Sprint OD); not part of the P1 to P6 portfolio and not shown on
domelayer.com. The code is public so scheduled GitHub Actions are free; **the data is never
committed** and lives only in a private R2 bucket. Full spec: [`TOOL_CONTEXT.md`](./TOOL_CONTEXT.md).
Cross-cutting context: [dome-docs](../dome-docs/), brief in `dome-docs/sprints/SPRINT_OD_OPEN_DATA.md`.

---

## Quick start

### Requirements
- Python 3.12 and [uv](https://docs.astral.sh/uv/)

### Setup

```bash
git clone https://github.com/Dome-Layer/dome-open-data.git
cd dome-open-data
uv venv --python 3.12 .venv
VIRTUAL_ENV=.venv uv pip install -r requirements.txt -r requirements-dev.txt
cp .env.example .env   # then fill in the R2 values
pre-commit install
```

### Run locally

```bash
export PYTHONPATH=src
python -m dome_open_data list                                       # validate the registry
python -m dome_open_data run worldbank_wdi --local-dir data/local   # into a local folder
python -m dome_open_data run worldbank_wdi                          # into R2 (reads .env)
python -m dome_open_data verify worldbank_wdi                       # recount newest vintage
```

`run` checks the source's change signal first and exits without writing anything when it is
unchanged; `--force` skips that check. A vintage that already exists is never overwritten.

### Query

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET r2 (TYPE r2, KEY_ID '...', SECRET '...', ACCOUNT_ID '...');
SELECT * FROM read_parquet('r2://dome-open-data/clean/economy/worldbank__wdi/*/*.parquet',
                           hive_partitioning = true)
WHERE series_id = 'worldbank.wdi.NY.GDP.MKTP.CD.ITA.A';
```

---

## Environment variables

See `.env.example`: `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` (an R2 token with
Object Read & Write on the one bucket) and `R2_BUCKET`. In Actions they are repository secrets and
the repository variable `R2_BUCKET`. Never commit real values; `.env` is gitignored.

---

## Operation

- **ci** (`ci.yml`): lock check, ruff, mypy (strict), registry load, pytest, `pip-audit`.
- **gitleaks** (`gitleaks.yml`): secret scan on every push and weekly.
- **collect** (`collect.yml`): `workflow_dispatch` for one source, then `verify`. Cadence
  schedules, alerting, freshness and storage checks arrive in Sprint OD session 3.

Adding a source is one entry in `sources.yaml`; the schema (including the mandatory `licence`
block) is enforced by `src/dome_open_data/registry.py`.

---

## Repository structure

```
dome-open-data/
├── TOOL_CONTEXT.md            Full spec and verified source endpoints (read first)
├── sources.yaml               Source registry
├── src/dome_open_data/
│   ├── registry.py            Registry schema and loader
│   ├── connectors/            One module per API dialect (worldbank today)
│   ├── http.py                Rate limiting, retries, JSON guard
│   ├── storage.py             R2 / local object storage and DuckDB access
│   ├── model.py               Common time-series model
│   ├── validate.py            Gate before clean/
│   ├── catalog.py             catalog/ partitions (sources, series, runs)
│   └── runner.py              run and verify
├── tests/
└── .github/workflows/         ci, gitleaks, collect
```

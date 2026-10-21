# Dome Open Data: TOOL_CONTEXT

*Tool-specific context document. Lives in this tool's repo alongside its code.*
*For cross-cutting Dome context (method, design system, architecture principles), see [dome-docs](../dome-docs/).*

> Moved here from `dome-docs/sprints/SPRINT_OD_TOOL_CONTEXT_DRAFT.md` in Sprint OD session 1
> (2026-10-19). Unlike the other DOME repos, this file is **committed**: it holds no secrets and
> it is where the verified source endpoints live.

---

## Identity

- **Tool name:** Dome Open Data
- **Portfolio position:** Internal parallel track. **Not** a P-numbered tool and not shown on domelayer.com (DOME_DECISIONS 2026-10-19)
- **DOME phase:** n/a (data foundation; no method phase)
- **Status:** in development (Sprint OD, Phase 1, session 1 done)
- **Repo:** https://github.com/Dome-Layer/dome-open-data (public code; data private in R2)
- **Demo URL:** n/a

---

## Purpose

### Problem statement
Open statistical data is scattered across dozens of publishers, each with its own API, codes,
formats and revision habits. Assembling a consistent, long-run, multi-domain view costs days each
time and the history of revisions is usually lost.

### Solution
A registry-driven pipeline that collects about 50 open sources (global; economy & finance, trade &
commodities, climate & geospatial, demographics & public sector; about 1950 onward) on a schedule,
keeps every download as a dated vintage, and publishes one uniform, licence-tagged Parquet layer
that DuckDB queries in place.

### Value proposition
One private, always-current, revision-aware store of the world's main open statistics, built and
run at zero cost.

### Target users
Francesco, for now. Who else it serves is decided at Gate 3 (monetisation direction).

---

## User flow

1. Francesco adds or edits an entry in `sources.yaml` (or asks Claude Code to).
2. The cadence workflow picks it up on its next run; nothing else changes. *(Session 3; today a
   run starts from the `collect` workflow or the CLI.)*
3. The collector checks the source's change signal and stops, writing nothing, if it is unchanged.
   Otherwise it writes a raw vintage, validates it, and writes the clean vintage.
4. On failure, one GitHub issue per source opens or gets a comment (label `open-data`). *(Session 3.)*
5. Francesco queries `clean/` with DuckDB, locally or in a notebook.

---

## Architecture

### Tech stack
- **Frontend:** none
- **Backend:** Python 3.12 package `dome_open_data` (`src/` layout, run with `PYTHONPATH=src`), run by GitHub Actions; no server
- **Database:** none. Cloudflare R2 bucket `dome-open-data` (private, r2.dev URL off, no custom domain) holding Parquet; DuckDB as the query engine
- **Key libraries:** `httpx` (async HTTP), `obstore` (async object storage: R2 via S3, local, memory), `pyarrow` (Parquet), `duckdb` (verify and query), `pydantic` (registry schema), `pyyaml`
- **Hosting:** GitHub-hosted runners (free on a public repo); R2 free tier (10 GB, no egress fees)

### LLM usage
None. The DOME LLM abstraction (DA-004) does not apply.

### Data handling
- **What data enters the tool:** published open statistics only. No user data, no personal data.
- **What is retained:** raw downloads (except sources marked `keep_raw: false`, where URL + checksum are kept), clean Parquet vintages, run log.
- **What is discarded:** nothing by default; vintages are the point. Pruning policy: open question.
- **Input hashing:** SHA-256 of each raw file (before compression), stored in the vintage's `_manifest.json`.

### Deployment modes
Cloud only (GitHub Actions + R2). Air-gapped and client-tenant modes do not apply: the pipeline's
job is to reach public sources. Docker is not required (DOME_DECISIONS 2026-10-19).

### Storage layout
```
raw/{publisher}/{dataset}/vintage=YYYY-MM-DD/<original file>.gz     gzip of the bytes as received
raw/{publisher}/{dataset}/vintage=YYYY-MM-DD/_manifest.json         change token, files, sha256, row counts
clean/{domain}/{publisher}__{dataset}/vintage=YYYY-MM-DD/part-0.parquet
catalog/sources/source={id}/part-0.parquet                          registry entry incl. licence (current)
catalog/series/source={id}/part-0.parquet                           title, definition, unit per series_id (current)
catalog/runs/source={id}/run=<UTC write time, us>-<suffix>.parquet    one file per run, append only
```
Raw and clean objects are written with a create-only put (`If-None-Match`), so an existing vintage
can never be overwritten. **Deviation from the brief:** `catalog/` is partitioned per source
instead of single `sources.parquet` / `series.parquet` / `runs.parquet` files, so the session 3
matrix jobs never read-modify-write the same object. Read with a glob and
`hive_partitioning = true`.

### Run semantics
- **Unchanged** change token: the run exits and writes nothing, not even a run row (the Actions log shows it).
- **Collected:** raw files, manifest, clean file, catalog entries, then a `collected` run row.
- **Failed:** whatever raw was written stays (it is the record of what the source sent); the clean
  write is skipped; a `failed` run row records the error.
- The change token of the last `collected` run is the comparison point, so a failed run is retried
  on the next schedule.

### Common time-series model
`series_id · geo (ISO alpha-3) · period (start date) · freq · value · unit · obs_status · vintage`.
`series_id` is `{publisher}.{dataset}.{code}.{geo}.{freq}`, for example
`worldbank.wdi.NY.GDP.MKTP.CD.ITA.A`. Rows with no value are dropped (counted in the manifest).
Non-time-series sources (filings, tenders, events, boundaries) keep their own shape.

---

## Governance

### Events emitted
None. The pipeline does not write `GovernanceEvent` rows: no AI decision, no user action.

### Rules applied
- **Licence gate:** a registry entry without `licence.name`, `commercial_ok`, `share_alike` and `attribution` is refused.
- **Validation gate (session 1):** schema equals the common model, at least one row, row count equals the raw recount, no nulls in key columns, no NaN, no duplicate `(series_id, period)`. Session 3 adds the vintage-to-vintage comparison (missing column, more than 50% fewer rows).
- **Response guard:** a JSON source that answers with anything but JSON (for example a 200 HTML bot challenge) fails the run before anything is stored.
- **Freshness rule:** a source older than its `expected_staleness_days` raises an issue. *(Session 3.)*
- **Storage rule:** bucket size ≥ 8 GB raises an issue. *(Session 3.)*

### Verification
`python -m dome_open_data verify <id>` reads the newest vintage's manifest, re-reads every raw file
(checking its SHA-256), recounts the rows the clean file must hold, and checks that DuckDB reading
the clean Parquet straight from the bucket returns the same count and the model's columns. The
`collect` workflow runs it after every collection.

### Human-in-loop triggers
Every failed gate opens or updates a GitHub issue; nothing is auto-fixed. *(Session 3.)*

### Confidence scoring
n/a.

---

## Integrations with other DOME tools

### Calls out to
None.

### Called from
None today. P4 Data Intelligence could read `clean/` later; not planned.

### Shared workflow ID
n/a.

---

## Configuration

### Environment variables
- `R2_ACCOUNT_ID`: Cloudflare account ID (secret)
- `R2_ACCESS_KEY_ID`: R2 token, Object Read & Write scoped to the bucket (secret)
- `R2_SECRET_ACCESS_KEY`: secret
- `R2_BUCKET`: `dome-open-data` (repository variable)
- Phase 2+: `FRED_API_KEY`, `COMTRADE_API_KEY`, `EIA_API_KEY`, `OPENAQ_API_KEY`, `CDS_API_KEY` (secrets, added when the source is)

In Actions these are **repository** secrets and a repository variable, not environment or
organisation ones: nothing deploys, and an environment with reviewers would stall scheduled runs.

### Config files
- `sources.yaml`: the source registry
- `crosswalk/countries.csv`: country code crosswalk, including historical entities *(session 2)*

### Registry schema
As in `dome-docs/sprints/SPRINT_OD_OPEN_DATA.md`, plus four fields:
- `publisher`, `dataset`: build the storage paths; `id` must equal `{publisher}_{dataset}`
- `dialect`: which API flavour a connector speaks (`worldbank` for `rest` today)
- `options`: connector-specific settings, validated by the connector (WDI: `source_id`, `start_year`, `indicators`)

Quote `"yes"` / `"no"` in `commercial_ok`: YAML 1.1 reads them as booleans (the loader maps a
boolean back, but quoting keeps the file honest).

---

## Sources

Verified against the live sources on **2026-10-19** (Sprint OD Step 0).

| Source | Connector | Endpoint (verified 2026-10-19) | Change signal | Quirks |
|---|---|---|---|---|
| World Bank WDI | rest (`worldbank`) | `https://api.worldbank.org/v2/`; data `country/all/indicator/{code}?source=2&date=1960:YYYY&per_page=20000`; metadata `source/2/indicator` | `lastupdated` on `sources/2` (2026-10-08), plus the indicator list and start year | HTTP `Last-Modified` on the API is the CDN cache time, useless. No key. Errors come back as 200 `[{"message": ...}]`. Responses can take over 90 s: timeouts are retried. Full WDI is 1,498 indicators, about 300 MB raw per vintage (bulk `WDI_CSV.zip` is 283 MB with a real ETag), so session 1 collects a **curated 72 indicators** (scope A): about 10.7 MB raw gzip and 4.9 MB clean per vintage, 760k rows, about 2 minutes. `geo` still includes World Bank aggregate codes (`WLD`, `EUU`, `AFE`...) until the session 2 crosswalk |
| IMF | sdmx | `https://api.imf.org/external/sdmx/3.0/` and `/2.1/`; WEO = `IMF.RES:WEO` 9.0.0 (`COUNTRY.INDICATOR.FREQUENCY`), exchange rates = `IMF.STA:ER` 4.0.1 | Named vintage dataflows (`WEO_2025_OCT_VINTAGE`, `WEO_2026_APR_VINTAGE`, `ER_2026_MAY_VINTAGE`...) | Legacy portal retired 2025-11-05. No key. SDMX-CSV via `Accept: application/vnd.sdmx.data+csv;version=2.0.0`. The 3.0 `c[TIME_PERIOD]=ge:` filter returned 400 in Step 0; 2.1 `startPeriod` works. October 2026 WEO not yet a named vintage on 2026-10-19 |
| ECB | sdmx | `https://data-api.ecb.europa.eu/service/data/{flow}/{key}?format=csvdata` | `updatedAfter` | No key. `Last-Modified` header present. **`updatedAfter` with nothing new returns 404, not 304**: treat as unchanged |
| Pink Sheet | bulk_file | Discover from `https://www.worldbank.org/en/research/commodity-markets`; today `https://thedocs.worldbank.org/en/doc/74e8be41ceb20fa0da750cda2f6b9e4e-0050012026/related/CMO-Historical-Data-Monthly.xlsx` (586 KB) | `Last-Modified` (2026-10-02) and the discovered URL | File URL hash changes: never hard-code it |
| UN WPP 2024 | bulk_file | `https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Demographic_Indicators_Medium.csv.gz` (16.6 MB) | ETag / `Last-Modified` (2024-12-13) | The brief's `population.un.org/wpp/downloads` page is **404**. Biennial editions: pin the edition in `series_id` |
| PWT 11.0 | bulk_file | Dataverse `doi:10.34894/FABVLR` via `https://dataverse.nl/api/datasets/:persistentId/?persistentId=...`; `pwt110.xlsx` file id 554105 (5.8 MB) | Dataverse version (2.0, released 2025-10-10) | CC BY 4.0. dataverse.nl challenge, see Maddison |
| Maddison 2023 | bulk_file | Dataverse `doi:10.34894/INZBF2` (also linked from rug.nl/ggdc) | Dataverse version | **dataverse.nl started serving an Anubis anti-bot challenge (HTTP 200, HTML) after a few requests**, including on direct file downloads. Never try to get past it. Decide the route in session 2 (runner IPs may or may not be challenged) |

---

## Known limitations

- GitHub's scheduler is best effort: runs can start late or be skipped under load. Fine for daily or slower data; not suitable for intraday.
- The free tier caps the store at about 10 GB, so gridded climate data is stored as aggregates only, and large sources need `keep_raw: false` or a curated selection (see WDI).
- Licence fields are recorded by hand from each source's terms; they are a guide, not legal advice.
- `catalog/runs/` gains one small file per collected or failed run; compact it if it ever grows large.

---

## Open questions

- Vintage pruning: keep every vintage forever, or thin old ones (for example monthly after a year)?
- Licence for the public code (none chosen yet, which means all rights reserved).
- Full WDI (all 1,498 indicators): via REST with `keep_raw: false`, or the bulk zip as a `bulk_file` source?
- dataverse.nl access route for PWT and Maddison (session 2).
- Gate 3: monetisation direction, licence recheck, and whether this appears on domelayer.com.

---

## Changelog

- **2026-10-21 (session 1 closed):** "Done when" met. A local run landed vintage 2026-10-20 and a forced `collect` dispatch from Actions landed 2026-10-21 (run 39562732321); both verified at 759,559 rows. R2 refused a deliberate overwrite (create-only writes hold on the real bucket). The R2 token was rolled after its value was printed in an error; credential values are now shape-checked before any request.
- **2026-10-19 (session 1):** Repo scaffolded; registry with licence gate; R2 storage with create-only writes; World Bank WDI (72 indicators) end to end through the `rest` connector into raw, clean and catalog; `verify` recount via DuckDB; CI, gitleaks and the `collect` workflow. Step 0 endpoint findings recorded above.
- **2026-10-19:** Specified (Sprint OD); positioning, priority and spend decided (DOME_DECISIONS 2026-10-19).

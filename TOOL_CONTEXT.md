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
- **Status:** in development (Sprint OD Phase 2A, the Open Data dashboard: step 1, the ALFRED connector, and step 2, the licence gate, landed 2026-11-25; 11 registry entries)
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
public/ledger.json                                                  the ledger page's data (metadata only)
```
ALFRED (`fred_alfred`) is dated by the publisher, so it has its own layout, one folder per series:
```
raw/fred/alfred/series=<ID>/vintage=<publication date>/<ID>_<YYYYMMDD>.csv.gz   bytes as received
raw/fred/alfred/series=<ID>/run=<UTC time>-<suffix>/_manifest.json               commit record of one run
clean/economy/fred__alfred/series=<ID>/run=<UTC time>-<suffix>/part-0.parquet    rows that run collected
```
Read its clean files with `hive_partitioning = false` (the `series=` and `run=` folders would
otherwise become columns) and only through `alfred.committed_clean_keys`, never a bare glob: see
"ALFRED" below for why.
Raw and clean objects are written with a create-only put (`If-None-Match`), so an existing vintage
can never be overwritten. **Deviation from the brief:** `catalog/` is partitioned per source
instead of single `sources.parquet` / `series.parquet` / `runs.parquet` files, so the session 3
matrix jobs never read-modify-write the same object. Read with a glob and
`hive_partitioning = true`.

### Run semantics
- **Unchanged** change token: no raw, manifest, clean or catalog data is written, but the run still
  writes an `unchanged` run row, so the run history has no holes (since #12, 2026-11-16: the
  alert check reads that history, and a missing row cannot be told apart from a missing run).
- **Collected:** raw files, manifest, clean file, catalog entries, then a `collected` run row.
- **Failed:** whatever raw was written stays (it is the record of what the source sent); the clean
  write is skipped; a `failed` run row records the error.
- The change token of the last `collected` run is the comparison point, so a failed run is retried
  on the next schedule.

### The public ledger (`public/ledger.json`)

The one object in this bucket that leaves it. The collector assembles it with
`python -m dome_open_data ledger` at the end of every collect run (one job per workflow run, not
one per source) and of every ops weekly run; the website Worker reads that single key through a
read-only R2 binding and renders `domelayer.com/dome/open-data`. Decided 2026-11-17; rejected were
a Parquet read from the Worker (10 ms CPU) and committing the JSON to the public repository.

**It carries metadata, counts and provenance, and never an observation value.** That is what keeps
the bucket private and what makes the `commercial_ok: ask` licences irrelevant to it: those
restrict redistributing the data, not the fact that we collect it. Per source: id, publisher,
dataset, domain, endpoint, cadence, newest and first vintage, vintages held, age against
`expected_staleness_days`, series and row counts, the licence block, and the last five run rows
(status, times, vintage, rows, error). Plus store totals and an `as_of`.

Two rules the code enforces, both in `tests/test_ledger.py`:

- **Failures are published.** A failed run, a stale source and a source that has never collected
  all appear as they are. A page that hid them would be worthless as evidence.
- **An error message is redacted before it is published.** `ledger.public_error` strips the value
  of any `api_key`, `key`, `token`, `secret`, `password`, `auth` or `signature` query parameter and
  caps the text at 300 characters. No source needs a key today; FRED does, and Phase 2 adds it.

`run_history_complete_from` is published too, because unchanged runs wrote no row before #12
(2026-11-16): without it a reader would count the gaps as missed runs.

### ALFRED (`fred_alfred`, Phase 2A step 1, 2026-11-25)

Every published version of a US series, from the St. Louis Fed's archive, with no key. Code in
`src/dome_open_data/connectors/alfred.py` (requests and parsing) and `alfred.py` (the store).

- **Routes** (verified by the Phase 2A step 0 spike, 2026-11-25): the publication dates come from
  `alfred.stlouisfed.org/graph/api/series/?id=<ID>&mode=alfred`, at
  `chart_series[0].series_objects.a.available_revision_dates`; each version from
  `graph/alfredgraph.csv?id=<ID>&vintage_date=<D>&cosd=&coed=`. The JSON route is undocumented,
  so `parse_series_info` checks its shape (list present, ascending ISO dates, the series id asked
  for, a known frequency) and raises `ShapeError` on any change; the documented fallback is FRED
  API v1 with a key. `fredgraph.csv` on the `fred.` host ignores `vintage_date`.
- **Requests are serial** (parallel ones came back empty about one time in four), at the
  registry's 2 a second, with the usual retries, and an empty CSV is asked for again up to four
  times before the run fails. **The user agent is `http.USER_AGENT`, never a browser one:** Akamai
  Bot Manager blocks a browser user agent sent by a non-browser client (the 17 November timeouts).
  Each CSV's header must be `observation_date,<ID>_<YYYYMMDD>` for the date asked, or the run fails.
- **Two dates per object:** the raw key carries the **publication date**; the run manifest records
  DOME's **collection time**, the URL, the SHA-256 of the bytes as received, and the row count.
  The clean `vintage` column is the publication date.
- **The manifest is the commit.** Raw files, then the clean file, then the manifest, last. A run
  that dies part way leaves files nothing points at; the next run fetches those dates again and
  reuses an identical leftover raw file. Readers use `committed_clean_keys`, so a leftover is never
  read twice. A publication date committed by two manifests fails `verify`.
- **Write once.** Each run fetches the newest publication date already held again and compares the
  hash (`--recheck all` does every one, `none` skips it). Same hash: nothing written. **Different
  hash:** the new bytes go beside the first as `<ID>_<YYYYMMDD>.at=<UTC time>.csv.gz`, the manifest
  lists it under `conflicts`, the clean zone keeps the first version, and the run ends `conflict`,
  which `alert-check` always escalates. A held date that ALFRED stops listing is raised the same
  way.
- **Daily, new dates only.** `cadence: daily`, so `collect daily` runs it with the other daily
  sources; a run fetches only publication dates not yet held. `options.max_new_per_run` (1,500)
  stops a scheduled run that meets a whole new series from outrunning the job timeout: the rest
  is collected on the following days. A backfill is one dispatch of `collect` with `source:
  fred_alfred` (optionally `series:`), which has a 60 minute timeout.
- **Copyright:** a series whose ALFRED notes mention a copyright is refused before any CSV is
  fetched, because this entry is public domain only (BEA, BLS, Federal Reserve Board).
- **Units:** levels are in each version's own units (chained 2000 dollars in 2008, 2017 dollars
  today) and the CSV does not say which, so `unit` is `as_published` and the current units are in
  the series catalogue. **Compare growth rates across versions, never levels.**
- **Run rows:** `rows_raw` counts new publication dates, `vintage` is the newest one collected,
  `change_token` reads like `GDPC1 +421`.
- **Proved by a test on real bytes** (`tests/fixtures/alfred/`, the 2007 and 2008 rows of `GDPC1`
  as served for three dates): Q1 2008 annualised growth reads **+0.60%** as published on
  2008-04-30, **-0.72%** after the 2009-07-31 annual revision, and **-1.70%** on 2026-11-25.
- Series held: `GDPC1` (421 publication dates on 2026-11-25, 1991-12-04 onward).

### The licence gate (Phase 2A step 2, 2026-11-25)

A source puts **values** in front of the public only if its registry entry has
`commercial_ok: "yes"` (which is also how a public-domain source is entered); `ask` and `no`
sources contribute counts and provenance only. `src/dome_open_data/licence.py`:

- **`publish` is the only writer under `public/`.** It runs `check_public` and refuses the write on
  any failure; the ledger goes through it too. A test fails if another module writes a `public/`
  key.
- In a public payload values travel only in a **value block**, a mapping with a `value` or `values`
  key, however deep. Each must name a registry `source_id` whose licence allows values, and carry
  that source's registry `attribution` character for character. `value_block` builds one with the
  attribution from the registry, so the app never types it.
- `tests/test_licence.py` puts a value from each `ask` source (the three IMF ones today) into a
  dashboard-shaped payload and asserts the write is refused and nothing reaches the store.

### Common time-series model
`series_id · geo (ISO alpha-3) · period (start date) · freq · value · unit · obs_status · vintage`.
`series_id` is `{publisher}.{dataset}.{code}.{geo}.{freq}`, for example
`worldbank.wdi.NY.GDP.MKTP.CD.ITA.A`. `geo` is the ISO alpha-3 code for countries and territories
(publisher codes that differ are mapped through the crosswalk, e.g. IMF `KOS` to `XKX`), the
ISO 3166-3 former code for historical entities (`SUN`, `YUG`, `CSK`), and the publisher's own
code for aggregates (`WLD`, `G001`, `UN900`, `EMU`), all listed in `crosswalk/`. `obs_status =
projection` marks forecasts (WEO from the vintage year, WPP from 2024). Rows with no value are dropped (counted in the manifest).
Non-time-series sources (filings, tenders, events, boundaries) keep their own shape.

---

## Governance

### Events emitted
None. The pipeline does not write `GovernanceEvent` rows: no AI decision, no user action.

### Rules applied
- **Licence gate:** a registry entry without `licence.name`, `commercial_ok`, `share_alike` and `attribution` is refused.
- **Validation gate (session 1):** schema equals the common model, at least one row, row count equals the raw recount, no nulls in key columns, no NaN, no duplicate `(series_id, period)`. Session 3 adds the vintage-to-vintage comparison (missing column, more than 50% fewer rows).
- **Response guard:** a source that answers with anything but what it should (JSON, SDMX-CSV, or an xlsx/gzip file checked by its leading bytes), for example a 200 HTML bot challenge, fails the run before anything is stored. Challenges are never worked around.
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
- `crosswalk/countries.csv`, `crosswalk/aggregates.csv`: country and aggregate codes, generated by
  `python -m dome_open_data crosswalk` from the UN WPP location list, the World Bank country API
  and the IMF codes WEO and ER use, plus the hand-kept `crosswalk/manual.csv` (Kosovo, the
  Channel Islands, historical states and their successors) and `crosswalk/overrides.csv`
  (publisher codes that differ from ISO). A code that matches nothing fails the build.

### Registry schema
As in `dome-docs/sprints/SPRINT_OD_OPEN_DATA.md`, plus four fields:
- `publisher`, `dataset`: build the storage paths; `id` must equal `{publisher}_{dataset}`
- `dialect`: which API flavour or file layout a connector speaks: `worldbank` (`rest`); `imf`,
  `ecb` (`sdmx`); `pinksheet`, `wpp`, `pwt`, `maddison` (`bulk_file`)
- `options`: connector-specific settings, validated by the connector. WDI: `source_id`,
  `start_year`, `indicators`. sdmx: `flow`, `key`, `start_period`, `freq_dimension`,
  `code_dimensions`, `geo_dimension` or a fixed `geo`, `series` (title and unit per code; required
  for `imf`), `unit_template`, `projection_from_vintage_year`, `source_url`. bulk_file: exactly one
  of `url`, `discover` (`page`, `pattern`), `dataverse` (`doi`, `filename`), plus `file_type`
  (`xlsx` or `gzip`, checked by magic bytes), `sheet`, `series`, `exclude`, `loc_types`,
  `projection_from`, `geo`
- `change_signal` adds `dataset_version` (IMF flow versions, dataverse dataset version)

Quote `"yes"` / `"no"` in `commercial_ok`: YAML 1.1 reads them as booleans (the loader maps a
boolean back, but quoting keeps the file honest).

---

## Sources

Verified against the live sources on **2026-10-19** (Sprint OD Step 0) and the session 2
sources again on **2026-10-21** (session 2 Step 0, then a full local collection of each).

| Source | Connector | Endpoint (verified 2026-10-19) | Change signal | Quirks |
|---|---|---|---|---|
| World Bank WDI | rest (`worldbank`) | `https://api.worldbank.org/v2/`; data `country/all/indicator/{code}?source=2&date=1960:YYYY&per_page=20000`; metadata `source/2/indicator` | `lastupdated` on `sources/2` (2026-10-08), plus the indicator list and start year | HTTP `Last-Modified` on the API is the CDN cache time, useless. No key. Errors come back as 200 `[{"message": ...}]`. Responses can take over 90 s: timeouts are retried. Full WDI is 1,498 indicators, about 300 MB raw per vintage (bulk `WDI_CSV.zip` is 283 MB with a real ETag), so session 1 collects a **curated 72 indicators** (scope A): about 10.7 MB raw gzip and 4.9 MB clean per vintage, 760k rows, about 2 minutes. `geo` still includes World Bank aggregate codes (`WLD`, `EUU`, `AFE`...) until the session 2 crosswalk |
| IMF WEO (`imf_weo`) | sdmx (`imf`) | `https://api.imf.org/external/sdmx/2.1/data/IMF.RES,WEO/{key}` with `Accept: application/vnd.sdmx.data+csv;version=2.0.0`, `detail=dataonly` | Versions of `WEO` and its named vintage flows (`WEO_2025_OCT_VINTAGE`, `WEO_2026_APR_VINTAGE` on 2026-10-21) from `dataflow/IMF.RES/all/latest` | No key. `detail=dataonly` cuts the 20-indicator pull from 390 MB to 19.5 MB, but drops unit and scale attributes, so titles and units live in the registry; values then come in **full units** (dollars, persons), not scaled. 210 geo codes: ISO alpha-3, 13 IMF aggregates (`G001`...), and `KOS`, `WBG` (mapped to `XKX`, `PSE`). Runs to vintage year + 5: periods from the vintage year on are `obs_status = projection`. 177,804 rows |
| IMF exchange rates (`imf_er`) | sdmx (`imf`) | `IMF.STA,ER`, key `COUNTRY.INDICATOR.TYPE_OF_TRANSFORMATION.FREQUENCY`; collected `.XDC_USD.PA_RT.A+M` | As WEO (`ER`, `ER_2026_MAY_VINTAGE`...) | An all-wildcard key answers **403**; a wildcard country with a fixed indicator works. Monthly periods are written `1986-M01`. Euro area members appear only until euro adoption. Historical entities present (`SUN`, `YUG`, `CSK`, `DDR`, `ANT`, `YAR`, `YMD`). One Zimbabwe row is dated 2027-03 as published (left as received; session 3's gate should flag future periods). 171,450 rows |
| ECB key rates (`ecb_policy_rates`), euro FX (`ecb_fx`) | sdmx (`ecb`) | `https://data-api.ecb.europa.eu/service/data/{flow}/{key}?format=csvdata&detail=dataonly`; `FM/B.U2.EUR.4F.KR.MRR_FR+DFR+MLFR.LEV`, `EXR/D..EUR.SP00.A` | `Last-Modified` on a `lastNObservations=1` request: follows the data (10 Sep 2026 policy decision, yesterday's fix) and is stable across calls | No key. **`updatedAfter` with nothing new returns 404, not 304**, so it is not used. Policy rates are business-daily dates of change (`B`, stored as `D`). Titles from the full-detail latest observation, kept as raw `_series.csv`. `geo = EMU`. 186 and 263,338 rows |
| Pink Sheet (`worldbank_pinksheet`) | bulk_file (`pinksheet`) | Link discovered on `https://www.worldbank.org/en/research/commodity-markets` (one match required); 2026-10-21: `.../74e8be41ceb20fa0da750cda2f6b9e4e-0050012026/related/CMO-Historical-Data-Monthly.xlsx` | Discovered URL + `Last-Modified` (2026-10-02) | Sheet `Monthly Prices`: names row, units row, then `1960M01` periods; `…` means no value. 71 series, code = slug of the name (`CRUDE_OIL_BRENT`), `geo = WLD`. The `Monthly Indices` sheet (nested headers) is not collected yet. 50,451 rows |
| UN WPP 2024 (`un_wpp`) | bulk_file (`wpp`) | `https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Demographic_Indicators_Medium.csv.gz` (16.6 MB, gzip magic checked) | ETag (`"0x8DD1BA9EBFD1565"`, 2024-12-13) | The `population.un.org/wpp/downloads` page is **404**. 21 curated indicators; location types Country/Area, World, Geographic region, Subregion (aggregates become `UN<LocID>`, e.g. `UN900` World). 1950 to 2100: 2024 onward is `projection`. 843,486 rows |
| PWT 11.0 (`ggdc_pwt`) | bulk_file (`pwt`) | dataverse.nl API `api/datasets/:persistentId/?persistentId=doi:10.34894/FABVLR`, file `pwt110.xlsx` (id 554105) via `api/access/datafile/{id}` | Dataverse version (2.0) + file id + checksum | Downloads redirect to SURF object storage (`objectstore.surf.nl`). **No Anubis challenge on 2026-10-21**, from a home connection; the xlsx magic check turns any HTML answer into a failed run, never a workaround. Titles and units from the file's Legend sheet; `i_*` flags and `cor_exp` excluded. 416,285 rows |
| IMF PortWatch chokepoints (`imf_portwatch`) | bulk_file (`arcgis_hub`) | Change signal from `editingInfo.lastEditDate` on the FeatureServer layer; file from the Hub CSV export `hub.arcgis.com/api/download/v1/items/3da2b9ca97684916b75c4013f95d18ab/csv?layers=0` | `editingInfo.lastEditDate` (1793715201792 on 2026-11-06) + the options hash | 28 chokepoints x 14 measures x every day since 2019-01-01, about 1.1 M observations a vintage, so **weekly** even though the data is daily (it also runs about ten days behind). `geo` holds the chokepoint id (`chokepoint1`), not a country; the name is in the series title. Counts are transit calls (one ship per 48 h); `capacity_*` are estimated trade volumes in metric tons (IMF WP/2021/225), not maximum capacity. **The quota is the constraint, see the Step 0 section above**, and on 2026-11-06 the Hub export itself was failing to regenerate | IMF terms, `commercial_ok: ask` |
| ALFRED (`fred_alfred`) | alfred | `https://alfred.stlouisfed.org/`: dates `graph/api/series/?id=<ID>&mode=alfred`, versions `graph/alfredgraph.csv?id=<ID>&vintage_date=<D>&cosd=&coed=` (verified 2026-11-25) | The publication date list itself | No key. Akamai blocks a browser user agent from a non-browser client; parallel requests can come back empty. One CSV per publication date. See "ALFRED" above. Public domain (US federal series only, copyright notes refused) |
| Maddison 2023 (`ggdc_maddison`) | bulk_file (`maddison`) | Same API, `doi:10.34894/INZBF2`, file `mpd2023_web.xlsx` (id 421302) | Dataverse version (1.0) + file id + checksum | Sheet `Full data`, years from 1 AD to 2022 (period `0001-01-01` is valid). Historical codes `CSK`, `SUN`, `YUG`; `SDN` is former Sudan. When fewer than 12 countries are shown, the original papers must be cited (in the attribution). 39,241 rows |

### Session 3 Step 0: IMF PortWatch chokepoints (verified 2026-11-06)

The only new source in session 3. Verified live before any connector was written.

- **Service:** `https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/FeatureServer/0`,
  an ArcGIS **Table** (no geometry), owner `IMF-portwatch_imf_dataviz`. Found through the ArcGIS Hub
  dataset API, not guessed: the org id in a guessed URL answers `Invalid URL`.
- **Shape:** 79,940 rows on 2026-11-06, 21 fields: `date` (DateOnly), `year`, `month`, `day`,
  `portid`, `portname`, then `n_*` vessel counts and `capacity_*` values by vessel class, plus
  `ObjectId`. `maxRecordCount` 1000, `supportsPagination` and `supportsOrderBy` true.
- **Coverage:** `date` runs 2019-01-01 to **2026-10-25**, so the daily series lags about ten days.
  An `expected_staleness_days` of 7 would cry wolf; 21 is the realistic setting.
- **The quota is the real constraint.** The service enforces the IMF org's own limit of 6,000
  request units a minute, shared across every consumer, and it was **already over quota on
  2026-11-06** with no load from here (6,256 to 6,694 units reported on four separate attempts,
  minutes apart). Paged reads could not be exercised live for that reason, twice.
- **A 429 arrives as HTTP 200** with `{"error": {"code": 429, ...}}` in the body, exactly like the
  World Bank's 200 errors. Any connector here has to parse the body and raise, never trust the status.
- **A bulk CSV export exists** for the same table through ArcGIS Hub:
  `https://hub.arcgis.com/api/download/v1/items/3da2b9ca97684916b75c4013f95d18ab/csv?layers=0`.
  On 2026-11-06 it answered **202** with `{"status": "PagingData", "progressInPercent": 5}`, meaning
  the Hub was regenerating it; a 200 carries the finished file. One request per vintage instead of
  eighty, and the Hub does the paging against the quota on its own side.
- **Licence:** IMF terms, so `commercial_ok: ask`, like the other IMF sources.

---

## Unattended operation (session 3)

Four schedules, one per cadence, each a matrix over the registry entries with that cadence.
`collect-cadence.yml` holds the work and the four `collect-<cadence>.yml` files are the
schedules; `collect.yml` stays for collecting one source on demand.

| Workflow | When | What runs |
|---|---|---|
| `collect-daily` | 05:23 UTC daily | every `cadence: daily` entry (`ecb_fx`) |
| `collect-weekly` | Mondays 05:41 UTC | `ecb_policy_rates`, `imf_portwatch` |
| `collect-monthly` | the 1st, 05:47 UTC | `worldbank_wdi`, `imf_weo`, `imf_er`, `worldbank_pinksheet` |
| `collect-yearly` | 2 January, 05:53 UTC | `un_wpp`, `ggdc_pwt`, `ggdc_maddison` |
| `ops weekly` | Mondays 06:37 UTC | freshness, the storage guard, the heartbeat commit |

- **The matrix is read from `sources.yaml` at run time** (`python -m dome_open_data ids --cadence
  weekly`), so adding a source never means editing a workflow. `fail-fast: false`, two jobs at a
  time, one concurrency group per cadence.
- **The vintage gate** (`validate_against_previous`) runs after the schema checks and before the
  clean write. It blocks a vintage that holds less than half the previous vintage's rows or
  series: a truncated download or a publisher mid-revision never replaces good data. Columns and
  dtypes need no comparison because every clean file is written against the one
  `TIMESERIES_SCHEMA`. Smaller drops pass: revisions legitimately remove rows, and an alert on
  every revision would be ignored within a month.
- **One open issue per subject** (`ops/alert.sh`, label `open-data`): the first failure opens it,
  later failures comment, a success closes it. Subjects are each source, stale sources, and the
  storage guard.
- **Freshness** compares each source's newest clean vintage with its own
  `expected_staleness_days`; a source never collected counts as stale. Exit 2 means it found
  something, which is not the same as the check breaking.
- **The storage guard** sums the bucket and alerts at 8 GiB of the 10 GB free tier. On 2026-11-06
  the store held 205 objects, 0.07 GiB.
- **The heartbeat** commits `ops/heartbeat.txt` weekly, because GitHub disables scheduled
  workflows after 60 days without repository activity and a workflow run does not count.
- **The run log** was already in place from session 1: one Parquet file per run under
  `catalog/runs/source=<id>/`, including failed runs. Nothing was added for it.

---

## Known limitations

- GitHub's scheduler is best effort: runs can start late or be skipped under load. Fine for daily or slower data; not suitable for intraday.
- The free tier caps the store at about 10 GB, so gridded climate data is stored as aggregates only, and large sources need `keep_raw: false` or a curated selection (see WDI).
- Licence fields are recorded by hand from each source's terms; they are a guide, not legal advice.
- `catalog/runs/` gains one small file per run, including runs that found nothing new (one row per
  run, as the sprint brief asks, and what the alert rule counts). About 3,700 small objects a year
  at the current cadences; compact it if that ever matters.
- `imf_portwatch` depends on ArcGIS Hub regenerating its CSV export. That is outside this
  pipeline's control, and it was broken when the source was added (see Open questions).

---

## Open questions

- Vintage pruning: keep every vintage forever, or thin old ones (for example monthly after a year)?
- **Do `un_wpp` and `ggdc_pwt` change tokens flap?** Both collected on 2026-11-16 with row
  counts identical to their 2026-10-21 vintages (843,486 and 416,285), so the publisher's
  signal moved while the data did not: an ETag for WPP, a Dataverse version plus checksum for
  PWT. At a yearly cadence that costs one duplicate vintage, so it is not urgent, but if it
  repeats on 2 January the signal needs a closer look (content hash rather than ETag).
- Licence for the public code (none chosen yet, which means all rights reserved).
- Full WDI (all 1,498 indicators): via REST with `keep_raw: false`, or the bulk zip as a `bulk_file` source?
- dataverse.nl from GitHub runners: no challenge on 2026-10-21 (both files downloaded from a runner);
  one connect timeout on a download. If a challenge ever appears the run fails cleanly and the
  fallback is a manual local run, not a workaround.
- Pink Sheet `Monthly Indices` and IMF ER end-of-period rates: not collected yet.
- PortWatch: whether to add the paged `rest` path after all. Francesco chose the Hub CSV export
  on 2026-11-06 and that is what is built, but the export has not been collected once: between
  10:24 and 11:12 UTC that day it answered 202 at 5% generated six times and HTTP 500 "Unable to
  perform query. Too many requests." four times, having been stuck since 2026-11-03. The weekly
  job therefore fails cleanly and opens one issue until the Hub recovers. If it never does, the
  options are paging the service with heavy backoff or dropping the source.
- Gate 3: monetisation direction, licence recheck, and whether this appears on domelayer.com.

---

## Changelog

- **2026-11-25 (Phase 2A steps 1 and 2):** the ALFRED connector (`fred_alfred`, `GDPC1` first,
  daily, new publication dates only, write once with a hash check, conflicts written beside and
  escalated) and the licence gate (`licence.py`: `publish` is the only writer under `public/`,
  values only from `commercial_ok: yes` sources, attribution from the registry). `collect` gained
  `series` and `recheck` inputs and a 60 minute timeout for backfills. A test reproduces the 2008
  revision story from real bytes.
- **2026-11-16 (alerting rule, #12 `7e96c3b`):** an issue now means "something is broken", not "a
  publisher had a bad minute". After a failed run, `alert-check` reads the source's own run history:
  a daily source is given one failure (its next attempt is hours away), slower cadences none, and a
  stale source alerts whatever its cadence. The retry window stays at four attempts on purpose.
  Unchanged runs now write their own row, so the history has no holes for the rule to miscount.
  **Proved live within the hour, by accident:** a `collect yearly` dispatch hit a dataverse.nl
  connect timeout on `ggdc_maddison`, the rule said "alert: 1 failed run in a row" (yearly, no
  grace), **issue #13 opened carrying that reason**, the next dispatch collected 39,241 rows and
  **#13 closed itself**. In the same two runs `un_wpp` and `ggdc_pwt` reported `unchanged` and left
  their rows, which is the new behaviour working in production. `ecb_fx`'s #11 stays open until its
  next successful daily run; under this rule its 15 November blip would never have opened it.
- **2026-11-16 (ten days unattended):** nothing was dispatched by hand. Nine daily runs (seven
  vintages for `ecb_fx`, two `unchanged` over the weekend because the ECB publishes no weekend fix,
  one failure), one weekly run in which **`imf_portwatch` collected 1,121,904 rows and 392 series**
  after the Hub export recovered by itself, and one ops weekly that found everything fresh, closed
  issue #8 and committed the heartbeat (`dd057e7`). Alerting behaved: **#10 closed itself** on the
  PortWatch recovery, **#11 opened** when the **ECB answered 503 on all four attempts** on
  15 November. That failure is upstream and transient (200 in 0.8 s the next morning), so **no fix
  was applied**: widening the retry window would hide exactly the flakiness Gate 1 measures, and the
  next scheduled run collects the same series. Store 250 objects, 0.09 GiB. **Gate 1 counts the nine
  sources that were refreshing (Francesco, 2026-11-16); PortWatch is tracked, not gating.**
  Scheduler note: runs land **6 to 8 hours after their cron**, which is best-effort scheduling on a
  public repository and harmless at these cadences.
- **2026-11-06 (session 3 verified from `main`):** the "done when" is met, by dispatch.
  `ops weekly` opened **one** issue (#8) with the freshness report; `collect daily` collected
  `ecb_fx` vintage 2026-11-06 (263,686 clean rows, 44 series, verified, through the new gate) so it
  stopped being stale; a deliberate break of `ecb_policy_rates` on a throwaway branch ran **both**
  weekly jobs independently and opened **one issue per source** (#9 broken, #10 PortWatch), no pile;
  the same workflow from `main` then **closed #9** with a "Collected again" comment (an `unchanged`
  run, so nothing was written and the 2026-10-21 vintage was re-verified) while **commenting on #10
  instead of opening another**; a second `ops weekly` **commented on #8** and the stale list had
  shrunk from two sources to one. The first dispatch had failed on `bash -e` swallowing the check's
  exit code (#7). Still open and expected: #8 and #10, both about `imf_portwatch`, until the Hub
  export recovers.
- **2026-11-06 (session 3):** cadence schedules, the vintage gate, one issue per subject, the
  weekly freshness and storage checks, the heartbeat, and the `arcgis_hub` dialect with the
  `imf_portwatch` entry. `ruff`, `mypy` and 86 tests green. Verified live: `freshness` against R2
  found `ecb_fx` 16 days stale against its 5-day limit (nothing has been scheduled since session
  2, which is the point of the check), `usage` reported 205 objects and 0.07 GiB, and a real
  `run imf_portwatch` failed cleanly on the Hub export (four attempts, HTTP 500, exit 1, nothing
  written but the failed run row). The workflows themselves can only be exercised once they are
  on `main`.
- **2026-11-06 (session 3 Step 0):** repo clean at `b19ed52`, no open PRs. Sprint drift fixed in
  `dome-docs` (`d5ca1f3`). IMF PortWatch chokepoints verified live: service URL, 21 fields, 79,940
  rows, 2019-01-01 to 2026-10-25, 1000-row pages, a shared quota that was already exceeded, 429 as
  HTTP 200, and a Hub CSV export as the alternative. Details above.
- **2026-10-21 (session 2 landed):** #4 merged (`d9c5dc7`). One `collect` dispatch per new source from Actions: all eight collected and verified in R2 at the same row counts as the local runs (ecb_policy_rates 186, ecb_fx 263,338, imf_weo 177,804, imf_er 171,450, worldbank_pinksheet 50,451, un_wpp 843,486, ggdc_pwt 416,285, ggdc_maddison 39,241). dataverse.nl served the runner real files, no challenge. Maddison's first run failed on a connect timeout to the file download (run 39577297911, a clean failure, nothing stored but the failed run row); the retry passed (run 39577678303).
- **2026-10-21 (session 2):** `sdmx` connector (dialects `imf`, `ecb`) and `bulk_file` connector (fixed URL, link discovered on a page, dataverse dataset; dialects `pinksheet`, `wpp`, `pwt`, `maddison`) with a magic-byte guard. Eight new registry entries: `imf_weo`, `imf_er`, `ecb_policy_rates`, `ecb_fx`, `worldbank_pinksheet`, `un_wpp`, `ggdc_pwt`, `ggdc_maddison`. Country crosswalk (246 countries incl. historical, 126 aggregates) and its builder. Each source collected locally end to end, verified, and a second run wrote nothing (change detection). R2 landing waits for the merge.
- **2026-10-21 (session 1 closed):** "Done when" met. A local run landed vintage 2026-10-20 and a forced `collect` dispatch from Actions landed 2026-10-21 (run 39562732321); both verified at 759,559 rows. R2 refused a deliberate overwrite (create-only writes hold on the real bucket). The R2 token was rolled after its value was printed in an error; credential values are now shape-checked before any request.
- **2026-10-19 (session 1):** Repo scaffolded; registry with licence gate; R2 storage with create-only writes; World Bank WDI (72 indicators) end to end through the `rest` connector into raw, clean and catalog; `verify` recount via DuckDB; CI, gitleaks and the `collect` workflow. Step 0 endpoint findings recorded above.
- **2026-10-19:** Specified (Sprint OD); positioning, priority and spend decided (DOME_DECISIONS 2026-10-19).

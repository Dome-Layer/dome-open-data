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
- **Status:** in development (Sprint OD, Phase 1, session 2 done: 9 registry entries)
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
| Maddison 2023 (`ggdc_maddison`) | bulk_file (`maddison`) | Same API, `doi:10.34894/INZBF2`, file `mpd2023_web.xlsx` (id 421302) | Dataverse version (1.0) + file id + checksum | Sheet `Full data`, years from 1 AD to 2022 (period `0001-01-01` is valid). Historical codes `CSK`, `SUN`, `YUG`; `SDN` is former Sudan. When fewer than 12 countries are shown, the original papers must be cited (in the attribution). 39,241 rows |

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
- dataverse.nl from GitHub runners: no challenge from a home connection on 2026-10-21; the first
  scheduled runs (session 3) will show whether runner IPs are challenged. If they are, the run
  fails cleanly and the fallback is a manual local run, not a workaround.
- Pink Sheet `Monthly Indices` and IMF ER end-of-period rates: not collected yet.
- Gate 3: monetisation direction, licence recheck, and whether this appears on domelayer.com.

---

## Changelog

- **2026-10-21 (session 2):** `sdmx` connector (dialects `imf`, `ecb`) and `bulk_file` connector (fixed URL, link discovered on a page, dataverse dataset; dialects `pinksheet`, `wpp`, `pwt`, `maddison`) with a magic-byte guard. Eight new registry entries: `imf_weo`, `imf_er`, `ecb_policy_rates`, `ecb_fx`, `worldbank_pinksheet`, `un_wpp`, `ggdc_pwt`, `ggdc_maddison`. Country crosswalk (246 countries incl. historical, 126 aggregates) and its builder. Each source collected locally end to end, verified, and a second run wrote nothing (change detection). R2 landing waits for the merge.
- **2026-10-21 (session 1 closed):** "Done when" met. A local run landed vintage 2026-10-20 and a forced `collect` dispatch from Actions landed 2026-10-21 (run 39562732321); both verified at 759,559 rows. R2 refused a deliberate overwrite (create-only writes hold on the real bucket). The R2 token was rolled after its value was printed in an error; credential values are now shape-checked before any request.
- **2026-10-19 (session 1):** Repo scaffolded; registry with licence gate; R2 storage with create-only writes; World Bank WDI (72 indicators) end to end through the `rest` connector into raw, clean and catalog; `verify` recount via DuckDB; CI, gitleaks and the `collect` workflow. Step 0 endpoint findings recorded above.
- **2026-10-19:** Specified (Sprint OD); positioning, priority and spend decided (DOME_DECISIONS 2026-10-19).

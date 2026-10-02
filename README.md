# dlt-demos

Incremental ("delta") data loading pipelines for corporate SaaS sources, built on [dlt](https://dlthub.com/) (data load tool). Each source is a self-contained pipeline that pulls records, infers/applies column type hints, and loads them into a destination warehouse using full ("replace") or incremental ("merge") loads.

Currently included:

| Source | API used | Auth | Notable resources |
|---|---|---|---|
| [`netsuite/`](netsuite) | NetSuite REST + SuiteQL | OAuth 2.0 (JWT client assertion, PS256) | `Account`, `Customer`, `Employee`, `Transaction`, `TransactionLine`, ... |
| [`salesforce/`](salesforce) | Salesforce Bulk API 2.0 (via `simple-salesforce`) | consumer key/secret + domain | `Account`, `Contact`, `Lead`, `Opportunity`, `RecordType` |

Both pipelines share the same shape:
- A `@dlt.source` in `source/__init__.py` declares one `@dlt.resource` per object, each tagged `replace` (full load) or `merge` (incremental, keyed off a last-modified/created cursor field).
- `source/helpers.py` fetches field metadata from the API (NetSuite's `metadata-catalog`, Salesforce's `describe`) to build dlt column hints and generate the SuiteQL/SOQL query for each resource, including the incremental `WHERE` clause.
- A handful of NetSuite resources (`DeletedRecord`, `Entity`, `TransactionLineLink`) use static column hints from `hints/*.json` instead, because their metadata endpoint doesn't return filterable fields.
- `<source>_pipeline.py` is the CLI entry point used to run a single resource. Each resource gets its own `pipeline_name using f"{Source}_{resource_name}"` while sharing one `dataset_name` per source — so every resource has its own `_dlt_state`/`_dlt_version`/`_dlt_loads` entries. This is done so resources can run in parallel without contending over shared state, and in production, a failed resource only needs that resource re-run, not a full rerun of the whole source.

## Prerequisites

- Python >= 3.13
- A destination dlt supports (defaults to local [DuckDB](https://duckdb.org/) in these demos — see `dlt`'s [destination docs](https://dlthub.com/docs/dlt-ecosystem/destinations/) to point at something like Azure SQL/MSSQL, Snowflake, BigQuery, etc.)
- Credentials for whichever source(s) you intend to run:
  - **NetSuite**: an integration record configured for OAuth 2.0 client credentials (M2M), with an uploaded certificate giving you `client_id`, `private_key` (PEM), and `kid`, plus your account's `account_id`.
  - **Salesforce**: a connected app's `consumer_key`/`consumer_secret` and your org's `domain` (e.g. `yourcompany.my`).

## Setup

```bash
git clone https://github.com/rando-brando/dlt-demos.git
cd dlt-demos
uv sync
```

Dependencies are declared in [`pyproject.toml`](pyproject.toml) (dlt with the `duckdb`, `mssql`, and `parquet` extras, `simple-salesforce`, `requests-oauthlib`, etc.) and resolved/installed by [uv](https://docs.astral.sh/uv/) — no separate requirements file. Run pipeline scripts with `uv run`, e.g. `uv run netsuite_pipeline.py Customer`.

### Configure secrets

Each source directory has its own `.dlt/` folder with a `config.toml` (non-secret runtime/destination settings, already checked in) and a `secrets.toml` (credentials, **gitignored** — create it yourself, never commit it).

`netsuite/.dlt/secrets.toml`:

```toml
[sources.netsuite.credentials]
client_id = "..."
private_key = "..."
kid = "..."

[sources.netsuite]
account_id = "..."

[destination.duckdb.credentials]
# only needed if you change the destination away from a local DuckDB file
```

`salesforce/.dlt/secrets.toml`:

```toml
[sources.salesforce.credentials]
consumer_key = "..."
consumer_secret = "..."
domain = "yourcompany.my"
```

If you repoint `destination` in `<source>_pipeline.py` at something other than DuckDB (e.g. `mssql`), add a matching `[destination.<name>.credentials]` block with that destination's connection details.

## Usage

Each pipeline loads one resource (table) at a time:

```bash
cd netsuite
uv run netsuite_pipeline.py Customer
```

```bash
cd salesforce
uv run salesforce_pipeline.py Opportunity
```

For incremental resources, pass `--field` to bind a custom incremental cursor with `--start`/`--end` bounds to backfill a specific window. Because `--end` is set, this does not advance the resource's stored watermark — safe to re-run without disturbing normal incremental loads:

```bash
uv run netsuite_pipeline.py Transaction --field lastmodifieddate --start 2026-01-01 --end 2026-02-01
```

Load info (rows loaded, schema changes, etc.) is printed to stdout after each run, and dlt logs the generated SuiteQL/SOQL query for visibility into exactly what was pulled.

## Extending

To add a new resource to a source:
1. Add a `@dlt.resource` function in that source's `source/__init__.py`, choosing `replace` or `merge` and a `primary_key`.
2. For `merge` resources, pick a last-modified-style cursor field for `dlt.sources.incremental(...)`.
3. If the API's metadata endpoint doesn't expose the fields you need (as with some NetSuite joined/derived tables), add a static hints file under `hints/` and load it with `file_hints(...)` instead of `metadata_hints(...)`.
4. Add the new resource to the tuple returned at the bottom of the source function.

## Security notes before you publish your own secrets

- Double-check `.dlt/secrets.toml` under both `netsuite/` and `salesforce/` stays untracked (`git status` / `git check-ignore -v <path>` should confirm this) before pushing — `.gitignore` already excludes `secrets.toml` and `*.secrets.toml`, but it's worth verifying in your own fork.
- This repo has no `LICENSE` file yet. Add one if you want to make the terms of reuse explicit for anyone pulling from this public repo.

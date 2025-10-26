
# Algorithm Explanation

This project automates ingestion of a dataset from data.gov.ua into PostgreSQL 
and provides an optional export of cases by a list of case numbers. The end‑to‑end pipeline 
runs from metadata discovery to resilient downloading, archive unpacking, CSV normalization, bulk import, 
and deduplicated merging.

## High‑Level Flow

1) Read configuration from `.env`:
   - `DATASET_ID` — dataset identifier on data.gov.ua.
   - `DATABASE_URL_SYNC` — PostgreSQL DSN for asyncpg (import/export), e.g. `postgresql://user:pass@host:5432/db`.

2) Fetch dataset metadata (`src/fetch_metadata.py`):
   - `fetch_dataset_metadata(dataset_id)`: calls `https://data.gov.ua/api/3/action/package_show?id=...`, checks `success`, returns `result`.
   - `extract_resources(metadata)`: extracts only required fields from `resources`: `name`, `description`, `format`, `url`.
   - In `src/main.py` resources are filtered to supported formats: `CSV` and `ZIP`.

3) Download resources concurrently with retries (`src/resource_downloader.py`):
   - Uses `ThreadPoolExecutor` for parallel downloads and `tqdm` for progress.
   - HTTP session is configured with retry/backoff (`Retry(total=5, backoff_factor=2)`) and `HEAD` to detect remote sizes.
   - Resume support: if a partial file exists, sets `Range` header; handles `200/206/416` and `Content-Length` mismatches by safely restarting.
   - `download_all_files(...)` aggregates per‑resource results with `status` and either `path` or `error`.

4) Unpack ZIP archives (`src/zip_unpacker.py`):
   - `unpack_zip(path, output_dir)`: extracts archives into `data/unpacked` and returns only `.csv` file paths.
   - Corrupted archives (`BadZipFile`) are handled gracefully (empty result, logged message).

5) Normalize CSVs (`src/csv_to_db.py::normalize_csv`):
   - Detect encoding via `charset_normalizer`; read with `pandas.read_csv` using automatic delimiter detection (fallback to `;`).
   - Standardize headers: lower‑case, trim, replace spaces/dashes with underscores, remove BOM.
   - Ensure all 13 `EXPECTED_COLUMNS` are present; add missing as `None` and reorder to the fixed schema:
     `court_name, case_number, case_proc, registration_date, judge, judges, participants, stage_date, stage_name, cause_result, cause_dep, type, description`.
   - Clean values: trim, remove non‑breaking spaces, normalize `case_number` and `court_name`.
   - Filter rows: drop records lacking `case_number` or `court_name`.
   - Deduplicate by `case_number` (keep first occurrence).
   - Parse dates from `dd.mm.yyyy` to date objects (`utils.parse_date`); invalid dates become `NULL`.
   - Write a headerless, cleaned CSV (`*.clean`) for fast bulk import via `COPY`.

6) Import into PostgreSQL (`src/csv_to_db.py::import_csv_files`):
   - Connect with `asyncpg` using `DATABASE_URL_SYNC` and start a transaction.
   - Create a temporary table `tmp_cases` (dropped on commit).
   - Sequentially `COPY` each cleaned file into `tmp_cases` (Semaphore(1) ensures no concurrent `COPY`).
   - Merge into `cases` table:
     - Insert distinct rows by `case_number` using `SELECT DISTINCT ON (case_number)` ordered by `stage_date DESC` to pick the latest stage per case.
     - On conflict (`case_number`) update using the latest `stage_date`, and apply `COALESCE` so new non‑null fields overwrite nulls while preserving existing data.
     - Only update when the incoming `stage_date` is newer than the stored one.
   - Commit the transaction; the temp table is dropped automatically.

7) Export by case numbers (`src/export_cases.py`):
   - `_read_case_numbers(path)`: reads the first CSV column, removes blanks/duplicates, optionally strips a header row.
   - `_fetch_cases(dsn, case_numbers)`: selects matching rows via `WHERE case_number = ANY($1::text[])`.
   - `_write_csv(path, rows)`: writes results with a fixed 13‑column header.
   - `export_cases(input_csv, output_csv)`: async end‑to‑end export. A simple `PyQt6` GUI is provided to select input/output files and run the export.

## Data Model (`cases` table)

- Columns: `court_name, case_number (unique), case_proc, registration_date, judge, judges, participants, stage_date, stage_name, cause_result, cause_dep, type, description`.
- Unique index on `case_number` enables fast lookups.

## Concurrency, Robustness, and Error Handling

- Downloads: retry with backoff, resume partial files, handle network glitches.
- ZIP: invalid archives are skipped without crashing the pipeline.
- CSV: skip bad lines, fallback delimiter/encoding, strict column normalization.
- Import: single transaction for consistency; `COPY` is serialized; conflict handling ensures latest stage wins while avoiding duplicates.

## Entry Points

- `src/main.py` — full pipeline: metadata → download → unpack → import to DB.
- `src/export_cases.py` — export cases to CSV by a list of case numbers (GUI).

import asyncio
import csv
import importlib.util
import os

import pytest

import export_cases as ec


def test_read_case_numbers_parsing(tmp_path):
    p = tmp_path / "nums.csv"
    p.write_text("case_number\n  A-1 \n\nB-2\nB-2\nC-3\n", encoding="utf-8")
    nums = ec._read_case_numbers(str(p))
    assert nums == ["A-1", "B-2", "C-3"]


@pytest.fixture(scope="function")
def get_database_dsn(request):
    has_plugin = importlib.util.find_spec("pytest_postgresql") is not None
    if has_plugin:
        try:
            pg_conn = request.getfixturevalue("postgresql")
        except Exception as e:
            pytest.skip(f"pytest-postgresql present but failed to start: {e}")
        params = pg_conn.get_dsn_parameters()
        user = params.get("user") or "postgres"
        password = params.get("password") or ""
        host = params.get("host") or "localhost"
        port = params.get("port") or "5432"
        dbname = params.get("dbname") or "postgres"
        cred = f"{user}:{password}@" if password else f"{user}@"
        dsn = f"postgresql://{cred}{host}:{port}/{dbname}"
        return dsn
    env_dsn = os.getenv("DATABASE_URL_SYNC")
    if not env_dsn:
        pytest.skip("No database available for export tests")
    return env_dsn


def _ensure_schema_sync(dsn: str):
    import asyncpg

    async def _run():
        conn = await asyncpg.connect(
            dsn.replace("postgresql+asyncpg://", "postgresql://")
        )
        try:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cases (
                    id SERIAL PRIMARY KEY,
                    court_name text NOT NULL,
                    case_number text NOT NULL UNIQUE,
                    case_proc text NULL,
                    registration_date date NULL,
                    judge text NULL,
                    judges text NULL,
                    participants text NULL,
                    stage_date date NULL,
                    stage_name text NULL,
                    cause_result text NULL,
                    cause_dep text NULL,
                    type text NULL,
                    description text NULL
                );
                TRUNCATE TABLE cases;
                """
            )
        finally:
            await conn.close()

    asyncio.run(_run())


def _insert_rows_sync(dsn: str, rows: list[tuple]):
    import asyncpg

    async def _run():
        conn = await asyncpg.connect(
            dsn.replace("postgresql+asyncpg://", "postgresql://")
        )
        try:
            await conn.executemany(
                """
                INSERT INTO cases (
                    court_name, case_number, case_proc, registration_date,
                    judge, judges, participants, stage_date, stage_name,
                    cause_result, cause_dep, type, description
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                ON CONFLICT (case_number) DO NOTHING
                """,
                rows,
            )
        finally:
            await conn.close()

    asyncio.run(_run())


def test_export_cases_end_to_end(tmp_path, monkeypatch, get_database_dsn):
    _ensure_schema_sync(get_database_dsn)
    _insert_rows_sync(
        get_database_dsn,
        [
            (
                "Court A",
                "X-1",
                None,
                None,
                None,
                None,
                None,
                None,
                "Stage 1",
                None,
                None,
                None,
                None,
            ),
            (
                "Court B",
                "Y-2",
                None,
                None,
                None,
                None,
                None,
                None,
                "Stage 2",
                None,
                None,
                None,
                None,
            ),
        ],
    )

    input_csv = tmp_path / "cases.csv"
    input_csv.write_text("case_number\nX-1\nZ-9\nY-2\n", encoding="utf-8")
    output_csv = tmp_path / "out.csv"
    monkeypatch.setenv("DATABASE_URL_SYNC", get_database_dsn)

    count = asyncio.run(ec.export_cases(str(input_csv), str(output_csv)))
    assert count == 2

    with open(output_csv, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    got_nums = [r["case_number"] for r in rows]
    assert got_nums == ["X-1", "Y-2"]

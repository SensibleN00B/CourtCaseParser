import asyncio
import importlib.util
import os
from typing import List

import asyncpg
import pytest

import csv_to_db

TEST_DSN_ENV = "TEST_DATABASE_URL"


async def _ensure_schema(dsn: str):
    conn = await asyncpg.connect(dsn)
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
            """
        )
        await conn.execute("TRUNCATE TABLE cases;")
    finally:
        await conn.close()


@pytest.fixture(scope="function")
def db_dsn(request):
    has_pytest_pg = importlib.util.find_spec("pytest_postgresql") is not None
    if has_pytest_pg:
        try:

            pg_conn = request.getfixturevalue("postgresql")
        except Exception as e:
            pytest.skip(f"pytest-postgresql present but failed to start: {e}")
        else:
            params = pg_conn.get_dsn_parameters()
            user = params.get("user") or "postgres"
            password = params.get("password") or ""
            host = params.get("host") or "localhost"
            port = params.get("port") or "5432"
            dbname = params.get("dbname") or "postgres"
            cred = f"{user}:{password}@" if password else f"{user}@"
            dsn = f"postgresql://{cred}{host}:{port}/{dbname}"
            asyncio.run(_ensure_schema(dsn))
            return dsn
    dsn_env = (
        os.getenv(TEST_DSN_ENV)
        or os.getenv("DATABASE_URL_SYNC")
        or os.getenv("DATABASE_URL")
    )
    if not dsn_env:
        pytest.skip(
            "pytest-postgresql not installed and no TEST_DATABASE_URL/DATABASE_URL_SYNC provided"
        )
    try:
        asyncio.run(_ensure_schema(dsn_env))
    except Exception:
        pytest.skip("Could not connect to provided database URL for tests")
    return dsn_env


def _write_csv(path, rows: List[List[str]]):
    path.write_text("\n".join([";".join(r) for r in rows]), encoding="utf-8")


def test_copy_to_postgres_imports_rows(tmp_path, monkeypatch, db_dsn):
    monkeypatch.setattr(csv_to_db, "DATABASE_URL", db_dsn)

    rows = [
        [
            "court_name",
            "case_number",
            "registration_date",
            "stage_date",
            "stage_name",
        ],
        ["Court A", "CASE-1", "01.01.2020", "02.01.2020", "Stage A"],
    ]
    f1 = tmp_path / "a.csv"
    _write_csv(f1, rows)

    asyncio.run(csv_to_db.import_csv_files(str(tmp_path)))

    async def _fetch():
        conn = await asyncpg.connect(db_dsn)
        try:
            return await conn.fetchrow(
                "SELECT court_name, case_number, stage_name, registration_date, stage_date FROM cases WHERE case_number=$1",
                "CASE-1",
            )
        finally:
            await conn.close()

    rec = asyncio.run(_fetch())
    assert rec is not None
    assert rec["court_name"] == "Court A"
    assert rec["case_number"] == "CASE-1"
    assert str(rec["registration_date"]) == "2020-01-01"
    assert str(rec["stage_date"]) == "2020-01-02"
    assert rec["stage_name"] == "Stage A"


def test_on_conflict_merge_updates_latest_stage(tmp_path, monkeypatch, db_dsn):
    monkeypatch.setattr(csv_to_db, "DATABASE_URL", db_dsn)

    f1 = tmp_path / "a.csv"
    _write_csv(
        f1,
        [
            ["court_name", "case_number", "stage_date", "stage_name"],
            ["Court X", "DUP-1", "01.02.2020", "Old Stage"],
        ],
    )
    f2 = tmp_path / "b.csv"
    _write_csv(
        f2,
        [
            ["court_name", "case_number", "stage_date", "stage_name"],
            ["Court X", "DUP-1", "05.02.2020", "New Stage"],
        ],
    )

    asyncio.run(csv_to_db.import_csv_files(str(tmp_path)))

    async def _fetch():
        conn = await asyncpg.connect(db_dsn)
        try:
            return await conn.fetchrow(
                "SELECT case_number, stage_name, stage_date FROM cases WHERE case_number=$1",
                "DUP-1",
            )
        finally:
            await conn.close()

    rec = asyncio.run(_fetch())
    assert rec is not None
    assert rec["stage_name"] == "New Stage"
    assert str(rec["stage_date"]) == "2020-02-05"


def test_copy_is_sequential_with_semaphore(tmp_path, monkeypatch, db_dsn):
    monkeypatch.setattr(csv_to_db, "DATABASE_URL", db_dsn)

    for i in range(3):
        _write_csv(
            tmp_path / f"f{i}.csv",
            [["court_name", "case_number"], ["Court", f"S-{i}"]],
        )

    max_concurrent = {"value": 0}
    current = {"value": 0}

    real_connect = asyncpg.connect

    class ConnWrapper:
        def __init__(self, conn):
            self._conn = conn

        def __getattr__(self, name):
            return getattr(self._conn, name)

        async def copy_to_table(self, *args, **kwargs):
            current["value"] += 1
            if current["value"] > max_concurrent["value"]:
                max_concurrent["value"] = current["value"]
            try:
                await asyncio.sleep(0.05)
                return await self._conn.copy_to_table(*args, **kwargs)
            finally:
                current["value"] -= 1

    async def wrapped_connect(dsn, *args, **kwargs):
        conn = await real_connect(dsn, *args, **kwargs)
        return ConnWrapper(conn)

    monkeypatch.setattr(asyncpg, "connect", wrapped_connect)

    asyncio.run(csv_to_db.import_csv_files(str(tmp_path)))

    assert max_concurrent["value"] == 1

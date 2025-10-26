import asyncio
import os

import asyncpg
import pandas as pd
from dotenv import load_dotenv

from detect_encoding import detect_encoding
from utils import parse_date

EXPECTED_COLUMNS = [
    "court_name",
    "case_number",
    "case_proc",
    "registration_date",
    "judge",
    "judges",
    "participants",
    "stage_date",
    "stage_name",
    "cause_result",
    "cause_dep",
    "type",
    "description",
]

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL_SYNC")


def normalize_csv(input_path: str, output_path: str) -> int:
    enc = detect_encoding(input_path)
    try:
        df = pd.read_csv(
            input_path,
            encoding=enc,
            sep=None,
            engine="python",
            dtype=str,
            on_bad_lines="skip",
        )
    except Exception:
        df = pd.read_csv(
            input_path,
            encoding=enc,
            sep=";",
            engine="python",
            dtype=str,
            on_bad_lines="skip",
        )

    df.columns = [
        c.replace("\ufeff", "").strip().lower().replace(" ", "_").replace("-", "_")
        for c in df.columns
    ]

    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[EXPECTED_COLUMNS]

    df["case_number"] = df["case_number"].fillna("").astype(str).str.strip()
    df["case_number"] = df["case_number"].replace(
        {"nan": "", "NaN": "", "NULL": "", "null": "", "None": ""}
    )
    df["court_name"] = df["court_name"].fillna("").astype(str).str.strip()

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.replace("\xa0", "", regex=False).str.strip()

    df = df[(df["case_number"] != "") & (df["court_name"] != "")]
    df = df.drop_duplicates(subset=["case_number"], keep="first")

    df["registration_date"] = df["registration_date"].apply(parse_date)
    df["stage_date"] = df["stage_date"].apply(parse_date)

    df.to_csv(output_path, index=False, header=False)
    return len(df)


async def import_csv_files(unpacked_dir: str):
    csv_files = [
        os.path.join(unpacked_dir, f)
        for f in os.listdir(unpacked_dir)
        if f.lower().endswith(".csv")
    ]
    if not csv_files:
        print("No CSV files found")
        return

    print(f"Processing {len(csv_files)} CSV files (COPY + normalization)...")

    sem = asyncio.Semaphore(1)
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        async with conn.transaction():
            await conn.execute(
                """
                CREATE TEMP TABLE tmp_cases
                (
                    court_name        text,
                    case_number       text,
                    case_proc         text,
                    registration_date date,
                    judge             text,
                    judges            text,
                    participants      text,
                    stage_date        date,
                    stage_name        text,
                    cause_result      text,
                    cause_dep         text,
                    type              text,
                    description       text
                ) ON COMMIT DROP;
                """
            )

            async def process_file(path: str):
                clean_path = path + ".clean"
                try:
                    rows = normalize_csv(path, clean_path)
                    print(f"Normalized {os.path.basename(path)} → {rows} rows")

                    async with sem:
                        with open(clean_path, "rb") as f:
                            await conn.copy_to_table(
                                table_name="tmp_cases",
                                source=f,
                                format="csv",
                                delimiter=",",
                                null="",
                                columns=EXPECTED_COLUMNS,
                            )
                        print(f"✅ COPY to tmp_cases: {os.path.basename(path)}")
                finally:
                    if os.path.exists(clean_path):
                        os.remove(clean_path)

            await asyncio.gather(*(process_file(f) for f in csv_files))

            print("All CSV copied to tmp_cases. Merging into cases...")

            await conn.execute(
                """
                INSERT INTO cases (court_name, case_number, case_proc, registration_date,
                                   judge, judges, participants, stage_date, stage_name,
                                   cause_result, cause_dep, type, description)
                SELECT DISTINCT
                ON (case_number) court_name, case_number, case_proc, registration_date,
                    judge, judges, participants, stage_date, stage_name,
                    cause_result, cause_dep, type, description
                FROM tmp_cases
                WHERE case_number IS NOT NULL AND case_number <> ''
                ORDER BY case_number, stage_date DESC NULLS LAST
                ON CONFLICT (case_number) DO
                UPDATE
                    SET
                    court_name = COALESCE(EXCLUDED.court_name, cases.court_name),
                    case_proc = COALESCE(EXCLUDED.case_proc, cases.case_proc),
                    registration_date = COALESCE(EXCLUDED.registration_date, cases.registration_date),
                    judge = COALESCE(EXCLUDED.judge, cases.judge),
                    judges = COALESCE(EXCLUDED.judges, cases.judges),
                    participants = COALESCE(EXCLUDED.participants, cases.participants),
                    stage_date = EXCLUDED.stage_date,
                    stage_name = COALESCE(EXCLUDED.stage_name, cases.stage_name),
                    cause_result = COALESCE(EXCLUDED.cause_result, cases.cause_result),
                    cause_dep = COALESCE(EXCLUDED.cause_dep, cases.cause_dep),
                    type = COALESCE(EXCLUDED.type, cases.type),
                    description = COALESCE(EXCLUDED.description, cases.description)
                WHERE EXCLUDED.stage_date IS NOT NULL
                  AND (cases.stage_date IS NULL
                   OR EXCLUDED.stage_date
                    > cases.stage_date);
                """
            )
        print("✅ Data successfully merged into cases.")

    finally:
        await conn.close()

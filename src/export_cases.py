import argparse
import asyncio
import csv
import os
from typing import Iterable, List

import asyncpg
from dotenv import load_dotenv

COLUMNS = [
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


def _read_case_numbers(csv_path: str) -> List[str]:
    numbers: List[str] = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            val = (row[0] or "").strip()
            if not val:
                continue
            numbers.append(val)

    if numbers and numbers[0].lower() in {"case_number", "number", "case", "case_no"}:
        numbers = numbers[1:]

    seen = set()
    unique: List[str] = []
    for n in numbers:
        if n not in seen:
            seen.add(n)
            unique.append(n)
    return unique


async def _fetch_cases(dsn: str, case_numbers: Iterable[str]) -> List[dict]:
    nums = list(case_numbers)
    if not nums:
        return []
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            f"SELECT {', '.join(COLUMNS)} FROM cases WHERE case_number = ANY($1::text[])",
            nums,
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()


def _write_csv(path: str, rows: List[dict]):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k) for k in COLUMNS})


async def export_cases(input_csv: str, output_csv: str) -> int:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL_SYNC")

    numbers = _read_case_numbers(input_csv)
    rows = await _fetch_cases(database_url, numbers)
    _write_csv(output_csv, rows)
    return len(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Export cases by case numbers from CSV"
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to CSV with case numbers (one per row)",
    )
    parser.add_argument("--output", "-o", required=True, help="Path to output CSV")
    args = parser.parse_args()

    count = asyncio.run(export_cases(args.input, args.output))
    print(f"Exported {count} rows to {args.output}")


if __name__ == "__main__":
    main()

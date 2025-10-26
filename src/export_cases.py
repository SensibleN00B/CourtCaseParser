import sys
import os
import csv
import asyncio
import asyncpg

from typing import List, Iterable
from dotenv import load_dotenv
from PyQt6.QtWidgets import (
    QApplication, QWidget, QPushButton, QLabel,
    QLineEdit, QFileDialog, QVBoxLayout, QMessageBox
)

COLUMNS = [
    "court_name", "case_number", "case_proc", "registration_date",
    "judge", "judges", "participants", "stage_date", "stage_name",
    "cause_result", "cause_dep", "type", "description",
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

    unique = []
    seen = set()
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
    if not database_url:
        raise ValueError("DATABASE_URL_SYNC is not set in .env file")

    numbers = _read_case_numbers(input_csv)
    rows = await _fetch_cases(database_url, numbers)
    _write_csv(output_csv, rows)
    return len(rows)


class ExportGUI(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Court Cases Exporter (PyQt6)")
        self.setGeometry(500, 300, 400, 200)

        self.input_path = QLineEdit(self)
        self.output_path = QLineEdit(self)

        self.btn_input = QPushButton("Select Input CSV", self)
        self.btn_output = QPushButton("Select Output CSV", self)
        self.btn_export = QPushButton("Start Export", self)

        layout = QVBoxLayout()
        layout.addWidget(QLabel("Input CSV:"))
        layout.addWidget(self.input_path)
        layout.addWidget(self.btn_input)

        layout.addWidget(QLabel("Output CSV:"))
        layout.addWidget(self.output_path)
        layout.addWidget(self.btn_output)

        layout.addWidget(self.btn_export)
        self.setLayout(layout)

        self.btn_input.clicked.connect(self.load_input_file)
        self.btn_output.clicked.connect(self.save_output_file)
        self.btn_export.clicked.connect(self.start_export)

    def load_input_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Choose input CSV", "", "CSV Files (*.csv)")
        if path:
            self.input_path.setText(path)

    def save_output_file(self):
        path, _ = QFileDialog.getSaveFileName(self, "Save output CSV", "", "CSV Files (*.csv)")
        if path:
            self.output_path.setText(path)

    def show_message(self, text: str):
        QMessageBox.information(self, "Info", text)

    def start_export(self):
        in_path = self.input_path.text().strip()
        out_path = self.output_path.text().strip()

        if not in_path or not out_path:
            self.show_message("Please select both input and output paths.")
            return

        try:
            count = asyncio.run(export_cases(in_path, out_path))
            self.show_message(f"✅ Exported {count} rows to:\n{out_path}")
        except Exception as e:
            self.show_message(f"❌ Error: {str(e)}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    gui = ExportGUI()
    gui.show()
    sys.exit(app.exec())

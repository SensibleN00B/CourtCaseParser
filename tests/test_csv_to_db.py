import csv

import csv_to_db as c2d


def test_normalize_csv_basic_cleaning(tmp_path, monkeypatch):
    content = (
        "Court Name;Case Number;registration-date;Judge;Stage Date;type\n"
        "Court A;123;01.02.2020;J1;05.02.2020;T\n"
        "; ; ; ; ; \n"
        "Court\xa0B;456;31.04.2020;J2;;T2\n"
        "Court B;456;01.05.2020;J2;;T2\n"
    )

    inp = tmp_path / 'in.csv'
    outp = tmp_path / 'out.csv'
    inp.write_text(content, encoding='utf-8')

    monkeypatch.setattr(c2d, 'detect_encoding', lambda p: 'utf-8')
    rows = c2d.normalize_csv(str(inp), str(outp))

    assert rows == 2

    with outp.open('r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f)
        out_rows = list(reader)

    assert len(out_rows) == 2
    for r in out_rows:
        assert len(r) == 13
        assert r[0] != ""
        assert r[1] != ""

    r1 = out_rows[0]
    assert r1[0] == 'Court A'
    assert r1[1] == '123'
    assert r1[3] == '2020-02-01'
    assert r1[7] == '2020-02-05'

    r2 = out_rows[1]
    assert r2[0] == 'CourtB'
    assert r2[1] == '456'
    assert r2[3] == ''
    assert r2[7] == ''


def test_normalize_csv_header_only(tmp_path, monkeypatch):
    inp = tmp_path / 'in.csv'
    output = tmp_path / 'out.csv'
    content = "court_name;case_number;registration_date\n"
    inp.write_text(content, encoding='utf-8')
    monkeypatch.setattr(c2d, 'detect_encoding', lambda p: 'utf-8')
    rows = c2d.normalize_csv(str(inp), str(output))
    assert rows == 0
    assert output.read_text(encoding='utf-8').strip() == ""


def test_normalize_csv_null_case_number(tmp_path, monkeypatch):
    inp = tmp_path / 'in.csv'
    outp = tmp_path / 'out.csv'
    content = (
        "court_name;case_number;registration_date\n"
        "Court A; ;01.01.2020\n"
    )
    inp.write_text(content, encoding='utf-8')
    monkeypatch.setattr(c2d, 'detect_encoding', lambda p: 'utf-8')
    rows = c2d.normalize_csv(str(inp), str(outp))
    assert rows == 0
    assert outp.read_text(encoding='utf-8').strip() == ""

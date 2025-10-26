import pytest
from datetime import date

import utils


@pytest.mark.parametrize(
    "value,expected",
    [
        ("01.02.2020", date(2020, 2, 1)),
        (" 31.12.1999 ", date(1999, 12, 31)),
    ],
)
def test_parse_date_valid(value, expected):
    assert utils.parse_date(value) == expected


@pytest.mark.parametrize(
    "value",
    ["2020-02-01", "31/12/1999", "32.01.2020"],
)
def test_parse_date_invalid(value):
    assert utils.parse_date(value) is None


@pytest.mark.parametrize("value", ["", "   ", None, 123])
def test_parse_date_non_string_or_blank(value):
    assert utils.parse_date(value) is None


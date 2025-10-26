import fetch_metadata as fm
import pytest


class DummyResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body or {"success": True, "result": {"resources": []}}
    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")
    def json(self):
        return self._body


def test_fetch_dataset_metadata_success(monkeypatch):
    body = {"success": True, "result": {"resources": [{"name": "r1"}]}}
    monkeypatch.setattr(fm.requests, 'get', lambda **kw: DummyResponse(200, body))
    result = fm.fetch_dataset_metadata('abc')
    assert result == body["result"]


def test_fetch_dataset_metadata_unsuccessful_flag(monkeypatch):
    body = {"success": False, "result": {}}
    monkeypatch.setattr(fm.requests, 'get', lambda **kw: DummyResponse(200, body))
    with pytest.raises(RuntimeError):
        fm.fetch_dataset_metadata('abc')


def test_extract_resources_basic():
    metadata = {
        "resources": [
            {
                "name": "A",
                "description": "desc",
                "format": "CSV",
                "url": "http://example/a.csv",
                "extra": "ignored",
            }
        ]
    }
    extracted = fm.extract_resources(metadata)
    assert len(extracted) == 1
    assert extracted[0] == {"name": "A", "description": "desc", "format": "CSV", "url": "http://example/a.csv"}

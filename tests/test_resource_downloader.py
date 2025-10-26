import os
from typing import Optional

import resource_downloader


class DummyTqdm:
    def __init__(self, iterable=None, **kwargs):
        self.iterable = iterable
        self.total = kwargs.get("total")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def __iter__(self):
        if self.iterable is None:
            return iter(())
        return iter(self.iterable)

    def update(self, n):
        pass

    def reset(self, total=0):
        pass


def test_parse_total_from_content_range():
    assert (
        resource_downloader._parse_total_from_content_range("bytes 0-1023/2048") == 2048
    )
    assert resource_downloader._parse_total_from_content_range("0-1/12345") == 12345
    assert resource_downloader._parse_total_from_content_range("bytes */*") is None
    assert resource_downloader._parse_total_from_content_range("invalid") is None
    assert resource_downloader._parse_total_from_content_range("bytes 0-1/xyz") is None
    assert resource_downloader._parse_total_from_content_range(None) is None


def test_create_session_config():
    session = resource_downloader.create_session()
    assert "https://" in session.adapters
    adapter = session.adapters["https://"]
    retries = adapter.max_retries
    assert getattr(retries, "total", None) == 5
    allowed = getattr(retries, "allowed_methods", None)
    if allowed is not None:
        assert set(allowed) == {"GET", "HEAD"}


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        headers: Optional[dict] = None,
        content: bytes = b"",
    ):
        self.status_code = status_code
        self.headers = headers or {}
        self._content = content
        self.ok = status_code < 400

    def iter_content(self, block_size):
        for i in range(0, len(self._content), block_size):
            yield self._content[i : i + block_size]

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def close(self):
        pass


class FakeSession:
    def __init__(self, content: bytes):
        self._content = content

    def head(self, url, timeout=None, allow_redirects=True):
        return FakeResponse(200, headers={"content-length": str(len(self._content))})

    def get(self, url, stream=True, timeout=None, headers=None):
        return FakeResponse(
            200,
            headers={"content-length": str(len(self._content))},
            content=self._content,
        )


def test_download_file_success(tmp_path, monkeypatch):
    monkeypatch.setattr(resource_downloader, "tqdm", DummyTqdm)
    content = b"hello world" * 10
    out_dir = tmp_path / "out"

    monkeypatch.setattr(
        resource_downloader, "create_session", lambda: FakeSession(content)
    )

    path = resource_downloader.download_file(
        "https://example.com/file.bin", str(out_dir)
    )
    assert os.path.isfile(path)
    assert (out_dir / "file.bin").exists()
    assert (out_dir / "file.bin").read_bytes() == content


def test_download_all_files_aggregates_results(tmp_path, monkeypatch):
    monkeypatch.setattr(resource_downloader, "tqdm", DummyTqdm)
    resources = [
        {"name": "A", "url": "https://example.com/a.bin"},
        {"name": "B", "url": None},
        {"name": "C", "url": "https://example.com/c.bin"},
    ]

    def fake_download(url, output_dir, position=0):
        if url.endswith("/c.bin"):
            raise RuntimeError("boom")
        return os.path.join(output_dir, os.path.basename(url))

    monkeypatch.setattr(resource_downloader, "download_file", fake_download)

    out_dir = tmp_path / "dl"
    results = resource_downloader.download_all_files(
        resources, str(out_dir), max_workers=2
    )

    assert len(results) == 2
    statuses = {r["name"]: r["status"] for r in results}
    assert statuses.get("A") == "success"
    assert statuses.get("C") == "failed"

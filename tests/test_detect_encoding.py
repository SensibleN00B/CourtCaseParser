from typing import Optional

import detect_encoding


class DummyBest:
    def __init__(self, encoding: str):
        self.encoding = encoding


class DummyFromPathResult:
    def __init__(self, encoding: Optional[str]):
        self._encoding = encoding
    def best(self):
        return DummyBest(self._encoding) if self._encoding else None


def test_detect_encoding_happy_path(tmp_path, monkeypatch):
    tmp = tmp_path / 'file.txt'
    tmp.write_bytes('Тест'.encode('utf-8'))
    monkeypatch.setattr(detect_encoding, 'from_path', lambda p: DummyFromPathResult('cp1251'))
    assert detect_encoding.detect_encoding(str(tmp)) == 'cp1251'


def test_detect_encoding_fallback_on_exception(tmp_path, monkeypatch):
    tmp = tmp_path / 'file.txt'
    tmp.write_bytes(b'abc')
    def boom(_):
        raise RuntimeError('fail')
    monkeypatch.setattr(detect_encoding, 'from_path', boom)
    assert detect_encoding.detect_encoding(str(tmp)) == 'utf-8'


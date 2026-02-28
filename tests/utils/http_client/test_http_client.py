import requests
import pytest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from utils.http_client.http_client import HttpClient

class FakeResponse:
    def __init__(self, url: str, status_code: int = 200, chunks=None):
        self.url = url
        self.status_code = status_code
        self._chunks = chunks or [b"hello", b" ", b"world"]

    def iter_content(self, chunk_size=8192):
        # chunk_size is ignored here, but we accept it to match requests API
        for c in self._chunks:
            yield c


def test_download_file_writes_content(tmp_path, monkeypatch):
    base_url = "https://example.com/api"
    client = HttpClient(base_url=base_url, timeout=30)

    dest = tmp_path / "downloads" / "file.bin"
    path = "/files/file.bin"

    def fake_get(url, stream=True, timeout=None):
        assert url == "https://example.com/api/files/file.bin"
        assert stream is True
        assert timeout == 30
        return FakeResponse(url=url, status_code=200, chunks=[b"abc", b"123"])

    monkeypatch.setattr(requests, "get", fake_get)

    ok = client.download_file(path=path, destination=str(dest))
    assert ok is True
    assert dest.exists()
    assert dest.read_bytes() == b"abc123"


def test_download_file_creates_parent_dirs(tmp_path, monkeypatch):
    client = HttpClient(base_url="https://example.com", timeout=10)

    dest = tmp_path / "a" / "b" / "c" / "thing.dat"

    def fake_get(url, stream=True, timeout=None):
        return FakeResponse(url=url, status_code=200, chunks=[b"x"])

    monkeypatch.setattr(requests, "get", fake_get)

    client.download_file(path="thing.dat", destination=str(dest))
    assert dest.exists()
    assert dest.read_bytes() == b"x"


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 500])
def test_download_file_raises_http_error_on_4xx_5xx(tmp_path, monkeypatch, status_code):
    client = HttpClient(base_url="https://example.com", timeout=30)
    dest = tmp_path / "out.bin"

    def fake_get(url, stream=True, timeout=None):
        return FakeResponse(url=url, status_code=status_code)

    monkeypatch.setattr(requests, "get", fake_get)

    with pytest.raises(requests.HTTPError) as exc:
        client.download_file(path="bad.bin", destination=str(dest))

    # message includes status
    assert f"HTTP {status_code}" in str(exc.value)
    # file should not exist (or at least be empty) since we error before writing
    assert not dest.exists()


def test_download_file_strips_and_joins_url_correctly(tmp_path, monkeypatch):
    # base_url has trailing slash, path has leading slash: should still be clean
    client = HttpClient(base_url="https://example.com/", timeout=30)
    dest = tmp_path / "x.bin"

    def fake_get(url, stream=True, timeout=None):
        assert url == "https://example.com/dir/file.csv"
        return FakeResponse(url=url, status_code=200, chunks=[b"ok"])

    monkeypatch.setattr(requests, "get", fake_get)

    client.download_file(path="/dir/file.csv", destination=str(dest))
    assert dest.read_bytes() == b"ok"
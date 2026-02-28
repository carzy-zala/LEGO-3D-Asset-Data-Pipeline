import gzip
from pathlib import Path

import sys


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

import src.ingestion.rebrickable as mod  

class DummyLogger:
    def __init__(self):
        self.infos = []
        self.errors = []

    def info(self, msg):
        self.infos.append(str(msg))

    def error(self, msg):
        self.errors.append(str(msg))


class FakeHttpClient:
    """
    Fake client that "downloads" by writing a small gz file to destination.
    You can configure it per-test to fail or raise.
    """
    def __init__(self, base_url: str, behavior=None):
        self.base_url = base_url
        self.behavior = behavior or {}

    def download_file(self, path: str, destination: str) -> bool:
        action = self.behavior.get(path, "ok")

        if action == "fail":
            return False
        if action == "raise":
            raise RuntimeError("boom")

        # Write a gz containing deterministic content
        content = f"csv for {path}\n".encode("utf-8")
        dest_path = Path(destination)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(dest_path, "wb") as gz:
            gz.write(content)
        return True


def test_extract_gz_extracts_bytes(tmp_path, monkeypatch):
    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    gz_path = tmp_path / "x.csv.gz"
    out_path = tmp_path / "x.csv"

    original = b"hello,lego\n1,2,3\n"
    with gzip.open(gz_path, "wb") as f:
        f.write(original)

    mod._extract_gz(gz_path, out_path)

    assert out_path.exists()
    assert out_path.read_bytes() == original
    assert any("Extracted" in m for m in logger.infos)


def test_download_rebrickable_files_happy_path(tmp_path, monkeypatch):
    # Patch config values inside the module
    raw_dir = tmp_path / "data" / "raw" / "rebrickable"
    monkeypatch.setattr(mod, "RAW_REBRICKABLE_DIR", raw_dir)
    monkeypatch.setattr(mod, "REBRICKABLE_BASE_URL", "https://example.com")

    files = [
        ("parts.csv.gz", "parts.csv.gz"),
        ("colors.csv.gz", "colors.csv.gz"),
    ]
    monkeypatch.setattr(mod, "REBRICKABLE_FILES", files)

    # Patch logger
    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    # Patch HttpClient constructor used by the module
    def fake_client_ctor(base_url: str):
        return FakeHttpClient(base_url=base_url)

    monkeypatch.setattr(mod, "HttpClient", fake_client_ctor)

    mod.download_rebrickable_files()

    # Assert extracted csv files exist and gz files removed
    for remote_path, filename in files:
        gz_path = raw_dir / filename
        csv_path = raw_dir / filename.replace(".gz", "")
        assert csv_path.exists()
        assert not gz_path.exists()  # should be deleted after extraction

        # Content check
        assert f"csv for {remote_path}\n".encode("utf-8") == csv_path.read_bytes()

    # Logging sanity
    assert any("Starting Rebrickable ingestion" in m for m in logger.infos)
    assert any("Rebrickable ingestion complete" in m for m in logger.infos)
    assert logger.errors == []


def test_download_rebrickable_files_skips_existing_csv(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw" / "rebrickable"
    monkeypatch.setattr(mod, "RAW_REBRICKABLE_DIR", raw_dir)
    monkeypatch.setattr(mod, "REBRICKABLE_BASE_URL", "https://example.com")

    files = [("parts.csv.gz", "parts.csv.gz")]
    monkeypatch.setattr(mod, "REBRICKABLE_FILES", files)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    # Create existing extracted file to trigger skip
    raw_dir.mkdir(parents=True, exist_ok=True)
    existing_csv = raw_dir / "parts.csv"
    existing_csv.write_text("already here", encoding="utf-8")

    # HttpClient shouldn't be called meaningfully, but give it anyway
    monkeypatch.setattr(mod, "HttpClient", lambda base_url: FakeHttpClient(base_url))

    mod.download_rebrickable_files()

    assert existing_csv.read_text(encoding="utf-8") == "already here"
    assert any("Already exists — skipping" in m for m in logger.infos)


def test_download_rebrickable_files_handles_failure_and_exception(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw" / "rebrickable"
    monkeypatch.setattr(mod, "RAW_REBRICKABLE_DIR", raw_dir)
    monkeypatch.setattr(mod, "REBRICKABLE_BASE_URL", "https://example.com")

    files = [
        ("ok.csv.gz", "ok.csv.gz"),
        ("fail.csv.gz", "fail.csv.gz"),
        ("boom.csv.gz", "boom.csv.gz"),
    ]
    monkeypatch.setattr(mod, "REBRICKABLE_FILES", files)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    behavior = {
        "fail.csv.gz": "fail",
        "boom.csv.gz": "raise",
    }
    monkeypatch.setattr(mod, "HttpClient", lambda base_url: FakeHttpClient(base_url, behavior=behavior))

    mod.download_rebrickable_files()

    # ok one exists
    assert (raw_dir / "ok.csv").exists()
    # fail/boom should not produce csv
    assert not (raw_dir / "fail.csv").exists()
    assert not (raw_dir / "boom.csv").exists()

    # Should log failure and exception
    assert any("Download failed" in m for m in logger.errors)
    assert any("Unexpected error" in m for m in logger.errors)
    assert any("Failed" in m for m in logger.errors)
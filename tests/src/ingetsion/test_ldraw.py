import zipfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import src.ingestion.ldraw as mod  

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
    Fake client that "downloads" complete.zip by writing a small zip file
    containing some parts.
    """
    def __init__(self, base_url: str, zip_builder):
        self.base_url = base_url
        self._zip_builder = zip_builder

    def download_file(self, path: str, destination: str) -> bool:
        # create zip at destination
        dest = Path(destination)
        dest.parent.mkdir(parents=True, exist_ok=True)
        self._zip_builder(dest)
        return True


def _make_zip(zip_path: Path, files: dict[str, bytes]):
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, data in files.items():
            zf.writestr(name, data)


def test_skip_when_all_parts_exist(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw" / "ldraw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    parts = ["3001", "3003"]
    for p in parts:
        (raw_dir / f"{p}.dat").write_bytes(b"existing")

    zip_local = tmp_path / "complete.zip"

    monkeypatch.setattr(mod, "RAW_LDRAW_DIR", raw_dir)
    monkeypatch.setattr(mod, "PARTS_TO_PROCESS", parts)
    monkeypatch.setattr(mod, "LDRAW_ZIP_LOCAL", zip_local)
    monkeypatch.setattr(mod, "LDRAW_PARTS_ZIP_PREFIX", "parts/")

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    # If it tries to download/extract, fail the test by raising
    monkeypatch.setattr(mod, "HttpClient", lambda *a, **k: (_ for _ in ()).throw(AssertionError("Should not download")))

    mod.download_ldraw_files()

    assert any("skipping download" in m.lower() for m in logger.infos)
    assert not zip_local.exists()


def test_download_and_extract_when_zip_missing(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw" / "ldraw"
    zip_local = tmp_path / "complete.zip"
    parts = ["3001", "3003"]

    prefix = "parts/"
    zip_files = {
        f"{prefix}3001.dat": b"brick3001",
        f"{prefix}3003.dat": b"brick3003",
        # extra junk should be ignored
        f"{prefix}9999.dat": b"junk",
    }

    monkeypatch.setattr(mod, "RAW_LDRAW_DIR", raw_dir)
    monkeypatch.setattr(mod, "PARTS_TO_PROCESS", parts)
    monkeypatch.setattr(mod, "LDRAW_ZIP_LOCAL", zip_local)
    monkeypatch.setattr(mod, "LDRAW_PARTS_ZIP_PREFIX", prefix)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    def zip_builder(dest: Path):
        _make_zip(dest, zip_files)

    monkeypatch.setattr(mod, "HttpClient", lambda base_url: FakeHttpClient(base_url, zip_builder))

    mod.download_ldraw_files()

    # extracted files exist
    assert (raw_dir / "3001.dat").read_bytes() == b"brick3001"
    assert (raw_dir / "3003.dat").read_bytes() == b"brick3003"

    # zip should be removed
    assert not zip_local.exists()

    assert any("Download complete" in m for m in logger.infos)
    assert any("All parts extracted successfully" in m for m in logger.infos)
    assert logger.errors == []


def test_skip_download_when_zip_exists(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw" / "ldraw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    zip_local = tmp_path / "complete.zip"
    prefix = "parts/"
    _make_zip(zip_local, {f"{prefix}3001.dat": b"x"})

    parts = ["3001"]

    monkeypatch.setattr(mod, "RAW_LDRAW_DIR", raw_dir)
    monkeypatch.setattr(mod, "PARTS_TO_PROCESS", parts)
    monkeypatch.setattr(mod, "LDRAW_ZIP_LOCAL", zip_local)
    monkeypatch.setattr(mod, "LDRAW_PARTS_ZIP_PREFIX", prefix)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    # If it tries to download, fail
    monkeypatch.setattr(mod, "HttpClient", lambda *a, **k: (_ for _ in ()).throw(AssertionError("Should not download")))

    mod.download_ldraw_files()

    assert (raw_dir / "3001.dat").exists()
    assert not zip_local.exists()
    assert any("already exists — skipping download" in m.lower() for m in logger.infos)


def test_extract_parts_logs_missing_part(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw" / "ldraw"
    zip_local = tmp_path / "complete.zip"
    prefix = "parts/"

    _make_zip(zip_local, {f"{prefix}3001.dat": b"ok"})

    monkeypatch.setattr(mod, "RAW_LDRAW_DIR", raw_dir)
    monkeypatch.setattr(mod, "LDRAW_PARTS_ZIP_PREFIX", prefix)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    mod._extract_parts(zip_local, ["3001", "3003"])  # 3003 missing

    assert (raw_dir / "3001.dat").exists()
    assert not (raw_dir / "3003.dat").exists()
    assert any("Part not found in zip: 3003.dat" in m for m in logger.errors)


def test_extract_parts_handles_bad_zip(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw" / "ldraw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    zip_local = tmp_path / "complete.zip"
    zip_local.write_bytes(b"not a real zip at all")

    monkeypatch.setattr(mod, "RAW_LDRAW_DIR", raw_dir)
    monkeypatch.setattr(mod, "LDRAW_PARTS_ZIP_PREFIX", "parts/")

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    mod._extract_parts(zip_local, ["3001"])

    assert any("corrupted" in m.lower() for m in logger.errors)
import importlib
import io
import zipfile
from pathlib import Path

import pytest

MODULE_UNDER_TEST = "src.ingestion.ldraw"


def _make_ldraw_zip_bytes(
    *,
    parts: dict[str, bytes],
    library_parts: dict[str, bytes] | None = None,
    library_p: dict[str, bytes] | None = None,
    extra_files: dict[str, bytes] | None = None,
) -> bytes:
    library_parts = library_parts or {}
    library_p = library_p or {}
    extra_files = extra_files or {}

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for part_id, content in parts.items():
            zf.writestr(f"ldraw/parts/{part_id}.dat", content)

        for rel, content in library_parts.items():
            zf.writestr(f"ldraw/parts/{rel}", content)
        for rel, content in library_p.items():
            zf.writestr(f"ldraw/p/{rel}", content)

        for rel, content in extra_files.items():
            zf.writestr(rel, content)

    return buf.getvalue()


@pytest.fixture()
def mod(tmp_path, monkeypatch):
    m = importlib.import_module(MODULE_UNDER_TEST)

    raw_dir = tmp_path / "data" / "raw" / "ldraw"
    lib_dir = raw_dir / "library"
    zip_local = tmp_path / "complete.zip"

    monkeypatch.setattr(m, "RAW_LDRAW_DIR", raw_dir, raising=True)
    monkeypatch.setattr(m, "LDRAW_LIBRARY_DIR", lib_dir, raising=True)
    monkeypatch.setattr(m, "LDRAW_ZIP_LOCAL", zip_local, raising=True)
    monkeypatch.setattr(m, "LDRAW_PARTS_ZIP_PREFIX", "ldraw/parts/", raising=True)
    monkeypatch.setattr(m, "PARTS_TO_PROCESS", ["3001", "3002"], raising=True)

    return m


def test_skips_everything_if_parts_and_library_already_exist(mod, monkeypatch, tmp_path):
    mod.RAW_LDRAW_DIR.mkdir(parents=True, exist_ok=True)
    for part_id in mod.PARTS_TO_PROCESS:
        (mod.RAW_LDRAW_DIR / f"{part_id}.dat").write_bytes(b"already-there")

    (mod.LDRAW_LIBRARY_DIR / "parts").mkdir(parents=True, exist_ok=True)

    class ExplodingClient:
        def __init__(self, *a, **k):
            raise AssertionError("HttpClient should not be constructed when skipping")

    monkeypatch.setattr(mod, "HttpClient", ExplodingClient, raising=True)

    mod.download_ldraw_files()

    for part_id in mod.PARTS_TO_PROCESS:
        assert (mod.RAW_LDRAW_DIR / f"{part_id}.dat").read_bytes() == b"already-there"


def test_download_failure_returns_without_extracting_or_deleting_zip(mod, monkeypatch):
    assert not mod.LDRAW_ZIP_LOCAL.exists()

    class FakeClient:
        def __init__(self, base_url: str):
            self.base_url = base_url
            self.called = False

        def download_file(self, path: str, destination: str) -> bool:
            self.called = True
            return False

    monkeypatch.setattr(mod, "HttpClient", FakeClient, raising=True)

    mod.download_ldraw_files()

    assert not mod.LDRAW_ZIP_LOCAL.exists()
    for part_id in mod.PARTS_TO_PROCESS:
        assert not (mod.RAW_LDRAW_DIR / f"{part_id}.dat").exists()
    assert not (mod.LDRAW_LIBRARY_DIR / "parts").exists()


def test_uses_existing_zip_extracts_parts_and_library_then_deletes_zip(mod):
    zip_bytes = _make_ldraw_zip_bytes(
        parts={"3001": b"part-3001", "3002": b"part-3002"},
        library_parts={
            "subpart1.dat": b"subpart1",
            "some/dir/subpart2.dat": b"subpart2",
        },
        library_p={
            "8/primitive.dat": b"prim",
        },
        extra_files={
            "docs/readme.txt": b"nope",
            "ldraw/models/model.ldr": b"also-nope",
        },
    )
    mod.LDRAW_ZIP_LOCAL.parent.mkdir(parents=True, exist_ok=True)
    mod.LDRAW_ZIP_LOCAL.write_bytes(zip_bytes)
    assert mod.LDRAW_ZIP_LOCAL.exists()

    mod.download_ldraw_files()

    assert (mod.RAW_LDRAW_DIR / "3001.dat").read_bytes() == b"part-3001"
    assert (mod.RAW_LDRAW_DIR / "3002.dat").read_bytes() == b"part-3002"

    assert (mod.LDRAW_LIBRARY_DIR / "parts" / "subpart1.dat").read_bytes() == b"subpart1"
    assert (mod.LDRAW_LIBRARY_DIR / "parts" / "some" / "dir" / "subpart2.dat").read_bytes() == b"subpart2"
    assert (mod.LDRAW_LIBRARY_DIR / "p" / "8" / "primitive.dat").read_bytes() == b"prim"

    assert not (mod.LDRAW_LIBRARY_DIR / "docs" / "readme.txt").exists()
    assert not (mod.LDRAW_LIBRARY_DIR / "models" / "model.ldr").exists()

    assert not mod.LDRAW_ZIP_LOCAL.exists()


def test_extract_skips_existing_part_and_logs_missing_part_without_crashing(mod):
    zip_bytes = _make_ldraw_zip_bytes(parts={"3001": b"part-3001"})
    mod.LDRAW_ZIP_LOCAL.write_bytes(zip_bytes)

    mod.RAW_LDRAW_DIR.mkdir(parents=True, exist_ok=True)
    (mod.RAW_LDRAW_DIR / "3001.dat").write_bytes(b"preexisting")

    mod._extract_parts_and_library(mod.LDRAW_ZIP_LOCAL, ["3001", "3002"])

    assert (mod.RAW_LDRAW_DIR / "3001.dat").read_bytes() == b"preexisting"
    assert not (mod.RAW_LDRAW_DIR / "3002.dat").exists()


def test_extract_library_does_not_overwrite_existing_library_files(mod):
    zip_bytes = _make_ldraw_zip_bytes(
        parts={"3001": b"part-3001"},
        library_parts={"keep.dat": b"from-zip"},
    )
    mod.LDRAW_ZIP_LOCAL.write_bytes(zip_bytes)

    existing_path = mod.LDRAW_LIBRARY_DIR / "parts" / "keep.dat"
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_bytes(b"already-here")

    mod._extract_parts_and_library(mod.LDRAW_ZIP_LOCAL, ["3001"])

    assert existing_path.read_bytes() == b"already-here"
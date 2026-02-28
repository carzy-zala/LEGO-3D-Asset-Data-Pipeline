import importlib
import json

import pandas as pd
import pytest

MODULE_UNDER_TEST = "src.transformation.bronze.ldraw.src.DS2B_ldraw_job"


@pytest.fixture()
def mod(tmp_path, monkeypatch):
    m = importlib.import_module(MODULE_UNDER_TEST)

    raw_dir = tmp_path / "data" / "raw" / "ldraw"
    bronze_dir = tmp_path / "data" / "processed" / "bronze" / "geometry"
    library_parts_dir = tmp_path / "data" / "raw" / "ldraw" / "library" / "parts"
    raw_dir.mkdir(parents=True, exist_ok=True)
    library_parts_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(m, "RAW_LDRAW_DIR", raw_dir, raising=True)
    monkeypatch.setattr(m, "BRONZE_GEOMETRY_DIR", bronze_dir, raising=True)
    monkeypatch.setattr(m, "LDRAW_LIBRARY_PARTS_DIR", library_parts_dir, raising=True)
    monkeypatch.setattr(m, "PARTS_TO_PROCESS", ["3001", "3002"], raising=True)

    cfg_path = tmp_path / "DS2B_ldraw_config.json"
    cfg = {
        "geometry_line_types": [2, 3, 4, 5],
        "output": {
            "description_file": "geometry_description.csv",
            "coordinates_file": "geometry_coordinates.csv",
        },
        "description_columns": ["part_id", "name"],
        "coordinates_columns": ["part_id", "x", "y", "z"],
    }
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(m, "CONFIG_PATH", cfg_path, raising=True)

    return m


def test_writes_both_csvs_when_parses_succeed(mod, monkeypatch):
    (mod.RAW_LDRAW_DIR / "3001.dat").write_text("dummy", encoding="utf-8")
    (mod.RAW_LDRAW_DIR / "3002.dat").write_text("dummy", encoding="utf-8")

    calls = []

    def fake_parse_dat_file(dat_path, geometry_line_types, ldraw_parts_dir):
        calls.append((dat_path, tuple(geometry_line_types), ldraw_parts_dir))
        part_id = dat_path.stem
        description = [part_id, f"name-{part_id}"]
        coords = [
            [part_id, 1.0, 2.0, 3.0],
            [part_id, 4.0, 5.0, 6.0],
        ]
        return description, coords

    monkeypatch.setattr(mod, "parse_dat_file", fake_parse_dat_file, raising=True)

    mod.run_ldraw_bronze()

    assert len(calls) == 2
    for dat_path, geom_types, parts_dir in calls:
        assert dat_path.parent == mod.RAW_LDRAW_DIR
        assert geom_types == (2, 3, 4, 5)
        assert parts_dir == mod.LDRAW_LIBRARY_PARTS_DIR

    desc_path = mod.BRONZE_GEOMETRY_DIR / "geometry_description.csv"
    coord_path = mod.BRONZE_GEOMETRY_DIR / "geometry_coordinates.csv"
    assert desc_path.exists()
    assert coord_path.exists()

    desc_df = pd.read_csv(desc_path, dtype={"part_id": "string"})
    coord_df = pd.read_csv(coord_path, dtype={"part_id": "string"})

    assert list(desc_df.columns) == ["part_id", "name"]
    assert len(desc_df) == 2

    assert list(coord_df.columns) == ["part_id", "x", "y", "z"]
    assert len(coord_df) == 4


def test_missing_dat_marks_failed_and_still_writes_for_successes(mod, monkeypatch):
    (mod.RAW_LDRAW_DIR / "3001.dat").write_text("dummy", encoding="utf-8")

    def fake_parse_dat_file(dat_path, geometry_line_types, ldraw_parts_dir):
        part_id = dat_path.stem
        return [part_id, f"name-{part_id}"], [[part_id, 0.0, 0.0, 0.0]]

    monkeypatch.setattr(mod, "parse_dat_file", fake_parse_dat_file, raising=True)

    mod.run_ldraw_bronze()

    desc_path = mod.BRONZE_GEOMETRY_DIR / "geometry_description.csv"
    coord_path = mod.BRONZE_GEOMETRY_DIR / "geometry_coordinates.csv"
    assert desc_path.exists()
    assert coord_path.exists()

    desc_df = pd.read_csv(desc_path, dtype={"part_id": "string"})
    coord_df = pd.read_csv(coord_path, dtype={"part_id": "string"})

    assert len(desc_df) == 1
    assert desc_df.iloc[0]["part_id"] == "3001"
    assert len(coord_df) == 1
    assert coord_df.iloc[0]["part_id"] == "3001"


def test_parse_exception_marks_failed_and_continues(mod, monkeypatch):
    (mod.RAW_LDRAW_DIR / "3001.dat").write_text("dummy", encoding="utf-8")
    (mod.RAW_LDRAW_DIR / "3002.dat").write_text("dummy", encoding="utf-8")

    def fake_parse_dat_file(dat_path, geometry_line_types, ldraw_parts_dir):
        if dat_path.stem == "3002":
            raise ValueError("boom")
        return ["3001", "name-3001"], [["3001", 1.0, 1.0, 1.0]]

    monkeypatch.setattr(mod, "parse_dat_file", fake_parse_dat_file, raising=True)

    mod.run_ldraw_bronze()

    desc_path = mod.BRONZE_GEOMETRY_DIR / "geometry_description.csv"
    coord_path = mod.BRONZE_GEOMETRY_DIR / "geometry_coordinates.csv"
    assert desc_path.exists()
    assert coord_path.exists()

    desc_df = pd.read_csv(desc_path, dtype={"part_id": "string"})
    coord_df = pd.read_csv(coord_path, dtype={"part_id": "string"})

    assert len(desc_df) == 1
    assert desc_df.iloc[0]["part_id"] == "3001"
    assert len(coord_df) == 1
    assert coord_df.iloc[0]["part_id"] == "3001"


def test_does_not_write_csvs_when_nothing_parsed(mod, monkeypatch):
    def exploding_parse(*a, **k):
        raise AssertionError("parse_dat_file should not be called when dat missing")

    monkeypatch.setattr(mod, "parse_dat_file", exploding_parse, raising=True)

    mod.run_ldraw_bronze()

    assert mod.BRONZE_GEOMETRY_DIR.exists()
    assert not (mod.BRONZE_GEOMETRY_DIR / "geometry_description.csv").exists()
    assert not (mod.BRONZE_GEOMETRY_DIR / "geometry_coordinates.csv").exists()
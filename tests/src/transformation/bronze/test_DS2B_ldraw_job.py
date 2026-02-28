import json
from pathlib import Path

import pandas as pd


import src.transformation.bronze.ldraw.src.DS2B_ldraw_job as mod  

class DummyLogger:
    def __init__(self):
        self.infos = []
        self.errors = []

    def info(self, msg):
        self.infos.append(str(msg))

    def error(self, msg):
        self.errors.append(str(msg))


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj), encoding="utf-8")


def _touch(path: Path, content: str = "0 test\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_run_ldraw_bronze_happy_path_writes_both_csvs(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw_ldraw"
    out_dir = tmp_path / "bronze_geometry"
    raw_dir.mkdir()
    out_dir.mkdir()

    parts = ["3001", "3003"]
    for p in parts:
        _touch(raw_dir / f"{p}.dat")

    cfg = {
        "geometry_line_types": [2, 3, 4],
        "output": {
            "description_file": "geometry_description.csv",
            "coordinates_file": "geometry_coordinates.csv",
        },
        "description_columns": ["part_id", "part_name", "author", "license"],
        "coordinates_columns": [
            "part_id", "line_type", "colour",
            "x1", "y1", "z1", "x2", "y2", "z2",
            "x3", "y3", "z3", "x4", "y4", "z4"
        ],
    }
    cfg_path = tmp_path / "config" / "DS2B_ldraw_config.json"
    _write_json(cfg_path, cfg)

    monkeypatch.setattr(mod, "RAW_LDRAW_DIR", raw_dir)
    monkeypatch.setattr(mod, "BRONZE_GEOMETRY_DIR", out_dir)
    monkeypatch.setattr(mod, "PARTS_TO_PROCESS", parts)
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    def fake_parse_dat_file(dat_path: Path, geometry_line_types):
        part_id = dat_path.stem
        description = {
            "part_id": part_id,
            "part_name": f"name-{part_id}",
            "author": "author",
            "license": "license",
        }
        coords = [
            {
                "part_id": part_id,
                "line_type": 2,
                "colour": "16",
                "x1": 0.0, "y1": 0.0, "z1": 0.0,
                "x2": 1.0, "y2": 1.0, "z2": 1.0,
                "x3": None, "y3": None, "z3": None,
                "x4": None, "y4": None, "z4": None,
            }
        ]
        return description, coords

    monkeypatch.setattr(mod, "parse_dat_file", fake_parse_dat_file)

    mod.run_ldraw_bronze()

    desc_csv = out_dir / "geometry_description.csv"
    coord_csv = out_dir / "geometry_coordinates.csv"

    assert desc_csv.exists()
    assert coord_csv.exists()

    desc_df = pd.read_csv(desc_csv, dtype=str)
    coord_df = pd.read_csv(coord_csv,dtype=str)

    assert len(desc_df) == 2
    assert set(desc_df["part_id"]) == {"3001", "3003"}

    assert len(coord_df) == 2
    assert set(coord_df["part_id"]) == {"3001", "3003"}

    assert any("LDraw Bronze complete" in m for m in logger.infos)
    assert logger.errors == []


def test_run_ldraw_bronze_missing_dat_is_logged_and_skipped(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw_ldraw"
    out_dir = tmp_path / "bronze_geometry"
    raw_dir.mkdir()
    out_dir.mkdir()

    parts = ["3001", "9999"]  # 9999 missing
    _touch(raw_dir / "3001.dat")

    cfg = {
        "geometry_line_types": [2],
        "output": {
            "description_file": "geometry_description.csv",
            "coordinates_file": "geometry_coordinates.csv",
        },
        "description_columns": ["part_id", "part_name", "author", "license"],
        "coordinates_columns": [
            "part_id", "line_type", "colour",
            "x1", "y1", "z1", "x2", "y2", "z2",
            "x3", "y3", "z3", "x4", "y4", "z4"
        ],
    }
    cfg_path = tmp_path / "config" / "DS2B_ldraw_config.json"
    _write_json(cfg_path, cfg)

    monkeypatch.setattr(mod, "RAW_LDRAW_DIR", raw_dir)
    monkeypatch.setattr(mod, "BRONZE_GEOMETRY_DIR", out_dir)
    monkeypatch.setattr(mod, "PARTS_TO_PROCESS", parts)
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    def fake_parse_dat_file(dat_path: Path, geometry_line_types):
        part_id = dat_path.stem
        return (
            {"part_id": part_id, "part_name": "n", "author": None, "license": None},
            []
        )

    monkeypatch.setattr(mod, "parse_dat_file", fake_parse_dat_file)

    mod.run_ldraw_bronze()

    desc_csv = out_dir / "geometry_description.csv"
    coord_csv = out_dir / "geometry_coordinates.csv"

    assert desc_csv.exists()
    desc_df = pd.read_csv(desc_csv, dtype=str)
    assert list(desc_df["part_id"]) == ["3001"]

    # no coords => coordinates file should not exist
    assert not coord_csv.exists()

    assert any("Missing .dat file" in m for m in logger.errors)
    assert any("Failed" in m for m in logger.errors)  # summary failed list


def test_run_ldraw_bronze_parse_failure_is_logged_and_skipped(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw_ldraw"
    out_dir = tmp_path / "bronze_geometry"
    raw_dir.mkdir()
    out_dir.mkdir()

    parts = ["3001", "3003"]
    for p in parts:
        _touch(raw_dir / f"{p}.dat")

    cfg = {
        "geometry_line_types": [2],
        "output": {
            "description_file": "geometry_description.csv",
            "coordinates_file": "geometry_coordinates.csv",
        },
        "description_columns": ["part_id", "part_name", "author", "license"],
        "coordinates_columns": [
            "part_id", "line_type", "colour",
            "x1", "y1", "z1", "x2", "y2", "z2",
            "x3", "y3", "z3", "x4", "y4", "z4"
        ],
    }
    cfg_path = tmp_path / "config" / "DS2B_ldraw_config.json"
    _write_json(cfg_path, cfg)

    monkeypatch.setattr(mod, "RAW_LDRAW_DIR", raw_dir)
    monkeypatch.setattr(mod, "BRONZE_GEOMETRY_DIR", out_dir)
    monkeypatch.setattr(mod, "PARTS_TO_PROCESS", parts)
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    def fake_parse_dat_file(dat_path: Path, geometry_line_types):
        if dat_path.stem == "3003":
            raise RuntimeError("parse exploded")
        return (
            {"part_id": "3001", "part_name": "n", "author": None, "license": None},
            []
        )

    monkeypatch.setattr(mod, "parse_dat_file", fake_parse_dat_file)

    mod.run_ldraw_bronze()

    desc_csv = out_dir / "geometry_description.csv"
    assert desc_csv.exists()
    desc_df = pd.read_csv(desc_csv, dtype=str)
    assert list(desc_df["part_id"]) == ["3001"]

    assert any("Failed to parse 3003.dat" in m for m in logger.errors)
    assert any("Failed" in m for m in logger.errors)
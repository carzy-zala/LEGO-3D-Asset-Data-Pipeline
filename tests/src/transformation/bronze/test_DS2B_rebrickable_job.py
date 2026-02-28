import json
from pathlib import Path

import pandas as pd

import src.transformation.bronze.rebrickable.src.DS2B_rebrickable_job as mod 

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


def _write_csv(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_run_rebrickable_bronze_happy_path(tmp_path, monkeypatch):
    
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    raw_dir.mkdir()
    bronze_dir.mkdir()

    _write_csv(raw_dir / "parts.csv", "part_num,name\n3001,Brick\n3003,Plate\n")
    _write_csv(raw_dir / "colors.csv", "id,name\n1,Red\n2,Blue\n")

    cfg = {
        "files": [
            {
                "source_file": "parts.csv",
                "output_file": "parts_out.csv",
                "keep_columns": ["part_num", "name"],
                "filter_by_parts": True,
            },
            {
                "source_file": "colors.csv",
                "output_file": "colors_out.csv",
                "keep_columns": ["id", "name"],
                "filter_by_parts": False,
            },
        ]
    }
    cfg_path = tmp_path / "config" / "DS2B_rebrickable_config.json"
    _write_json(cfg_path, cfg)

    monkeypatch.setattr(mod, "RAW_REBRICKABLE_DIR", raw_dir)
    monkeypatch.setattr(mod, "BRONZE_CATALOGUE_DIR", bronze_dir)
    monkeypatch.setattr(mod, "PARTS_TO_PROCESS", ["3001"])  # filter to one part
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    def fake_filter_csv(source_path, keep_columns, filter_by_parts, parts_to_process):
        df = pd.read_csv(source_path, dtype=str)[keep_columns]
        if filter_by_parts and "part_num" in df.columns:
            df = df[df["part_num"].isin(parts_to_process)]
        return df.reset_index(drop=True)

    monkeypatch.setattr(mod, "filter_csv", fake_filter_csv)

    mod.run_rebrickable_bronze()

    out_parts = bronze_dir / "parts_out.csv"
    out_colors = bronze_dir / "colors_out.csv"
    assert out_parts.exists()
    assert out_colors.exists()

    parts_df = pd.read_csv(out_parts, dtype=str)
    assert list(parts_df["part_num"]) == ["3001"]

    colors_df = pd.read_csv(out_colors, dtype=str)
    assert len(colors_df) == 2

    assert any("Rebrickable Bronze complete" in m for m in logger.infos)
    assert logger.errors == []


def test_run_rebrickable_bronze_missing_source_file(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    raw_dir.mkdir()
    bronze_dir.mkdir()

    cfg = {
        "files": [
            {
                "source_file": "missing.csv",
                "output_file": "out.csv",
                "keep_columns": ["a"],
                "filter_by_parts": False,
            }
        ]
    }
    cfg_path = tmp_path / "config" / "DS2B_rebrickable_config.json"
    _write_json(cfg_path, cfg)

    monkeypatch.setattr(mod, "RAW_REBRICKABLE_DIR", raw_dir)
    monkeypatch.setattr(mod, "BRONZE_CATALOGUE_DIR", bronze_dir)
    monkeypatch.setattr(mod, "PARTS_TO_PROCESS", [])
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    mod.run_rebrickable_bronze()

    assert any("Source file not found" in m for m in logger.errors)
    assert not (bronze_dir / "out.csv").exists()


def test_run_rebrickable_bronze_schema_error_valueerror(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    raw_dir.mkdir()
    bronze_dir.mkdir()

    _write_csv(raw_dir / "parts.csv", "part_num,name\n3001,Brick\n")

    cfg = {
        "files": [
            {
                "source_file": "parts.csv",
                "output_file": "out.csv",
                "keep_columns": ["missing_col"],
                "filter_by_parts": False,
            }
        ]
    }
    cfg_path = tmp_path / "config" / "DS2B_rebrickable_config.json"
    _write_json(cfg_path, cfg)

    monkeypatch.setattr(mod, "RAW_REBRICKABLE_DIR", raw_dir)
    monkeypatch.setattr(mod, "BRONZE_CATALOGUE_DIR", bronze_dir)
    monkeypatch.setattr(mod, "PARTS_TO_PROCESS", [])
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    def boom_filter_csv(*args, **kwargs):
        raise ValueError("Columns not found")

    monkeypatch.setattr(mod, "filter_csv", boom_filter_csv)

   
    mod.run_rebrickable_bronze()

   
    assert any("Schema error" in m for m in logger.errors)
    assert not (bronze_dir / "out.csv").exists()


def test_run_rebrickable_bronze_unexpected_exception(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    raw_dir.mkdir()
    bronze_dir.mkdir()

    _write_csv(raw_dir / "parts.csv", "part_num,name\n3001,Brick\n")

    cfg = {
        "files": [
            {
                "source_file": "parts.csv",
                "output_file": "out.csv",
                "keep_columns": ["part_num", "name"],
                "filter_by_parts": False,
            }
        ]
    }
    cfg_path = tmp_path / "config" / "DS2B_rebrickable_config.json"
    _write_json(cfg_path, cfg)

    monkeypatch.setattr(mod, "RAW_REBRICKABLE_DIR", raw_dir)
    monkeypatch.setattr(mod, "BRONZE_CATALOGUE_DIR", bronze_dir)
    monkeypatch.setattr(mod, "PARTS_TO_PROCESS", [])
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    def boom_filter_csv(*args, **kwargs):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(mod, "filter_csv", boom_filter_csv)

    mod.run_rebrickable_bronze()

    assert any("Unexpected error processing" in m for m in logger.errors)
    assert not (bronze_dir / "out.csv").exists()
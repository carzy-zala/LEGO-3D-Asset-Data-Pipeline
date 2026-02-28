import json
from pathlib import Path

import pandas as pd

import src.transformation.silver.ldraw.src.B2S_ldraw_job as mod  # <-- update


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


def _write_df_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _base_config():
    return {
        "input": {
            "description_file": "geometry_description.csv",
            "coordinates_file": "geometry_coordinates.csv",
        },
        "output": {
            "description_file": "geometry_description.csv",
            "coordinates_file": "geometry_coordinates.csv",
        },
        "description_validation": {"required_columns": ["part_id"]},
        "coordinates_validation": {"required_columns": ["part_id", "line_type"]},
    }


def test_run_ldraw_silver_happy_path_writes_outputs(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze_geometry"
    silver_root = tmp_path / "silver"
    silver_dir = silver_root / "ldraw"
    cfg_path = tmp_path / "config" / "B2S_ldraw_config.json"

    cfg = _base_config()
    _write_json(cfg_path, cfg)

    desc_in = pd.DataFrame(
        {"part_id": ["3001"], "part_name": ["Brick"], "author": ["a"], "license": ["l"]}
    )
    coord_in = pd.DataFrame(
        {"part_id": ["3001"], "line_type": ["2"], "colour": ["16"], "x1": ["0"], "y1": ["0"], "z1": ["0"]}
    )

    _write_df_csv(bronze_dir / cfg["input"]["description_file"], desc_in)
    _write_df_csv(bronze_dir / cfg["input"]["coordinates_file"], coord_in)

    monkeypatch.setattr(mod, "BRONZE_GEOMETRY_DIR", bronze_dir)
    monkeypatch.setattr(mod, "SILVER_DIR", silver_root)
    monkeypatch.setattr(mod, "SILVER_LDRAW_DIR", silver_dir)
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    def fake_validate_description(df, description_validation):
        out = df.copy()
        out["is_valid"] = True
        out["issues"] = ""
        return out

    def fake_validate_coordinates(df, coordinates_validation):
        out = df.copy()
        out["is_valid"] = True
        out["issues"] = ""
        return out

    monkeypatch.setattr(mod, "validate_description", fake_validate_description)
    monkeypatch.setattr(mod, "validate_coordinates", fake_validate_coordinates)

    mod.run_ldraw_silver()

    desc_out = silver_dir / cfg["output"]["description_file"]
    coord_out = silver_dir / cfg["output"]["coordinates_file"]
    assert desc_out.exists()
    assert coord_out.exists()

    out_desc = pd.read_csv(desc_out, dtype=str)
    out_coord = pd.read_csv(coord_out, dtype=str)

    assert "is_valid" in out_desc.columns
    assert "issues" in out_desc.columns
    assert "is_valid" in out_coord.columns
    assert "issues" in out_coord.columns

    assert any("LDraw Silver complete" in m for m in logger.infos)
    assert logger.errors == []


def test_run_ldraw_silver_logs_invalid_rows(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze_geometry"
    silver_root = tmp_path / "silver"
    silver_dir = silver_root / "ldraw"
    cfg_path = tmp_path / "config" / "B2S_ldraw_config.json"

    cfg = _base_config()
    _write_json(cfg_path, cfg)

    desc_in = pd.DataFrame({"part_id": ["3001", "3003"]})
    coord_in = pd.DataFrame({"part_id": ["3001", "3001"], "line_type": ["2", "3"]})

    _write_df_csv(bronze_dir / cfg["input"]["description_file"], desc_in)
    _write_df_csv(bronze_dir / cfg["input"]["coordinates_file"], coord_in)

    monkeypatch.setattr(mod, "BRONZE_GEOMETRY_DIR", bronze_dir)
    monkeypatch.setattr(mod, "SILVER_DIR", silver_root)
    monkeypatch.setattr(mod, "SILVER_LDRAW_DIR", silver_dir)
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)
    def fake_validate_description(df, description_validation):
        out = df.copy()
        out["is_valid"] = [True, False]
        out["issues"] = ["", "missing author"]
        return out

    def fake_validate_coordinates(df, coordinates_validation):
        out = df.copy()
        out["is_valid"] = [False, True]
        out["issues"] = ["bad coords", ""]
        return out

    monkeypatch.setattr(mod, "validate_description", fake_validate_description)
    monkeypatch.setattr(mod, "validate_coordinates", fake_validate_coordinates)

    mod.run_ldraw_silver()

    assert any("Invalid description rows" in m for m in logger.errors)
    assert any("part_id=3003" in m for m in logger.errors)

    assert any("Invalid coordinate rows" in m for m in logger.errors)
    assert any("line_type=2" in m for m in logger.errors)

    assert (silver_dir / cfg["output"]["description_file"]).exists()
    assert (silver_dir / cfg["output"]["coordinates_file"]).exists()


def test_run_ldraw_silver_missing_bronze_file_returns_early(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze_geometry"
    silver_root = tmp_path / "silver"
    silver_dir = silver_root / "ldraw"
    cfg_path = tmp_path / "config" / "B2S_ldraw_config.json"

    cfg = _base_config()
    _write_json(cfg_path, cfg)

    _write_df_csv(bronze_dir / cfg["input"]["description_file"], pd.DataFrame({"part_id": ["3001"]}))
    

    monkeypatch.setattr(mod, "BRONZE_GEOMETRY_DIR", bronze_dir)
    monkeypatch.setattr(mod, "SILVER_DIR", silver_root)
    monkeypatch.setattr(mod, "SILVER_LDRAW_DIR", silver_dir)
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    monkeypatch.setattr(mod, "validate_description", lambda *a, **k: (_ for _ in ()).throw(AssertionError("Should not validate")))
    monkeypatch.setattr(mod, "validate_coordinates", lambda *a, **k: (_ for _ in ()).throw(AssertionError("Should not validate")))

    mod.run_ldraw_silver()

    assert any("Bronze file not found" in m for m in logger.errors)
    assert not (silver_dir / cfg["output"]["description_file"]).exists()
    assert not (silver_dir / cfg["output"]["coordinates_file"]).exists()
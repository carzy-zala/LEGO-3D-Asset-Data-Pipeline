import json
from pathlib import Path

import pandas as pd

import src.transformation.gold.src.S2G_gold_job as mod  

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
            "ldraw_description_file": "geometry_description.csv",
            "ldraw_coordinates_file": "geometry_coordinates.csv",
            "rebrickable_file": "rebrickable_catalogue.csv",
        },
        "coordinate_fields": ["x1", "y1", "z1"],
        "output": {
            "manifest_file": "manifest.json",
        },
    }


def test_run_gold_happy_path_writes_part_jsons_and_manifest(tmp_path, monkeypatch):
    
    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    silver_ldraw = silver_root / "ldraw"
    silver_reb = silver_root / "rebrickable"
    gold_parts = gold_root / "parts"
    cfg_path = tmp_path / "config" / "S2G_gold_config.json"

    cfg = _base_config()
    _write_json(cfg_path, cfg)
    _write_df_csv(silver_ldraw / cfg["input"]["ldraw_description_file"],
                  pd.DataFrame({"part_id": ["3001", "3003"], "part_name": ["A", "B"]}))
    _write_df_csv(silver_ldraw / cfg["input"]["ldraw_coordinates_file"],
                  pd.DataFrame({"part_id": ["3001", "3003"], "line_type": ["3", "2"], "is_valid": ["True", "True"]}))
    _write_df_csv(silver_reb / cfg["input"]["rebrickable_file"],
                  pd.DataFrame({"part_num": ["3001", "3003"], "category_name": ["Bricks", "Bricks"]}))

    monkeypatch.setattr(mod, "SILVER_DIR", silver_root)
    monkeypatch.setattr(mod, "GOLD_DIR", gold_root)
    monkeypatch.setattr(mod, "SILVER_LDRAW_DIR", silver_ldraw)
    monkeypatch.setattr(mod, "SILVER_REB_DIR", silver_reb)
    monkeypatch.setattr(mod, "GOLD_PARTS_DIR", gold_parts)
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    monkeypatch.setattr(mod, "PARTS_TO_PROCESS", ["3001", "3003"])
    monkeypatch.setattr(mod, "PIPELINE_VERSION", "9.9.9")

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    def fake_build_part_json(part_id, desc_df, coord_df, rebrickable_df, coordinate_fields, pipeline_version):
        return {
            "part_id": part_id,
            "part_name": f"name-{part_id}",
            "category": "Bricks",
            "geometry": {"stats": {"total_faces": 7}},
            "pipeline_metadata": {"is_valid": True, "quality_issues": []},
        }

    monkeypatch.setattr(mod, "build_part_json", fake_build_part_json)

    mod.run_gold()

    p1 = gold_parts / "3001.json"
    p3 = gold_parts / "3003.json"
    assert p1.exists()
    assert p3.exists()

    j1 = json.loads(p1.read_text(encoding="utf-8"))
    assert j1["part_id"] == "3001"
    assert j1["pipeline_metadata"]["is_valid"] is True

    manifest_path = gold_root / cfg["output"]["manifest_file"]
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["pipeline_version"] == "9.9.9"
    assert manifest["total_parts"] == 2
    assert manifest["valid_parts"] == 2
    assert manifest["invalid_parts"] == 0
    assert manifest["failed_parts"] == []
    assert len(manifest["parts"]) == 2

    assert any("Gold transformation complete" in m for m in logger.infos)
    assert logger.errors == []


def test_run_gold_handles_missing_silver_files_returns_early(tmp_path, monkeypatch):
    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    silver_ldraw = silver_root / "ldraw"
    silver_reb = silver_root / "rebrickable"
    gold_parts = gold_root / "parts"
    cfg_path = tmp_path / "config" / "S2G_gold_config.json"

    cfg = _base_config()
    _write_json(cfg_path, cfg)

    _write_df_csv(silver_ldraw / cfg["input"]["ldraw_description_file"],
                  pd.DataFrame({"part_id": ["3001"]}))

    monkeypatch.setattr(mod, "SILVER_DIR", silver_root)
    monkeypatch.setattr(mod, "GOLD_DIR", gold_root)
    monkeypatch.setattr(mod, "SILVER_LDRAW_DIR", silver_ldraw)
    monkeypatch.setattr(mod, "SILVER_REB_DIR", silver_reb)
    monkeypatch.setattr(mod, "GOLD_PARTS_DIR", gold_parts)
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)
    monkeypatch.setattr(mod, "PARTS_TO_PROCESS", ["3001"])
    monkeypatch.setattr(mod, "PIPELINE_VERSION", "1.0.0")

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    monkeypatch.setattr(mod, "build_part_json", lambda *a, **k: (_ for _ in ()).throw(AssertionError("Should not build")))

    mod.run_gold()

    assert any("Silver file not found" in m for m in logger.errors)
    assert not (gold_root / cfg["output"]["manifest_file"]).exists()
    assert not (gold_parts / "3001.json").exists()


def test_run_gold_marks_failed_part_when_build_raises(tmp_path, monkeypatch):
    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    silver_ldraw = silver_root / "ldraw"
    silver_reb = silver_root / "rebrickable"
    gold_parts = gold_root / "parts"
    cfg_path = tmp_path / "config" / "S2G_gold_config.json"

    cfg = _base_config()
    _write_json(cfg_path, cfg)

    _write_df_csv(silver_ldraw / cfg["input"]["ldraw_description_file"],
                  pd.DataFrame({"part_id": ["3001", "3003"]}))
    _write_df_csv(silver_ldraw / cfg["input"]["ldraw_coordinates_file"],
                  pd.DataFrame({"part_id": ["3001", "3003"], "line_type": ["3", "3"], "is_valid": ["True", "True"]}))
    _write_df_csv(silver_reb / cfg["input"]["rebrickable_file"],
                  pd.DataFrame({"part_num": ["3001", "3003"]}))

    monkeypatch.setattr(mod, "SILVER_DIR", silver_root)
    monkeypatch.setattr(mod, "GOLD_DIR", gold_root)
    monkeypatch.setattr(mod, "SILVER_LDRAW_DIR", silver_ldraw)
    monkeypatch.setattr(mod, "SILVER_REB_DIR", silver_reb)
    monkeypatch.setattr(mod, "GOLD_PARTS_DIR", gold_parts)
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)
    monkeypatch.setattr(mod, "PARTS_TO_PROCESS", ["3001", "3003"])
    monkeypatch.setattr(mod, "PIPELINE_VERSION", "2.0.0")

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    def fake_build_part_json(part_id, **kwargs):
        if part_id == "3003":
            raise RuntimeError("boom")
        return {
            "part_id": part_id,
            "part_name": f"name-{part_id}",
            "category": "Bricks",
            "geometry": {"stats": {"total_faces": 1}},
            "pipeline_metadata": {"is_valid": False, "quality_issues": ["some_issue"]},
        }

    monkeypatch.setattr(mod, "build_part_json", fake_build_part_json)

    mod.run_gold()

    assert (gold_parts / "3001.json").exists()
    assert not (gold_parts / "3003.json").exists()

    manifest_path = gold_root / cfg["output"]["manifest_file"]
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["total_parts"] == 1  # only successfully built parts included
    assert manifest["failed_parts"] == ["3003"]

    assert any("Failed to build Gold JSON for 3003" in m for m in logger.errors)
import json
from pathlib import Path

import pandas as pd

import src.transformation.silver.rebrickable.src.B2S_rebrickable_job as mod 

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
            "parts_file": "parts.csv",
            "colors_file": "colors.csv",
            "part_categories_file": "part_categories.csv",
            "inventory_parts_file": "inventory_parts.csv",
        },
        "joins": [{"left": "inventory_parts", "right": "parts", "left_on": "part_num", "right_on": "part_num", "how": "left"}],
        "output_columns": ["part_num", "name"],
        "validation_rules": {"required_columns": ["part_num"], "not_null_columns": [], "allowed_materials": []},
        "output_file": "rebrickable_catalogue.csv",
    }


def test_run_rebrickable_silver_happy_path_writes_output(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze_catalogue"
    silver_root = tmp_path / "silver"
    silver_dir = silver_root / "rebrickable"
    cfg_path = tmp_path / "config" / "B2S_rebrickable_config.json"

    cfg = _base_config()
    _write_json(cfg_path, cfg)

   
    _write_df_csv(bronze_dir / cfg["input"]["parts_file"],
                  pd.DataFrame({"part_num": ["3001"], "name": ["Brick"]}))
    _write_df_csv(bronze_dir / cfg["input"]["colors_file"],
                  pd.DataFrame({"color_id": ["1"], "color_name": ["Red"]}))
    _write_df_csv(bronze_dir / cfg["input"]["part_categories_file"],
                  pd.DataFrame({"id": ["1"], "category_name": ["Bricks"]}))
    _write_df_csv(bronze_dir / cfg["input"]["inventory_parts_file"],
                  pd.DataFrame({"part_num": ["3001"], "color_id": ["1"], "qty": ["2"]}))

   
    monkeypatch.setattr(mod, "BRONZE_CATALOGUE_DIR", bronze_dir)
    monkeypatch.setattr(mod, "SILVER_DIR", silver_root)
    monkeypatch.setattr(mod, "SILVER_REBRICKABLE_DIR", silver_dir)
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)


    def fake_merge_catalogues(**kwargs):
       
        assert len(kwargs["parts_df"]) == 1
        assert len(kwargs["inventory_parts_df"]) == 1
        return pd.DataFrame({"part_num": ["3001"], "name": ["Brick"]})

    def fake_validate(df, validation_rules):
        return df, [] 

    monkeypatch.setattr(mod, "merge_catalogues", fake_merge_catalogues)
    monkeypatch.setattr(mod, "validate", fake_validate)

    mod.run_rebrickable_silver()

    out_path = silver_dir / cfg["output_file"]
    assert out_path.exists()

    out_df = pd.read_csv(out_path, dtype=str)
    assert list(out_df.columns) == ["part_num", "name"]
    assert len(out_df) == 1

    assert any("Rebrickable Silver complete" in m for m in logger.infos)
    assert logger.errors == []


def test_run_rebrickable_silver_logs_validation_issues(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze_catalogue"
    silver_root = tmp_path / "silver"
    silver_dir = silver_root / "rebrickable"
    cfg_path = tmp_path / "config" / "B2S_rebrickable_config.json"

    cfg = _base_config()
    _write_json(cfg_path, cfg)

    _write_df_csv(bronze_dir / cfg["input"]["parts_file"],
                  pd.DataFrame({"part_num": ["3001"], "name": ["Brick"]}))
    _write_df_csv(bronze_dir / cfg["input"]["colors_file"],
                  pd.DataFrame({"color_id": ["1"], "color_name": ["Red"]}))
    _write_df_csv(bronze_dir / cfg["input"]["part_categories_file"],
                  pd.DataFrame({"id": ["1"], "category_name": ["Bricks"]}))
    _write_df_csv(bronze_dir / cfg["input"]["inventory_parts_file"],
                  pd.DataFrame({"part_num": ["3001"], "color_id": ["1"], "qty": ["2"]}))

    monkeypatch.setattr(mod, "BRONZE_CATALOGUE_DIR", bronze_dir)
    monkeypatch.setattr(mod, "SILVER_DIR", silver_root)
    monkeypatch.setattr(mod, "SILVER_REBRICKABLE_DIR", silver_dir)
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    monkeypatch.setattr(mod, "merge_catalogues", lambda **kwargs: pd.DataFrame({"part_num": ["3001"], "name": ["Brick"]}))

    issues = [
        {"type": "null_values", "detail": "1 null values found in 'part_num'"},
        {"type": "invalid_material", "detail": "Unexpected material values found: ['WOOD']"},
    ]
    monkeypatch.setattr(mod, "validate", lambda df, validation_rules: (df, issues))

    mod.run_rebrickable_silver()

    out_path = silver_dir / cfg["output_file"]
    assert out_path.exists()
    assert any("Validation issue" in m for m in logger.errors)
    assert any("Issues found" in m for m in logger.infos)


def test_run_rebrickable_silver_missing_bronze_file_returns_early(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze_catalogue"
    silver_root = tmp_path / "silver"
    silver_dir = silver_root / "rebrickable"
    cfg_path = tmp_path / "config" / "B2S_rebrickable_config.json"

    cfg = _base_config()
    _write_json(cfg_path, cfg)

    _write_df_csv(bronze_dir / cfg["input"]["parts_file"],
                  pd.DataFrame({"part_num": ["3001"], "name": ["Brick"]}))

    monkeypatch.setattr(mod, "BRONZE_CATALOGUE_DIR", bronze_dir)
    monkeypatch.setattr(mod, "SILVER_DIR", silver_root)
    monkeypatch.setattr(mod, "SILVER_REBRICKABLE_DIR", silver_dir)
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    monkeypatch.setattr(mod, "merge_catalogues", lambda **kwargs: (_ for _ in ()).throw(AssertionError("Should not merge")))
    monkeypatch.setattr(mod, "validate", lambda df, validation_rules: (_ for _ in ()).throw(AssertionError("Should not validate")))

    mod.run_rebrickable_silver()

    assert any("Bronze file not found" in m for m in logger.errors)
    assert not (silver_dir / cfg["output_file"]).exists()


def test_run_rebrickable_silver_merge_failure_returns_early(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze_catalogue"
    silver_root = tmp_path / "silver"
    silver_dir = silver_root / "rebrickable"
    cfg_path = tmp_path / "config" / "B2S_rebrickable_config.json"

    cfg = _base_config()
    _write_json(cfg_path, cfg)

    _write_df_csv(bronze_dir / cfg["input"]["parts_file"],
                  pd.DataFrame({"part_num": ["3001"], "name": ["Brick"]}))
    _write_df_csv(bronze_dir / cfg["input"]["colors_file"],
                  pd.DataFrame({"color_id": ["1"], "color_name": ["Red"]}))
    _write_df_csv(bronze_dir / cfg["input"]["part_categories_file"],
                  pd.DataFrame({"id": ["1"], "category_name": ["Bricks"]}))
    _write_df_csv(bronze_dir / cfg["input"]["inventory_parts_file"],
                  pd.DataFrame({"part_num": ["3001"], "color_id": ["1"], "qty": ["2"]}))

    monkeypatch.setattr(mod, "BRONZE_CATALOGUE_DIR", bronze_dir)
    monkeypatch.setattr(mod, "SILVER_DIR", silver_root)
    monkeypatch.setattr(mod, "SILVER_REBRICKABLE_DIR", silver_dir)
    monkeypatch.setattr(mod, "CONFIG_PATH", cfg_path)

    logger = DummyLogger()
    monkeypatch.setattr(mod, "logger", logger)

    def boom_merge(**kwargs):
        raise RuntimeError("merge exploded")

    monkeypatch.setattr(mod, "merge_catalogues", boom_merge)
    monkeypatch.setattr(mod, "validate", lambda df, validation_rules: (_ for _ in ()).throw(AssertionError("Should not validate")))

    mod.run_rebrickable_silver()

    assert any("Merge failed" in m for m in logger.errors)
    assert not (silver_dir / cfg["output_file"]).exists()
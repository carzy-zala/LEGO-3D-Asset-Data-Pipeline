import pandas as pd
import pytest
from pathlib import Path

from src.transformation.bronze.rebrickable.src.DS2B_rebrickable import filter_csv   

def _write_csv(tmp_path: Path, name: str, content: str) -> Path:
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return path


def test_keeps_only_requested_columns(tmp_path):
    csv = _write_csv(
        tmp_path,
        "parts.csv",
        "part_num,name,color\n3001,Brick,Red\n3003,Plate,Blue\n"
    )

    df = filter_csv(
        source_path=csv,
        keep_columns=["part_num", "name"],
        filter_by_parts=False,
        parts_to_process=[]
    )

    assert list(df.columns) == ["part_num", "name"]
    assert len(df) == 2


def test_raises_when_missing_columns(tmp_path):
    csv = _write_csv(
        tmp_path,
        "parts.csv",
        "part_num,name\n3001,Brick\n"
    )

    with pytest.raises(ValueError) as exc:
        filter_csv(
            source_path=csv,
            keep_columns=["part_num", "missing"],
            filter_by_parts=False,
            parts_to_process=[]
        )

    assert "missing" in str(exc.value)
    assert "Available columns" in str(exc.value)


def test_filters_by_part_num(tmp_path):
    csv = _write_csv(
        tmp_path,
        "parts.csv",
        "part_num,name\n3001,Brick\n3003,Plate\n3004,Slope\n"
    )

    df = filter_csv(
        source_path=csv,
        keep_columns=["part_num", "name"],
        filter_by_parts=True,
        parts_to_process=["3001", "3004"]
    )

    assert list(df["part_num"]) == ["3001", "3004"]
    assert len(df) == 2


def test_does_not_filter_when_flag_false(tmp_path):
    csv = _write_csv(
        tmp_path,
        "parts.csv",
        "part_num,name\n3001,Brick\n3003,Plate\n"
    )

    df = filter_csv(
        source_path=csv,
        keep_columns=["part_num", "name"],
        filter_by_parts=False,
        parts_to_process=["3001"]
    )

    # Should keep all rows
    assert len(df) == 2


def test_strips_whitespace(tmp_path):
    csv = _write_csv(
        tmp_path,
        "parts.csv",
        "part_num,name\n 3001 , Brick \n"
    )

    df = filter_csv(
        source_path=csv,
        keep_columns=["part_num", "name"],
        filter_by_parts=False,
        parts_to_process=[]
    )

    assert df.loc[0, "part_num"] == "3001"
    assert df.loc[0, "name"] == "Brick"


def test_index_reset_after_filter(tmp_path):
    csv = _write_csv(
        tmp_path,
        "parts.csv",
        "part_num,name\n3001,Brick\n3003,Plate\n3004,Slope\n"
    )

    df = filter_csv(
        source_path=csv,
        keep_columns=["part_num", "name"],
        filter_by_parts=True,
        parts_to_process=["3004"]
    )

    # index should be 0, not original row index
    assert list(df.index) == [0]
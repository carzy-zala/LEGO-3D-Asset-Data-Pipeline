import pandas as pd
import pytest

from src.transformation.silver.ldraw.src.B2S_ldraw import validate_description, validate_coordinates  

def test_validate_description_adds_columns_and_flags_nulls():
    df = pd.DataFrame(
        {
            "part_id": ["3001", "3003"],
            "part_name": ["Brick", None],
            "author": ["A", None],
        }
    )

    rules = {"not_null_columns": ["part_name", "author"]}

    out = validate_description(df=df, description_validation=rules)

    assert "is_valid" in out.columns
    assert "issues" in out.columns

    # row 0 has no nulls
    assert bool(out.loc[0, "is_valid"]) is True
    assert out.loc[0, "issues"] == ""

    # row 1 missing part_name and author
    assert bool(out.loc[1, "is_valid"]) is False
    assert out.loc[1, "issues"] == "missing_part_name,missing_author"


def test_validate_description_ignores_missing_columns_in_rules():
    df = pd.DataFrame({"part_id": ["3001"]})
    rules = {"not_null_columns": ["author"]}  # author not present

    out = validate_description(df=df, description_validation=rules)

    assert bool(out.loc[0, "is_valid"]) is True
    assert out.loc[0, "issues"] == ""


def test_validate_coordinates_flags_invalid_line_type_parse():
    df = pd.DataFrame(
        {
            "part_id": ["3001", "3003"],
            "line_type": ["x", None],
        }
    )

    rules = {
        "valid_line_types": [2, 3, 4],
        "vertex_requirements": {},
        "numeric_columns": [],
    }

    out = validate_coordinates(df=df, coordinates_validation=rules)

    assert bool(out.loc[0, "is_valid"]) is False
    assert out.loc[0, "issues"] == "invalid_line_type"

    assert bool(out.loc[1, "is_valid"]) is False
    assert out.loc[1, "issues"] == "invalid_line_type"


def test_validate_coordinates_flags_unexpected_line_type():
    df = pd.DataFrame(
        {
            "part_id": ["3001"],
            "line_type": ["5"],  # parseable but not allowed
            "x1": ["0"],
            "y1": ["0"],
        }
    )

    rules = {
        "valid_line_types": [2, 3, 4],
        "vertex_requirements": {"5": ["x1"]},
        "numeric_columns": ["x1", "y1"],
    }

    out = validate_coordinates(df=df, coordinates_validation=rules)

    assert bool(out.loc[0, "is_valid"]) is False
    assert out.loc[0, "issues"].startswith("unexpected_line_type_5")


def test_validate_coordinates_flags_missing_vertices():
    df = pd.DataFrame(
        {
            "part_id": ["3001"],
            "line_type": ["2"],
            "x1": ["0"],
            "y1": ["0"],
            "z1": ["0"],
            "x2": [None],  # missing required vertex
            "y2": ["1"],
            "z2": ["1"],
        }
    )

    rules = {
        "valid_line_types": [2, 3, 4],
        "vertex_requirements": {"2": ["x1", "y1", "z1", "x2", "y2", "z2"]},
        "numeric_columns": ["x1", "y1", "z1", "x2", "y2", "z2"],
    }

    out = validate_coordinates(df=df, coordinates_validation=rules)

    assert bool(out.loc[0, "is_valid"]) is False
    assert "missing_vertex_x2" in out.loc[0, "issues"]


def test_validate_coordinates_flags_non_numeric_for_required_vertices_only():
    df = pd.DataFrame(
        {
            "part_id": ["3001"],
            "line_type": ["2"],
            "x1": ["0"],
            "y1": ["NOPE"],  # required + non-numeric => should flag
            "z1": ["0"],
            "x2": ["1"],
            "y2": ["1"],
            "z2": ["1"],
            "x3": ["BAD"],   # exists but not required for type 2
        }
    )

    rules = {
        "valid_line_types": [2, 3, 4],
        "vertex_requirements": {"2": ["x1", "y1", "z1", "x2", "y2", "z2"]},
        "numeric_columns": ["x1", "y1", "z1", "x2", "y2", "z2", "x3"],
    }

    out = validate_coordinates(df=df, coordinates_validation=rules)

    assert bool(out.loc[0, "is_valid"]) is False
    issues = out.loc[0, "issues"].split(",")

    assert "non_numeric_y1" in issues
    assert "non_numeric_x3" not in issues


def test_validate_coordinates_valid_row_has_no_issues():
    df = pd.DataFrame(
        {
            "part_id": ["3001"],
            "line_type": ["3"],
            "x1": ["0"],
            "y1": ["0"],
            "z1": ["0"],
            "x2": ["1"],
            "y2": ["0"],
            "z2": ["0"],
            "x3": ["0"],
            "y3": ["1"],
            "z3": ["0"],
        }
    )

    rules = {
        "valid_line_types": [2, 3, 4],
        "vertex_requirements": {
            "3": ["x1", "y1", "z1", "x2", "y2", "z2", "x3", "y3", "z3"]
        },
        "numeric_columns": ["x1", "y1", "z1", "x2", "y2", "z2", "x3", "y3", "z3"],
    }

    out = validate_coordinates(df=df, coordinates_validation=rules)

    assert bool(out.loc[0, "is_valid"]) is True
    assert out.loc[0, "issues"] == ""
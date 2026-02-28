import pandas as pd
import pytest

from src.transformation.silver.rebrickable.src.B2S_rebrickable import merge_catalogues, validate  

def _dfs():
    parts_df = pd.DataFrame(
        {
            "part_num": ["3001", "3003"],
            "name": ["Brick 2x4", "Brick 2x2"],
            "part_cat_id": ["1", "2"],
            "part_material": ["ABS", "WOOD"], 
        }
    )

    colors_df = pd.DataFrame(
        {"color_id": ["1", "2"], "color_name": ["Red", "Blue"]}
    )

    part_categories_df = pd.DataFrame(
        {"id": ["1", "2"], "category_name": ["Bricks", "Plates"]}
    )

    inventory_parts_df = pd.DataFrame(
        {
            "part_num": ["3001", "3003", "3001"],
            "color_id": ["1", "2", "2"],
            "qty": [2, 1, 5],
            "extra_col": ["dropme", "dropme", "dropme"],
        }
    )

    return parts_df, colors_df, part_categories_df, inventory_parts_df


def test_merge_catalogues_multi_join_rename_drop_and_output_columns():
    parts_df, colors_df, part_categories_df, inventory_parts_df = _dfs()

    joins = [
        {
            "left": "inventory_parts",
            "right": "parts",
            "left_on": "part_num",
            "right_on": "part_num",
            "how": "left",
        },
        {
            "left": "result",
            "right": "colors",
            "left_on": "color_id",
            "right_on": "color_id",
            "how": "left",
            "rename": {"color_name": "colour_name"},
        },
        {
            "left": "result",
            "right": "part_categories",
            "left_on": "part_cat_id",
            "right_on": "id",
            "how": "left",
            "drop_columns": ["id", "extra_col"],
        },
    ]

    output_columns = [
        "part_num",
        "name",
        "qty",
        "colour_name",
        "category_name",
        "part_material",
        "does_not_exist",  
    ]

    out = merge_catalogues(
        parts_df=parts_df,
        colors_df=colors_df,
        part_categories_df=part_categories_df,
        inventory_parts_df=inventory_parts_df,
        joins=joins,
        output_columns=output_columns,
    )


    assert list(out.columns) == [
        "part_num",
        "name",
        "qty",
        "colour_name",
        "category_name",
        "part_material",
    ]

    assert len(out) == 3

    assert "colour_name" in out.columns
    assert "color_name" not in out.columns
    assert "extra_col" not in out.columns
    assert "id" not in out.columns
    assert list(out.index) == [0, 1, 2]


def test_merge_catalogues_handles_missing_output_columns_gracefully():
    parts_df, colors_df, part_categories_df, inventory_parts_df = _dfs()

    joins = [
        {
            "left": "inventory_parts",
            "right": "parts",
            "left_on": "part_num",
            "right_on": "part_num",
            "how": "left",
        }
    ]

    out = merge_catalogues(
        parts_df=parts_df,
        colors_df=colors_df,
        part_categories_df=part_categories_df,
        inventory_parts_df=inventory_parts_df,
        joins=joins,
        output_columns=["nope1", "nope2"],
    )

    assert out.shape[0] == 3 
    assert out.shape[1] == 0 


def test_validate_reports_missing_column_nulls_and_invalid_material():
    
    df = pd.DataFrame(
        {
            "part_num": ["3001", None],             
            "part_material": ["ABS", "WOOD"],       
            "category_name": ["Bricks", "Bricks"],
        }
    )

    rules = {
        "required_columns": ["part_num", "category_name", "missing_col"],
        "not_null_columns": ["part_num"],
        "allowed_materials": ["ABS", "PETG"],
    }

    validated, issues = validate(df=df, validation_rules=rules)

    assert validated.shape == df.shape
    issue_types = {i["type"] for i in issues}
    assert "missing_column" in issue_types
    assert "null_values" in issue_types
    assert "invalid_material" in issue_types

    # More specific checks
    missing_cols = [i for i in issues if i["type"] == "missing_column"]
    assert any(i["column"] == "missing_col" for i in missing_cols)

    null_issues = [i for i in issues if i["type"] == "null_values"]
    assert any(i["column"] == "part_num" and "1 null values" in i["detail"] for i in null_issues)

    mat_issues = [i for i in issues if i["type"] == "invalid_material"]
    assert mat_issues[0]["column"] == "part_material"
    assert "WOOD" in mat_issues[0]["detail"]


def test_validate_no_issues_when_rules_satisfied():
    df = pd.DataFrame(
        {
            "part_num": ["3001", "3003"],
            "part_material": ["ABS", "PETG"],
        }
    )

    rules = {
        "required_columns": ["part_num", "part_material"],
        "not_null_columns": ["part_num", "part_material"],
        "allowed_materials": ["ABS", "PETG"],
    }

    _, issues = validate(df=df, validation_rules=rules)
    assert issues == []
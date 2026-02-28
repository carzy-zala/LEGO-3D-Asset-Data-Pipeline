import pandas as pd
from typing import Dict, List, Tuple


def merge_catalogues(
    parts_df           : pd.DataFrame,
    colors_df          : pd.DataFrame,
    part_categories_df : pd.DataFrame,
    inventory_parts_df : pd.DataFrame,
    joins              : List[Dict],
    output_columns     : List[str]
) -> pd.DataFrame:
    """
    Merges all 4 Rebrickable bronze tables into a single enriched catalogue.

    Args:
        parts_df           : Bronze parts dataframe
        colors_df          : Bronze colors dataframe
        part_categories_df : Bronze part_categories dataframe
        inventory_parts_df : Bronze inventory_parts dataframe
        joins              : Join definitions from config
        output_columns     : Final columns to keep in output

    Returns:
        Single merged and cleaned DataFrame
    """

    # ── Map name → dataframe for dynamic join resolution ──────────────────
    tables = {
        "parts"           : parts_df,
        "colors"          : colors_df,
        "part_categories" : part_categories_df,
        "inventory_parts" : inventory_parts_df
    }

    result = None

    for join_cfg in joins:

        # ── Resolve left and right tables ──────────────────────────────────
        left_df  = result if join_cfg["left"] == "result" else tables[join_cfg["left"]]
        right_df = tables[join_cfg["right"]]

        # ── Perform join ───────────────────────────────────────────────────
        result = pd.merge(
            left_df,
            right_df,
            left_on  = join_cfg["left_on"],
            right_on = join_cfg["right_on"],
            how      = join_cfg["how"]
        )

        # ── Rename columns if defined ──────────────────────────────────────
        if join_cfg.get("rename"):
            result = result.rename(columns=join_cfg["rename"])

        # ── Drop unwanted columns ──────────────────────────────────────────
        cols_to_drop = [c for c in join_cfg.get("drop_columns", []) if c in result.columns]
        if cols_to_drop:
            result = result.drop(columns=cols_to_drop)

    # ── Keep only output columns that exist ───────────────────────────────
    available = [c for c in output_columns if c in result.columns]
    result    = result[available]

    # ── Reset index ────────────────────────────────────────────────────────
    result = result.reset_index(drop=True)

    return result


def validate(
    df               : pd.DataFrame,
    validation_rules : Dict
) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Validates the merged catalogue DataFrame.
    Flags rows with issues but does NOT drop them.

    Args:
        df               : Merged catalogue DataFrame
        validation_rules : Validation rules from config

    Returns:
        Tuple of (validated_df, list_of_issues)
    """

    issues = []

    # ── Check required columns exist ──────────────────────────────────────
    for col in validation_rules.get("required_columns", []):
        if col not in df.columns:
            issues.append({
                "type"   : "missing_column",
                "column" : col,
                "detail" : f"Required column '{col}' not found in output"
            })

    # ── Check not-null columns ─────────────────────────────────────────────
    for col in validation_rules.get("not_null_columns", []):
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                issues.append({
                    "type"   : "null_values",
                    "column" : col,
                    "detail" : f"{null_count} null values found in '{col}'"
                })

    # ── Check allowed materials ────────────────────────────────────────────
    allowed_materials = validation_rules.get("allowed_materials", [])
    if allowed_materials and "part_material" in df.columns:
        invalid = df[~df["part_material"].isin(allowed_materials)]["part_material"].unique()
        if len(invalid) > 0:
            issues.append({
                "type"   : "invalid_material",
                "column" : "part_material",
                "detail" : f"Unexpected material values found: {list(invalid)}"
            })

    return df, issues
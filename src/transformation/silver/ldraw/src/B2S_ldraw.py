import pandas as pd
from typing import Dict, Tuple


def validate_description(
    df                    : pd.DataFrame,
    description_validation: Dict
) -> pd.DataFrame:
    """
    Validates geometry_description bronze CSV.
    Adds two columns:
        is_valid : True/False
        issues   : comma separated list of issues found, empty if none

    Args:
        df                     : Bronze geometry_description DataFrame
        description_validation : Validation rules from config

    Returns:
        DataFrame with is_valid and issues columns added
    """

    df = df.copy()
    df["is_valid"] = True
    df["issues"]   = ""

    for _, row in df.iterrows():
        row_issues = []

     
        for col in description_validation.get("not_null_columns", []):
            if col in df.columns and pd.isnull(row[col]):
                row_issues.append(f"missing_{col}")

        if row_issues:
            df.at[row.name, "is_valid"] = False
            df.at[row.name, "issues"]   = ",".join(row_issues)

    return df


def validate_coordinates(
    df                       : pd.DataFrame,
    coordinates_validation   : Dict
) -> pd.DataFrame:
    """
    Validates geometry_coordinates bronze CSV.
    Adds two columns:
        is_valid : True/False
        issues   : comma separated list of issues found, empty if none

    Checks:
        - line_type is in valid_line_types
        - required vertices for each line_type are not null
        - coordinate values are numeric

    Args:
        df                     : Bronze geometry_coordinates DataFrame
        coordinates_validation : Validation rules from config

    Returns:
        DataFrame with is_valid and issues columns added
    """

    df = df.copy()
    df["is_valid"] = True
    df["issues"]   = ""

    valid_line_types    = coordinates_validation.get("valid_line_types", [])
    vertex_requirements = coordinates_validation.get("vertex_requirements", {})
    numeric_columns     = coordinates_validation.get("numeric_columns", [])

    for idx, row in df.iterrows():
        row_issues = []

        try:
            line_type = int(row["line_type"])
        except (ValueError, TypeError):
            df.at[idx, "is_valid"] = False
            df.at[idx, "issues"]   = "invalid_line_type"
            continue

        if line_type not in valid_line_types:
            row_issues.append(f"unexpected_line_type_{line_type}")

        required_vertices = vertex_requirements.get(str(line_type), [])
        for col in required_vertices:
            if col in df.columns and pd.isnull(row[col]):
                row_issues.append(f"missing_vertex_{col}")

        for col in numeric_columns:
            val = row.get(col)
            if col in required_vertices and not pd.isnull(val):
                try:
                    float(val)
                except (ValueError, TypeError):
                    row_issues.append(f"non_numeric_{col}")

        if row_issues:
            df.at[idx, "is_valid"] = False
            df.at[idx, "issues"]   = ",".join(row_issues)

    return df
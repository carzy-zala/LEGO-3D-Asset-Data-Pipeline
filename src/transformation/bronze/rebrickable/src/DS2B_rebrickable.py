import pandas as pd
from pathlib import Path
from typing import List


def filter_csv(
    source_path: Path,
    keep_columns: List[str],
    filter_by_parts: bool,
    parts_to_process: List[str]
) -> pd.DataFrame:
    """
    Reads a raw Rebrickable CSV, keeps only required columns,
    and optionally filters rows to only the parts we care about.

    Args:
        source_path      : Path to the raw CSV file
        keep_columns     : List of columns to keep
        filter_by_parts  : Whether to filter rows by part_num
        parts_to_process : List of part IDs from config

    Returns:
        Filtered and cleaned DataFrame
    """

    df = pd.read_csv(source_path, dtype=str)

    missing_cols = [col for col in keep_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Columns not found in {source_path.name}: {missing_cols}\n"
            f"Available columns: {list(df.columns)}"
        )

    df = df[keep_columns]

    if filter_by_parts and "part_num" in df.columns:
        df = df[df["part_num"].isin(parts_to_process)]

    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].str.strip()

    return df.reset_index(drop=True)
import json
import pandas as pd
from pathlib import Path

from config import (
    BRONZE_CATALOGUE_DIR,
    SILVER_DIR
)
from utils.logger.logger import get_logger
from src.transformation.silver.rebrickable.src.B2S_rebrickable import merge_catalogues, validate

logger = get_logger("silver")

# Config and output paths
CONFIG_PATH      = Path(__file__).parent.parent / "config" / "B2S_rebrickable_config.json"
SILVER_REBRICKABLE_DIR = SILVER_DIR / "rebrickable"


def run_rebrickable_silver() -> None:
    """
    Job that reads all 4 bronze Rebrickable CSVs, merges and validates them,
    and writes a single enriched catalogue CSV to silver/rebrickable/.

    Output:
        data/processed/silver/rebrickable/rebrickable_catalogue.csv
    """

    logger.info("=" * 60)
    logger.info("Starting Rebrickable Silver transformation")
    logger.info(f"Source      : {BRONZE_CATALOGUE_DIR}")
    logger.info(f"Destination : {SILVER_REBRICKABLE_DIR}")
    logger.info("=" * 60)

    # ── Create output directory ────────────────────────────────────────────
    SILVER_REBRICKABLE_DIR.mkdir(parents=True, exist_ok=True)

    # ── Load config ────────────────────────────────────────────────────────
    logger.info(f"Loading config → {CONFIG_PATH.name}")
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    # ── Load all bronze CSVs ───────────────────────────────────────────────
    try:
        logger.info("Loading bronze CSVs...")

        parts_df           = pd.read_csv(BRONZE_CATALOGUE_DIR / config["input"]["parts_file"],           dtype=str)
        colors_df          = pd.read_csv(BRONZE_CATALOGUE_DIR / config["input"]["colors_file"],          dtype=str)
        part_categories_df = pd.read_csv(BRONZE_CATALOGUE_DIR / config["input"]["part_categories_file"],dtype=str)
        inventory_parts_df = pd.read_csv(BRONZE_CATALOGUE_DIR / config["input"]["inventory_parts_file"],dtype=str)

        logger.info(f"parts           : {len(parts_df)} rows")
        logger.info(f"colors          : {len(colors_df)} rows")
        logger.info(f"part_categories : {len(part_categories_df)} rows")
        logger.info(f"inventory_parts : {len(inventory_parts_df)} rows")

    except FileNotFoundError as e:
        logger.error(f"Bronze file not found → {e}")
        return

    # ── Merge ──────────────────────────────────────────────────────────────
    logger.info("Merging catalogues...")
    try:
        merged_df = merge_catalogues(
            parts_df           = parts_df,
            colors_df          = colors_df,
            part_categories_df = part_categories_df,
            inventory_parts_df = inventory_parts_df,
            joins              = config["joins"],
            output_columns     = config["output_columns"]
        )
        logger.info(f"Merged → {len(merged_df)} rows, {len(merged_df.columns)} columns")

    except Exception as e:
        logger.error(f"Merge failed → {e}")
        return

    # ── Validate ───────────────────────────────────────────────────────────
    logger.info("Validating merged catalogue...")
    merged_df, issues = validate(
        df               = merged_df,
        validation_rules = config["validation_rules"]
    )

    if issues:
        for issue in issues:
            logger.error(f"Validation issue → [{issue['type']}] {issue['detail']}")
    else:
        logger.info("Validation passed — no issues found")

    # ── Write output ───────────────────────────────────────────────────────
    output_path = SILVER_REBRICKABLE_DIR / config["output_file"]
    merged_df.to_csv(output_path, index=False)
    logger.info(f"Written → {config['output_file']} ({len(merged_df)} rows)")

    # ── Summary ────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Rebrickable Silver complete")
    logger.info(f"Output rows    : {len(merged_df)}")
    logger.info(f"Output columns : {list(merged_df.columns)}")
    logger.info(f"Issues found   : {len(issues)}")
    logger.info("=" * 60)
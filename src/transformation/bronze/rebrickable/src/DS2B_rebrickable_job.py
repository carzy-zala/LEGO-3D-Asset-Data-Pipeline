import json
from pathlib import Path

from config import (
    RAW_REBRICKABLE_DIR,
    BRONZE_CATALOGUE_DIR,
    PARTS_TO_PROCESS
)
from utils.logger.logger import get_logger
from src.transformation.bronze.rebrickable.src.DS2B_rebrickable import filter_csv

logger = get_logger("bronze")

CONFIG_PATH = Path(__file__).parent.parent / "config" / "DS2B_rebrickable_config.json"

def run_rebrickable_bronze() -> None:
    """
    Job that iterates through each Rebrickable CSV defined in config,
    applies filtering and column selection, and writes output to bronze/catalogue/.

    Output:
        data/processed/bronze/catalogue/parts.csv
        data/processed/bronze/catalogue/colors.csv
        data/processed/bronze/catalogue/part_categories.csv
        data/processed/bronze/catalogue/inventory_parts.csv
    """

    logger.info("=" * 60)
    logger.info("Starting Rebrickable Bronze transformation")
    logger.info(f"Source      : {RAW_REBRICKABLE_DIR}")
    logger.info(f"Destination : {BRONZE_CATALOGUE_DIR}")
    logger.info("=" * 60)

    BRONZE_CATALOGUE_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(f"Loading config → {CONFIG_PATH.name}")
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    success_count = 0
    failed_files  = []

    for file_cfg in config["files"]:

        source_file      = file_cfg["source_file"]
        output_file      = file_cfg["output_file"]
        keep_columns     = file_cfg["keep_columns"]
        filter_by_parts  = file_cfg["filter_by_parts"]

        source_path      = RAW_REBRICKABLE_DIR / source_file
        output_path      = BRONZE_CATALOGUE_DIR / output_file

        logger.info(f"Processing → {source_file}")


        if not source_path.exists():
            logger.error(f"Source file not found → {source_path}")
            failed_files.append(source_file)
            continue

        try:

            df = filter_csv(
                source_path     = source_path,
                keep_columns    = keep_columns,
                filter_by_parts = filter_by_parts,
                parts_to_process= PARTS_TO_PROCESS
            )

            df.to_csv(output_path, index=False)

            logger.info(f"Done → {output_file} ({len(df)} rows, {len(df.columns)} columns)")
            success_count += 1

        except ValueError as e:
            logger.error(f"Schema error in {source_file} → {e}")
            failed_files.append(source_file)

        except Exception as e:
            logger.error(f"Unexpected error processing {source_file} → {e}")
            failed_files.append(source_file)

    logger.info("=" * 60)
    logger.info(f"Rebrickable Bronze complete")
    logger.info(f"Success : {success_count}/{len(config['files'])}")

    if failed_files:
        logger.error(f"Failed  : {failed_files}")
    else:
        logger.info("All files processed successfully")

    logger.info("=" * 60)
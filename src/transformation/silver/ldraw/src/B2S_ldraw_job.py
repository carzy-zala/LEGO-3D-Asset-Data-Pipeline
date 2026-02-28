import json
import pandas as pd
from pathlib import Path

from config import (
    BRONZE_GEOMETRY_DIR,
    SILVER_DIR
)
from utils.logger.logger import get_logger
from src.transformation.silver.ldraw.src.B2S_ldraw import (
    validate_description,
    validate_coordinates
)

logger = get_logger("silver")
CONFIG_PATH        = Path(__file__).parent.parent / "config" / "B2S_ldraw_config.json"
SILVER_LDRAW_DIR   = SILVER_DIR / "ldraw"


def run_ldraw_silver() -> None:
    """
    Job that reads both LDraw bronze CSVs, validates them,
    adds is_valid and issues columns, and writes to silver/ldraw/.

    Output:
        data/processed/silver/ldraw/geometry_description.csv
        data/processed/silver/ldraw/geometry_coordinates.csv
    """

    logger.info("=" * 60)
    logger.info("Starting LDraw Silver transformation")
    logger.info(f"Source      : {BRONZE_GEOMETRY_DIR}")
    logger.info(f"Destination : {SILVER_LDRAW_DIR}")
    logger.info("=" * 60)

    SILVER_LDRAW_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(f"Loading config → {CONFIG_PATH.name}")
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    try:
        logger.info("Loading bronze LDraw CSVs...")

        desc_path  = BRONZE_GEOMETRY_DIR / config["input"]["description_file"]
        coord_path = BRONZE_GEOMETRY_DIR / config["input"]["coordinates_file"]

        desc_df  = pd.read_csv(desc_path,  dtype=str)
        coord_df = pd.read_csv(coord_path, dtype=str)

        logger.info(f"geometry_description : {len(desc_df)} rows")
        logger.info(f"geometry_coordinates : {len(coord_df)} rows")

    except FileNotFoundError as e:
        logger.error(f"Bronze file not found → {e}")
        return

    logger.info("Validating geometry_description...")
    desc_df = validate_description(
        df                     = desc_df,
        description_validation = config["description_validation"]
    )

    desc_invalid = desc_df[desc_df["is_valid"] == False]
    if len(desc_invalid) > 0:
        logger.error(f"Invalid description rows : {len(desc_invalid)}")
        for _, row in desc_invalid.iterrows():
            logger.error(f"  part_id={row['part_id']} | issues={row['issues']}")
    else:
        logger.info("geometry_description validation passed")

    logger.info("Validating geometry_coordinates...")
    coord_df = validate_coordinates(
        df                     = coord_df,
        coordinates_validation = config["coordinates_validation"]
    )

    coord_invalid = coord_df[coord_df["is_valid"] == False]
    if len(coord_invalid) > 0:
        logger.error(f"Invalid coordinate rows : {len(coord_invalid)}")
        for _, row in coord_invalid.iterrows():
            logger.error(f"  part_id={row['part_id']} | line_type={row['line_type']} | issues={row['issues']}")
    else:
        logger.info("geometry_coordinates validation passed")

    desc_out  = SILVER_LDRAW_DIR / config["output"]["description_file"]
    coord_out = SILVER_LDRAW_DIR / config["output"]["coordinates_file"]

    desc_df.to_csv(desc_out,   index=False)
    coord_df.to_csv(coord_out, index=False)

    logger.info(f"Written → {config['output']['description_file']} ({len(desc_df)} rows)")
    logger.info(f"Written → {config['output']['coordinates_file']} ({len(coord_df)} rows)")

    logger.info("=" * 60)
    logger.info("LDraw Silver complete")
    logger.info(f"Description rows  : {len(desc_df)} total | {len(desc_invalid)} invalid")
    logger.info(f"Coordinate rows   : {len(coord_df)} total | {len(coord_invalid)} invalid")
    logger.info("=" * 60)
import json
import pandas as pd
from pathlib import Path

from config import (
    RAW_LDRAW_DIR,
    BRONZE_GEOMETRY_DIR,
    PARTS_TO_PROCESS
)
from utils.logger.logger import get_logger
from src.transformation.bronze.ldraw.src.DS2B_ldraw import parse_dat_file

logger = get_logger("bronze")


CONFIG_PATH = Path(__file__).parent.parent / "config" / "DS2B_ldraw_config.json"


def run_ldraw_bronze() -> None:
    """
    Job that iterates through each .dat file defined in PARTS_TO_PROCESS,
    parses geometry and metadata, and writes two output CSVs to bronze/geometry/.

    Output:
        data/processed/bronze/geometry/geometry_description.csv
        data/processed/bronze/geometry/geometry_coordinates.csv
    """

    logger.info("=" * 60)
    logger.info("Starting LDraw Bronze transformation")
    logger.info(f"Parts to process : {PARTS_TO_PROCESS}")
    logger.info(f"Source           : {RAW_LDRAW_DIR}")
    logger.info(f"Destination      : {BRONZE_GEOMETRY_DIR}")
    logger.info("=" * 60)

    # ── Create output directory ────────────────────────────────────────────
    BRONZE_GEOMETRY_DIR.mkdir(parents=True, exist_ok=True)

    # ── Load config ────────────────────────────────────────────────────────
    logger.info(f"Loading config → {CONFIG_PATH.name}")
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    geometry_line_types  = config["geometry_line_types"]
    description_file     = config["output"]["description_file"]
    coordinates_file     = config["output"]["coordinates_file"]

    # ── Accumulators ───────────────────────────────────────────────────────
    all_descriptions = []
    all_coordinates  = []
    failed_parts     = []

    # ── Iterate each part ──────────────────────────────────────────────────
    for part_id in PARTS_TO_PROCESS:

        dat_path = RAW_LDRAW_DIR / f"{part_id}.dat"

        # ── Check file exists ──────────────────────────────────────────────
        if not dat_path.exists():
            logger.error(f"Missing .dat file → {dat_path.name}")
            failed_parts.append(part_id)
            continue

        logger.info(f"Parsing → {dat_path.name}")

        try:
            description, coordinates = parse_dat_file(
                dat_path            = dat_path,
                geometry_line_types = geometry_line_types
            )

            all_descriptions.append(description)
            all_coordinates.extend(coordinates)

            logger.info(
                f"Done → {part_id} | "
                f"coords: {len(coordinates)} rows"
            )

        except Exception as e:
            logger.error(f"Failed to parse {part_id}.dat → {e}")
            failed_parts.append(part_id)

    # ── Write description CSV ──────────────────────────────────────────────
    if all_descriptions:
        desc_df   = pd.DataFrame(all_descriptions, columns=config["description_columns"])
        desc_path = BRONZE_GEOMETRY_DIR / description_file
        desc_df.to_csv(desc_path, index=False)
        logger.info(f"Written → {description_file} ({len(desc_df)} rows)")

    # ── Write coordinates CSV ──────────────────────────────────────────────
    if all_coordinates:
        coord_df   = pd.DataFrame(all_coordinates, columns=config["coordinates_columns"])
        coord_path = BRONZE_GEOMETRY_DIR / coordinates_file
        coord_df.to_csv(coord_path, index=False)
        logger.info(f"Written → {coordinates_file} ({len(coord_df)} rows)")

    # ── Summary ────────────────────────────────────────────────────────────
    success_count = len(PARTS_TO_PROCESS) - len(failed_parts)

    logger.info("=" * 60)
    logger.info(f"LDraw Bronze complete")
    logger.info(f"Success : {success_count}/{len(PARTS_TO_PROCESS)}")

    if failed_parts:
        logger.error(f"Failed  : {failed_parts}")
    else:
        logger.info("All parts parsed successfully")

    logger.info("=" * 60)
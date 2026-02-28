import json
import pandas as pd
from pathlib import Path
from datetime import UTC, datetime

from config import (
    SILVER_DIR,
    GOLD_DIR,
    PARTS_TO_PROCESS,
    PIPELINE_VERSION
)
from utils.logger.logger import get_logger
from src.transformation.gold.src.S2G_gold import build_part_json

logger = get_logger("gold")

CONFIG_PATH      = Path(__file__).parent.parent / "config" / "S2G_gold_config.json"
SILVER_LDRAW_DIR = SILVER_DIR / "ldraw"
SILVER_REB_DIR   = SILVER_DIR / "rebrickable"
GOLD_PARTS_DIR   = GOLD_DIR   / "parts"


def run_gold() -> None:
    """
    Job that reads both Silver outputs, joins them per part,
    writes one JSON per part, and generates a manifest.json.

    Output:
        data/processed/gold/parts/3001.json
        data/processed/gold/parts/3003.json
        ... one per part
        data/processed/gold/manifest.json
    """

    logger.info("=" * 60)
    logger.info("Starting Gold transformation")
    logger.info(f"Destination : {GOLD_DIR}")
    logger.info("=" * 60)

    # ── Create output directories ──────────────────────────────────────────
    GOLD_PARTS_DIR.mkdir(parents=True, exist_ok=True)

    # ── Load config ────────────────────────────────────────────────────────
    logger.info(f"Loading config → {CONFIG_PATH.name}")
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    # ── Load silver CSVs ───────────────────────────────────────────────────
    try:
        logger.info("Loading Silver CSVs...")

        desc_df  = pd.read_csv(
            SILVER_LDRAW_DIR / config["input"]["ldraw_description_file"], dtype=str
        )
        coord_df = pd.read_csv(
            SILVER_LDRAW_DIR / config["input"]["ldraw_coordinates_file"], dtype=str
        )
        reb_df   = pd.read_csv(
            SILVER_REB_DIR   / config["input"]["rebrickable_file"], dtype=str
        )

        logger.info(f"geometry_description  : {len(desc_df)} rows")
        logger.info(f"geometry_coordinates  : {len(coord_df)} rows")
        logger.info(f"rebrickable_catalogue : {len(reb_df)} rows")

    except FileNotFoundError as e:
        logger.error(f"Silver file not found → {e}")
        return

    # ── Process each part ──────────────────────────────────────────────────
    manifest_parts = []
    success_count  = 0
    failed_parts   = []

    for part_id in PARTS_TO_PROCESS:

        logger.info(f"Building Gold JSON → {part_id}")

        try:
            # ── Build enriched JSON ────────────────────────────────────────
            part_json = build_part_json(
                part_id           = part_id,
                desc_df           = desc_df,
                coord_df          = coord_df,
                rebrickable_df    = reb_df,
                coordinate_fields = config["coordinate_fields"],
                pipeline_version  = PIPELINE_VERSION
            )

            # ── Write part JSON ────────────────────────────────────────────
            output_path = GOLD_PARTS_DIR / f"{part_id}.json"
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(part_json, f, indent=2, default=str)

            logger.info(
                f"Written → {part_id}.json | "
                f"valid={part_json['pipeline_metadata']['is_valid']} | "
                f"faces={part_json['geometry']['stats']['total_faces']}"
            )

            # ── Add to manifest ────────────────────────────────────────────
            manifest_parts.append({
                "part_id"    : part_id,
                "part_name"  : part_json.get("part_name"),
                "category"   : part_json.get("category"),
                "is_valid"   : part_json["pipeline_metadata"]["is_valid"],
                "total_faces": part_json["geometry"]["stats"]["total_faces"],
                "issues"     : part_json["pipeline_metadata"]["quality_issues"],
                "output_path": str(output_path)
            })

            success_count += 1

        except Exception as e:
            logger.error(f"Failed to build Gold JSON for {part_id} → {e}")
            failed_parts.append(part_id)

    # ── Write manifest ─────────────────────────────────────────────────────
    _write_manifest(manifest_parts, failed_parts, config)

    # ── Summary ────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Gold transformation complete")
    logger.info(f"Success : {success_count}/{len(PARTS_TO_PROCESS)}")
    if failed_parts:
        logger.error(f"Failed  : {failed_parts}")
    else:
        logger.info("All parts processed successfully")
    logger.info("=" * 60)


def _write_manifest(
    manifest_parts : list,
    failed_parts   : list,
    config         : dict
) -> None:
    """Writes manifest.json summarising the entire Gold dataset."""

    valid_count   = sum(1 for p in manifest_parts if p["is_valid"])
    invalid_count = len(manifest_parts) - valid_count

    manifest = {
        "pipeline_version" : PIPELINE_VERSION,
        "processed_date": datetime.now(UTC).strftime("%Y-%m-%d"),
        "total_parts"      : len(manifest_parts),
        "valid_parts"      : valid_count,
        "invalid_parts"    : invalid_count,
        "failed_parts"     : failed_parts,
        "parts"            : manifest_parts
    }

    manifest_path = GOLD_DIR / config["output"]["manifest_file"]
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    logger.info(f"Manifest written → {manifest_path.name}")
    logger.info(f"Total={len(manifest_parts)} | Valid={valid_count} | Invalid={invalid_count}")
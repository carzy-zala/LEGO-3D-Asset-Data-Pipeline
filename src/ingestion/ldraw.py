import zipfile
from pathlib import Path

from config import (
    RAW_LDRAW_DIR,
    PARTS_TO_PROCESS,
    LDRAW_ZIP_LOCAL,
    LDRAW_PARTS_ZIP_PREFIX
)
from utils.http_client.http_client import HttpClient
from utils.logger.logger import get_logger

logger = get_logger("ingestion")


def download_ldraw_files() -> None:
    """
    Downloads the LDraw complete parts library zip and extracts
    only the parts defined in PARTS_TO_PROCESS from config.

    Output:
        data/raw/ldraw/3001.dat
        data/raw/ldraw/3003.dat
        ... etc
    """

    logger.info("=" * 60)
    logger.info("Starting LDraw ingestion")
    logger.info(f"Parts to extract : {PARTS_TO_PROCESS}")
    logger.info(f"Destination      : {RAW_LDRAW_DIR}")
    logger.info("=" * 60)

    RAW_LDRAW_DIR.mkdir(parents=True, exist_ok=True)

    existing = [p for p in PARTS_TO_PROCESS if (RAW_LDRAW_DIR / f"{p}.dat").exists()]
    if len(existing) == len(PARTS_TO_PROCESS):
        logger.info("All .dat files already exist — skipping download")
        return

    if not LDRAW_ZIP_LOCAL.exists():
        logger.info("Downloading LDraw complete parts library (~170MB)...")
        client = HttpClient(base_url="https://library.ldraw.org")
        success = client.download_file(
            path="/library/updates/complete.zip",
            destination=str(LDRAW_ZIP_LOCAL)
        )
        if not success:
            logger.error("Failed to download LDraw complete.zip")
            return
        logger.info("Download complete")
    else:
        logger.info("complete.zip already exists — skipping download")

    logger.info("Extracting required parts from zip...")
    _extract_parts(LDRAW_ZIP_LOCAL, PARTS_TO_PROCESS)

    logger.info("Removing complete.zip to save disk space...")
    LDRAW_ZIP_LOCAL.unlink()
    logger.info("complete.zip removed")

    extracted = [p for p in PARTS_TO_PROCESS if (RAW_LDRAW_DIR / f"{p}.dat").exists()]
    missing   = [p for p in PARTS_TO_PROCESS if p not in extracted]

    logger.info("=" * 60)
    logger.info(f"LDraw ingestion complete")
    logger.info(f"Extracted : {len(extracted)}/{len(PARTS_TO_PROCESS)}")

    if missing:
        logger.error(f"Missing parts not found in library: {missing}")
    else:
        logger.info("All parts extracted successfully")

    logger.info("=" * 60)


def _extract_parts(zip_path: Path, part_ids: list) -> None:
    """
    Extracts specific part .dat files from the LDraw zip archive.

    Args:
        zip_path : Path to complete.zip
        part_ids : List of part IDs to extract e.g. ["3001", "3003"]
    """
    try:
        RAW_LDRAW_DIR.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            all_files = zf.namelist()

            for part_id in part_ids:

                zip_entry = f"{LDRAW_PARTS_ZIP_PREFIX}{part_id}.dat"

                if zip_entry not in all_files:
                    logger.error(f"Part not found in zip: {part_id}.dat")
                    continue

                destination = RAW_LDRAW_DIR / f"{part_id}.dat"

                if destination.exists():
                    logger.info(f"Already exists — skipping: {part_id}.dat")
                    continue

                data = zf.read(zip_entry)
                destination.write_bytes(data)
                logger.info(f"Extracted → {destination.name}")

    except zipfile.BadZipFile:
        logger.error(f"complete.zip is corrupted → {zip_path}")
    except Exception as e:
        logger.error(f"Unexpected error extracting parts → {e}")
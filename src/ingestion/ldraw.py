import zipfile
from pathlib import Path

from config import (
    RAW_LDRAW_DIR,
    PARTS_TO_PROCESS,
    LDRAW_COMPLETE_ZIP_URL,
    LDRAW_ZIP_LOCAL,
    LDRAW_PARTS_ZIP_PREFIX,
    LDRAW_LIBRARY_DIR
)
from utils.http_client.http_client import HttpClient
from utils.logger.logger import get_logger

logger = get_logger("ingestion")


def download_ldraw_files() -> None:
    """
    Downloads the LDraw complete parts library zip and:
    1. Extracts target .dat files to data/raw/ldraw/
    2. Extracts full library to data/raw/ldraw/library/ for sub-part resolution

    Output:
        data/raw/ldraw/3001.dat          ← target parts
        data/raw/ldraw/library/parts/    ← full parts library for sub-part resolution
        data/raw/ldraw/library/p/        ← primitives library
    """

    logger.info("=" * 60)
    logger.info("Starting LDraw ingestion")
    logger.info(f"Parts to extract : {PARTS_TO_PROCESS}")
    logger.info(f"Destination      : {RAW_LDRAW_DIR}")
    logger.info("=" * 60)

    RAW_LDRAW_DIR.mkdir(parents=True, exist_ok=True)

    # ── Check if already fully set up ─────────────────────────────────────
    existing    = [p for p in PARTS_TO_PROCESS if (RAW_LDRAW_DIR / f"{p}.dat").exists()]
    lib_exists  = (LDRAW_LIBRARY_DIR / "parts").exists()

    if len(existing) == len(PARTS_TO_PROCESS) and lib_exists:
        logger.info("All .dat files and library already exist — skipping download")
        return

    # ── Download complete.zip ──────────────────────────────────────────────
    if not LDRAW_ZIP_LOCAL.exists():
        logger.info("Downloading LDraw complete parts library (~170MB)...")
        client = HttpClient(base_url="https://library.ldraw.org")
        success = client.download_file(
            path        = "/library/updates/complete.zip",
            destination = str(LDRAW_ZIP_LOCAL)
        )
        if not success:
            logger.error("Failed to download LDraw complete.zip")
            return
        logger.info("Download complete")
    else:
        logger.info("complete.zip already exists — skipping download")

    # ── Extract target parts + full library ───────────────────────────────
    logger.info("Extracting parts and library...")
    _extract_parts_and_library(LDRAW_ZIP_LOCAL, PARTS_TO_PROCESS)

    # ── Remove zip to save disk space ──────────────────────────────────────
    logger.info("Removing complete.zip...")
    LDRAW_ZIP_LOCAL.unlink()

    # ── Summary ────────────────────────────────────────────────────────────
    extracted = [p for p in PARTS_TO_PROCESS if (RAW_LDRAW_DIR / f"{p}.dat").exists()]
    missing   = [p for p in PARTS_TO_PROCESS if p not in extracted]

    logger.info("=" * 60)
    logger.info(f"LDraw ingestion complete")
    logger.info(f"Extracted parts  : {len(extracted)}/{len(PARTS_TO_PROCESS)}")
    logger.info(f"Library path     : {LDRAW_LIBRARY_DIR}")

    if missing:
        logger.error(f"Missing parts: {missing}")
    else:
        logger.info("All parts extracted successfully")

    logger.info("=" * 60)


def _extract_parts_and_library(zip_path: Path, part_ids: list) -> None:
    """
    Extracts:
    1. Target .dat files to RAW_LDRAW_DIR
    2. Full parts/ and p/ directories to LDRAW_LIBRARY_DIR
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            all_files = zf.namelist()

            # ── Extract target parts ───────────────────────────────────────
            for part_id in part_ids:
                zip_entry   = f"{LDRAW_PARTS_ZIP_PREFIX}{part_id}.dat"
                destination = RAW_LDRAW_DIR / f"{part_id}.dat"

                if destination.exists():
                    logger.info(f"Already exists — skipping: {part_id}.dat")
                    continue

                if zip_entry not in all_files:
                    logger.error(f"Part not found in zip: {part_id}.dat")
                    continue

                data = zf.read(zip_entry)
                destination.write_bytes(data)
                logger.info(f"Extracted → {destination.name}")

            # ── Extract full library for sub-part resolution ───────────────
            logger.info("Extracting full library for sub-part resolution...")
            count = 0
            for entry in all_files:
                # Extract parts/ and p/ directories only
                if entry.startswith("ldraw/parts/") or entry.startswith("ldraw/p/"):
                    # Map ldraw/parts/ → library/parts/
                    relative    = entry.replace("ldraw/", "", 1)
                    output_path = LDRAW_LIBRARY_DIR / relative

                    if output_path.exists():
                        continue

                    output_path.parent.mkdir(parents=True, exist_ok=True)

                    if not entry.endswith("/"):
                        data = zf.read(entry)
                        output_path.write_bytes(data)
                        count += 1

            logger.info(f"Library extracted → {count} files")

    except zipfile.BadZipFile:
        logger.error(f"complete.zip is corrupted → {zip_path}")
    except Exception as e:
        logger.error(f"Unexpected error → {e}")
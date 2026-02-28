import gzip
import shutil
from pathlib import Path

from config import (
    RAW_REBRICKABLE_DIR,
    REBRICKABLE_BASE_URL,
    REBRICKABLE_FILES
)
from utils.http_client.http_client import HttpClient
from utils.logger.logger import get_logger

logger = get_logger("ingestion")


def download_rebrickable_files() -> None:
    """
    Downloads all required Rebrickable CSV files into data/raw/rebrickable/.
    Files are served as .gz — this function downloads and extracts them.

    Output:
        data/raw/rebrickable/parts.csv
        data/raw/rebrickable/colors.csv
        data/raw/rebrickable/part_categories.csv
        data/raw/rebrickable/inventory_parts.csv
    """

    logger.info("=" * 60)
    logger.info("Starting Rebrickable ingestion")
    logger.info(f"Destination → {RAW_REBRICKABLE_DIR}")
    logger.info("=" * 60)

    RAW_REBRICKABLE_DIR.mkdir(parents=True, exist_ok=True)

    client = HttpClient(base_url=REBRICKABLE_BASE_URL)

    success_count = 0
    failed_files  = []

    for remote_path, filename in REBRICKABLE_FILES:

        gz_destination  = RAW_REBRICKABLE_DIR / filename
        csv_destination = RAW_REBRICKABLE_DIR / filename.replace(".gz", "")

        
        if csv_destination.exists():
            logger.info(f"Already exists — skipping → {csv_destination.name}")
            success_count += 1
            continue

        logger.info(f"Downloading → {filename}")
        try:
            success = client.download_file(
                path=remote_path,
                destination=str(gz_destination)
            )

            if not success:
                logger.error(f"Download failed → {filename}")
                failed_files.append(filename)
                continue

            logger.info(f"Extracting → {filename}")
            _extract_gz(gz_destination, csv_destination)

            gz_destination.unlink()
            logger.info(f"Done → {csv_destination.name}")
            success_count += 1

        except Exception as e:
            logger.error(f"Unexpected error for {filename} → {e}")
            failed_files.append(filename)


    logger.info("=" * 60)
    logger.info(f"Rebrickable ingestion complete")
    logger.info(f"Success : {success_count}/{len(REBRICKABLE_FILES)}")

    if failed_files:
        logger.error(f"Failed  : {failed_files}")
    else:
        logger.info("All files downloaded successfully")

    logger.info("=" * 60)


def _extract_gz(gz_path: Path, destination: Path) -> None:
    """
    Extracts a .gz file to destination path.

    Args:
        gz_path     : Path to the .gz file
        destination : Path to save the extracted file
    """
    with gzip.open(gz_path, "rb") as f_in:
        with open(destination, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    logger.info(f"Extracted → {destination.name}")
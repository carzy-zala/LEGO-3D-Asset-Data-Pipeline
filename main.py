import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.ingestion.rebrickable import download_rebrickable_files
from src.ingestion.ldraw import download_ldraw_files
from src.transformation.bronze.rebrickable.src.DS2B_rebrickable_job import run_rebrickable_bronze
from src.transformation.bronze.ldraw.src.DS2B_ldraw_job import run_ldraw_bronze
from src.transformation.silver.rebrickable.src.B2S_rebrickable_job import run_rebrickable_silver
from src.transformation.silver.ldraw.src.B2S_ldraw_job import run_ldraw_silver
from src.transformation.gold.src.S2G_gold_job import run_gold

if __name__ == "__main__":
    # Stage 1 — Ingestion
    download_rebrickable_files()
    download_ldraw_files()

    # Stage 2 — Bronze
    run_rebrickable_bronze()
    run_ldraw_bronze()

    # Stage 3 — Silver
    run_rebrickable_silver()
    run_ldraw_silver()

    # Stage 4 — Gold
    run_gold()
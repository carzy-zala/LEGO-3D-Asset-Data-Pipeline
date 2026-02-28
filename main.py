import sys
from pathlib import Path

from src.transformation.bronze.ldraw.src.DS2B_ldraw_job import run_ldraw_bronze
from src.transformation.silver.ldraw.src.B2S_ldraw_job import run_ldraw_silver
from src.transformation.silver.rebrickable.src.B2S_rebrickable_job import run_rebrickable_silver
sys.path.insert(0, str(Path(__file__).parent))

from src.ingestion.rebrickable import download_rebrickable_files
from src.ingestion.ldraw import download_ldraw_files
from src.transformation.bronze.rebrickable.src.DS2B_rebrickable_job import run_rebrickable_bronze

if __name__ == "__main__":
    download_rebrickable_files()
    download_ldraw_files()
    
    run_rebrickable_bronze()
    run_ldraw_bronze()
    
    run_rebrickable_silver()
    run_ldraw_silver()
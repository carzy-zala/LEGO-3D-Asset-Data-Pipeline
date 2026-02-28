import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.ingestion.rebrickable import download_rebrickable_files
from src.ingestion.ldraw import download_ldraw_files
from src.transformation.bronze.rebrickable.src.DS2B_rebrickable_job import run_rebrickable_bronze

if __name__ == "__main__":
    download_rebrickable_files()
    download_ldraw_files()
    
    run_rebrickable_bronze()
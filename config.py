from pathlib import Path

# ── Base Paths ─────────────────────────────────────────────────────────────
BASE_DIR                = Path(__file__).parent
DATA_DIR                = BASE_DIR / "data"

# ── Raw ────────────────────────────────────────────────────────────────────
RAW_DIR                 = DATA_DIR / "raw"
RAW_REBRICKABLE_DIR     = RAW_DIR / "rebrickable"
RAW_LDRAW_DIR           = RAW_DIR / "ldraw"

# ── Rebrickable ────────────────────────────────────────────────────────────
REBRICKABLE_BASE_URL    = "https://cdn.rebrickable.com"

# Files to download — (remote path, local filename)
REBRICKABLE_FILES = [
    ("/media/downloads/parts.csv.gz",            "parts.csv.gz"),
    ("/media/downloads/colors.csv.gz",           "colors.csv.gz"),
    ("/media/downloads/part_categories.csv.gz",  "part_categories.csv.gz"),
    ("/media/downloads/inventory_parts.csv.gz",  "inventory_parts.csv.gz"),
]

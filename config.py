from pathlib import Path

# ── Base Paths ─────────────────────────────────────────────────────────────
BASE_DIR                = Path(__file__).parent
DATA_DIR                = BASE_DIR / "data"

# ── Raw ────────────────────────────────────────────────────────────────────
RAW_DIR                 = DATA_DIR / "raw"
RAW_REBRICKABLE_DIR     = RAW_DIR / "rebrickable"
RAW_LDRAW_DIR           = RAW_DIR / "ldraw"


PARTS_TO_PROCESS = [
    "3001",   # Brick 2x4
    "3003",   # Brick 2x2
    "3005",   # Brick 1x1
    "3010",   # Brick 1x4
    "3020",   # Plate 2x4
    "3070b",  # Tile 1x1
    "3049c",  # Slope
    "4733",   # Brick with Studs
]

# ── Rebrickable ────────────────────────────────────────────────────────────
REBRICKABLE_BASE_URL    = "https://cdn.rebrickable.com"

# Files to download — (remote path, local filename)
REBRICKABLE_FILES = [
    ("/media/downloads/parts.csv.gz",            "parts.csv.gz"),
    ("/media/downloads/colors.csv.gz",           "colors.csv.gz"),
    ("/media/downloads/part_categories.csv.gz",  "part_categories.csv.gz"),
    ("/media/downloads/inventory_parts.csv.gz",  "inventory_parts.csv.gz"),
]

# ── LDraw ──────────────────────────────────────────────────────────────────
LDRAW_ZIP_LOCAL         = RAW_LDRAW_DIR / "complete.zip"
LDRAW_PARTS_ZIP_PREFIX  = "ldraw/parts/"
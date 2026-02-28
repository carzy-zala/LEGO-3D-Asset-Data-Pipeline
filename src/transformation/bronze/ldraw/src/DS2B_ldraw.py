from pathlib import Path
from typing import Dict, List, Tuple


def parse_dat_file(
    dat_path: Path,
    geometry_line_types: List[int],
    ldraw_parts_dir: Path = None,
    _visited: set = None
) -> Tuple[Dict, List[Dict]]:
    """
    Parses a single LDraw .dat file into two structures:
    - description : metadata from header comment lines (line type 0)
    - coordinates : geometry rows from line types 2, 3, 4

    Recursively resolves line type 1 sub-part references
    to extract full geometry.

    Args:
        dat_path            : Path to the .dat file
        geometry_line_types : Which line types to extract as geometry
        ldraw_parts_dir     : Root parts directory for resolving sub-parts
        _visited            : Internal set to prevent infinite recursion

    Returns:
        Tuple of (description_dict, list_of_coordinate_dicts)
    """

    # ── Guard against infinite recursion ──────────────────────────────────
    if _visited is None:
        _visited = set()

    real_path = str(dat_path.resolve())
    if real_path in _visited:
        return _init_description(dat_path.stem), []
    _visited.add(real_path)

    part_id     = dat_path.stem
    description = _init_description(part_id)
    coordinates = []

    with open(dat_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    for raw_line in lines:
        line = raw_line.strip()

        if not line:
            continue

        tokens = line.split()

        if not tokens[0].lstrip("-").isdigit():
            continue

        line_type = int(tokens[0])

        # ── Line type 0 — metadata ─────────────────────────────────────────
        if line_type == 0:
            _extract_metadata(tokens, description)
            continue

        # ── Line type 1 — sub-part reference ──────────────────────────────
        # Format: 1 colour x y z a b c d e f g h i filename
        if line_type == 1 and ldraw_parts_dir and len(tokens) >= 15:
            sub_filename = tokens[14]
            sub_path     = _resolve_subpart(sub_filename, ldraw_parts_dir, dat_path.parent)

            if sub_path and sub_path.exists():
                _, sub_coords = parse_dat_file(
                    dat_path            = sub_path,
                    geometry_line_types = geometry_line_types,
                    ldraw_parts_dir     = ldraw_parts_dir,
                    _visited            = _visited
                )
                # Tag sub-part coordinates with the root part_id
                for coord in sub_coords:
                    coord["part_id"] = part_id
                coordinates.extend(sub_coords)
            continue

        # ── Line types 2, 3, 4 — direct geometry ──────────────────────────
        if line_type in geometry_line_types:
            coord_row = _extract_coordinates(part_id, line_type, tokens)
            if coord_row:
                coordinates.append(coord_row)

    return description, coordinates


# ── Private Helpers ────────────────────────────────────────────────────────

def _resolve_subpart(
    filename        : str,
    ldraw_parts_dir : Path,
    current_dir     : Path
) -> Path:
    """
    Resolves a sub-part filename to an actual file path.
    LDraw sub-parts can live in:
        - ldraw/parts/s/         (sub-parts folder)
        - ldraw/parts/           (main parts folder)
        - ldraw/p/               (primitives folder)

    Args:
        filename        : Sub-part filename e.g. "s\\3001s01.dat" or "stud.dat"
        ldraw_parts_dir : Root parts directory
        current_dir     : Directory of the current .dat file

    Returns:
        Resolved Path or None if not found
    """

    # Normalise path separators
    normalised = filename.replace("\\", "/")

    # Search locations in order of priority
    search_paths = [
        ldraw_parts_dir / normalised,                    # ldraw/parts/s/3001s01.dat
        ldraw_parts_dir / normalised.split("/")[-1],     # ldraw/parts/3001s01.dat
        ldraw_parts_dir.parent / "p" / normalised,       # ldraw/p/stud.dat
        current_dir / normalised                         # same directory
    ]

    for path in search_paths:
        if path.exists():
            return path

    return None


def _init_description(part_id: str) -> Dict:
    """Returns empty description dict for a part."""
    return {
        "part_id"  : part_id,
        "part_name": None,
        "author"   : None,
        "license"  : None
    }


def _extract_metadata(tokens: List[str], description: Dict) -> None:
    """
    Extracts metadata from line type 0 comment lines.
    Mutates description dict in place.
    """
    if len(tokens) < 2:
        return

    content = " ".join(tokens[1:])

    if (
        description["part_name"] is None
        and not content.startswith("Name:")
        and not content.startswith("Author:")
        and not content.startswith("!")
        and not content.startswith("FILE")
        and not content.startswith("BFC")
    ):
        description["part_name"] = content

    elif content.startswith("Author:"):
        description["author"] = content.replace("Author:", "").strip()

    elif content.startswith("!LICENSE"):
        description["license"] = content.replace("!LICENSE", "").strip()


def _extract_coordinates(
    part_id   : str,
    line_type : int,
    tokens    : List[str]
) -> Dict:
    """
    Extracts coordinate row from a geometry line.

    Line type 2 → 2 vertices →  8 tokens total
    Line type 3 → 3 vertices → 11 tokens total
    Line type 4 → 4 vertices → 14 tokens total
    """

    expected_tokens = {2: 8, 3: 11, 4: 14}

    if len(tokens) < expected_tokens.get(line_type, 0):
        return None

    try:
        colour = tokens[1]
        coords = [float(t) for t in tokens[2:expected_tokens[line_type]]]
        padded = coords + [None] * (12 - len(coords))

        return {
            "part_id"  : part_id,
            "line_type": line_type,
            "colour"   : colour,
            "x1": padded[0],  "y1": padded[1],  "z1": padded[2],
            "x2": padded[3],  "y2": padded[4],  "z2": padded[5],
            "x3": padded[6],  "y3": padded[7],  "z3": padded[8],
            "x4": padded[9],  "y4": padded[10], "z4": padded[11]
        }

    except (ValueError, IndexError):
        return None
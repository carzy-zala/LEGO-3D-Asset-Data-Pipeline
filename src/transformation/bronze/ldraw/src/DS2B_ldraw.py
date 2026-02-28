from pathlib import Path
from typing import Dict, List, Tuple
import re


def parse_dat_file(
    dat_path: Path,
    geometry_line_types: List[int]
) -> Tuple[Dict, List[Dict]]:
    """
    Parses a single LDraw .dat file into two structures:
    - description : metadata from header comment lines (line type 0)
    - coordinates : geometry rows from line types 2, 3, 4

    Args:
        dat_path             : Path to the .dat file
        geometry_line_types  : Which line types to extract as geometry (from config)

    Returns:
        Tuple of (description_dict, list_of_coordinate_dicts)
    """

    part_id     = dat_path.stem  # filename without .dat = part_id
    description = _init_description(part_id)
    coordinates = []

    with open(dat_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    for raw_line in lines:
        line = raw_line.strip()

        # ── Skip empty lines ───────────────────────────────────────────────
        if not line:
            continue

        tokens = line.split()

        # ── Skip if no valid line type token ───────────────────────────────
        if not tokens[0].lstrip("-").isdigit():
            continue

        line_type = int(tokens[0])

        # ── Line type 0 — metadata ─────────────────────────────────────────
        if line_type == 0:
            _extract_metadata(tokens, description)
            continue

        # ── Line types 2, 3, 4 — geometry ─────────────────────────────────
        if line_type in geometry_line_types:
            coord_row = _extract_coordinates(part_id, line_type, tokens)
            if coord_row:
                coordinates.append(coord_row)

    return description, coordinates



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

    LDraw header example:
        0 Brick 2 x 4
        0 Name: 3001.dat
        0 Author: James Jessiman
        0 !LICENSE Licensed under CC BY 4.0
    """
    if len(tokens) < 2:
        return

    # Join everything after the line type
    content = " ".join(tokens[1:])

    # ── Part name — first non-keyword comment line ─────────────────────────
    if (
        description["part_name"] is None
        and not content.startswith("Name:")
        and not content.startswith("Author:")
        and not content.startswith("!")
        and not content.startswith("FILE")
    ):
        description["part_name"] = content

    # ── Author ─────────────────────────────────────────────────────────────
    elif content.startswith("Author:"):
        description["author"] = content.replace("Author:", "").strip()

    # ── License ────────────────────────────────────────────────────────────
    elif content.startswith("!LICENSE"):
        description["license"] = content.replace("!LICENSE", "").strip()


def _extract_coordinates(
    part_id: str,
    line_type: int,
    tokens: List[str]
) -> Dict:
    """
    Extracts coordinate row from a geometry line.

    LDraw geometry format:
        type  colour  x1 y1 z1  x2 y2 z2  [x3 y3 z3]  [x4 y4 z4]

    Line type 2 → 2 vertices → 7  tokens after type (colour + 6 coords)
    Line type 3 → 3 vertices → 10 tokens after type (colour + 9 coords)
    Line type 4 → 4 vertices → 13 tokens after type (colour + 12 coords)

    Returns:
        Dict with part_id, line_type, colour, x1..z4 (NULL if not applicable)
        None if line is malformed
    """

    # Expected token counts per line type (including the line_type token itself)
    expected_tokens = {2: 8, 3: 11, 4: 14}

    if len(tokens) < expected_tokens.get(line_type, 0):
        return None

    try:
        colour = tokens[1]

        # ── Parse all available coordinates ───────────────────────────────
        coords = [float(t) for t in tokens[2:expected_tokens[line_type]]]

        # ── Build row with NULLs for missing vertices ──────────────────────
        # Pad to always have 12 coordinate values (4 vertices x 3 axes)
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
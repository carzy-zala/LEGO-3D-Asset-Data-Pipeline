import pandas as pd
from typing import Dict, List, Optional


def build_part_json(
    part_id            : str,
    desc_df            : pd.DataFrame,
    coord_df           : pd.DataFrame,
    rebrickable_df     : pd.DataFrame,
    coordinate_fields  : List[str],
    pipeline_version   : str
) -> Dict:
    """
    Builds a single enriched JSON record for one LEGO part
    by joining LDraw description, coordinates, and Rebrickable metadata.

    Args:
        part_id           : The part ID to build JSON for
        desc_df           : Silver geometry_description DataFrame
        coord_df          : Silver geometry_coordinates DataFrame
        rebrickable_df    : Silver rebrickable_catalogue DataFrame
        coordinate_fields : Coordinate columns to include from config
        pipeline_version  : Pipeline version from config

    Returns:
        Enriched dict ready to be written as JSON
    """

    from datetime import datetime

    # ── Get description row ────────────────────────────────────────────────
    desc_row = desc_df[desc_df["part_id"] == part_id]
    desc     = desc_row.iloc[0].to_dict() if not desc_row.empty else {}

    # ── Get coordinate rows for this part ──────────────────────────────────
    part_coords = coord_df[coord_df["part_id"] == part_id]

    # ── Get rebrickable rows for this part ─────────────────────────────────
    reb_rows = rebrickable_df[rebrickable_df["part_num"] == part_id]

    # ── Build geometry stats ───────────────────────────────────────────────
    valid_coords  = part_coords[part_coords["is_valid"] == "True"] if "is_valid" in part_coords.columns else part_coords
    total_faces   = len(valid_coords)
    triangles     = len(valid_coords[valid_coords["line_type"] == "3"])
    quads         = len(valid_coords[valid_coords["line_type"] == "4"])
    lines         = len(valid_coords[valid_coords["line_type"] == "2"])

    # ── Build coordinates list ─────────────────────────────────────────────
    coordinates = _build_coordinates(valid_coords, coordinate_fields)

    # ── Build colour info from first rebrickable row ───────────────────────
    colour = _build_colour(reb_rows)

    # ── Determine overall validity ─────────────────────────────────────────
    desc_valid  = str(desc.get("is_valid", "True")) == "True"
    coord_valid = total_faces > 0
    is_valid    = desc_valid and coord_valid

    # ── Collect all quality issues ─────────────────────────────────────────
    quality_issues = _collect_issues(desc, part_coords)

    # ── Assemble final JSON ────────────────────────────────────────────────
    return {
        "part_id"   : part_id,
        "part_name" : desc.get("part_name"),
        "category"  : reb_rows.iloc[0]["category_name"] if not reb_rows.empty else None,
        "material"  : reb_rows.iloc[0]["part_material"]  if not reb_rows.empty else None,
        "colour"    : colour,
        "geometry"  : {
            "description": {
                "author"  : desc.get("author"),
                "license" : desc.get("license")
            },
            "stats": {
                "total_faces" : total_faces,
                "triangles"   : triangles,
                "quads"       : quads,
                "lines"       : lines
            },
            "coordinates": coordinates
        },
        "pipeline_metadata": {
            "pipeline_version" : pipeline_version,
            "processed_date"   : datetime.utcnow().strftime("%Y-%m-%d"),
            "is_valid"         : is_valid,
            "quality_issues"   : quality_issues
        }
    }


# ── Private Helpers ────────────────────────────────────────────────────────

def _build_coordinates(
    coords_df         : pd.DataFrame,
    coordinate_fields : List[str]
) -> List[Dict]:
    """
    Converts coordinate rows into a list of dicts.
    Drops NULL vertex columns per row based on line_type.
    """
    result = []

    for _, row in coords_df.iterrows():
        line_type = int(row["line_type"]) if not pd.isnull(row["line_type"]) else None

        # Determine how many vertices this line type has
        vertex_count = {2: 2, 3: 3, 4: 4}.get(line_type, 0)

        # Build vertices list — only include valid vertices for this line_type
        vertices = []
        for i in range(1, vertex_count + 1):
            x = row.get(f"x{i}")
            y = row.get(f"y{i}")
            z = row.get(f"z{i}")
            if not pd.isnull(x):
                vertices.append({
                    "x": float(x),
                    "y": float(y),
                    "z": float(z)
                })

        result.append({
            "line_type" : line_type,
            "colour"    : row.get("colour"),
            "vertices"  : vertices
        })

    return result


def _build_colour(reb_rows: pd.DataFrame) -> Optional[Dict]:
    """Builds colour dict from rebrickable rows."""
    if reb_rows.empty:
        return None

    row = reb_rows.iloc[0]
    return {
        "color_id"       : row.get("color_id"),
        "name"           : row.get("color_name"),
        "rgb"            : row.get("rgb"),
        "is_transparent" : row.get("is_trans")
    }


def _collect_issues(
    desc      : Dict,
    coord_df  : pd.DataFrame
) -> List[str]:
    """Collects all quality issues from description and coordinate validation."""
    issues = []

    # From description
    desc_issues = desc.get("issues", "")
    if desc_issues:
        issues.extend(str(desc_issues).split(","))

    # From coordinates
    if "issues" in coord_df.columns:
        coord_issues = coord_df[
            (coord_df["is_valid"] == "False") &
            (coord_df["issues"].notna()) &
            (coord_df["issues"] != "")
        ]["issues"].tolist()
        issues.extend(coord_issues)

    return [i for i in issues if i]
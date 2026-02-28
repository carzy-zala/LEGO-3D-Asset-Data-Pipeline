from pathlib import Path

from src.transformation.bronze.ldraw.src.DS2B_ldraw import parse_dat_file 

def _write_dat(tmp_path: Path, name: str, text: str) -> Path:
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    return p


def test_parse_metadata_part_name_author_license(tmp_path):
    dat = _write_dat(
        tmp_path,
        "3001.dat",
        "\n".join([
            "0 Brick 2 x 4",
            "0 Name: 3001.dat",
            "0 Author: James Jessiman",
            "0 !LICENSE Licensed under CC BY 4.0",
            "0 FILE whatever",   # should not override part_name
        ]) + "\n"
    )

    desc, coords = parse_dat_file(dat_path=dat, geometry_line_types=[2, 3, 4])

    assert desc["part_id"] == "3001"
    assert desc["part_name"] == "Brick 2 x 4"
    assert desc["author"] == "James Jessiman"
    assert desc["license"] == "Licensed under CC BY 4.0"
    assert coords == []


def test_parse_geometry_types_2_3_4(tmp_path):
    dat = _write_dat(
        tmp_path,
        "3003.dat",
        "\n".join([
            "0 Plate 2 x 2",
            "2 16 0 0 0  10 0 0",
            "3 24 0 0 0  10 0 0  0 10 0",
            "4 7  0 0 0  10 0 0  10 10 0  0 10 0",
        ]) + "\n"
    )

    desc, coords = parse_dat_file(dat_path=dat, geometry_line_types=[2, 3, 4])

    assert desc["part_id"] == "3003"
    assert desc["part_name"] == "Plate 2 x 2"
    assert len(coords) == 3

    # type 2 should have x1..z2 and x3..z4 None
    c2 = coords[0]
    assert c2["line_type"] == 2
    assert c2["colour"] == "16"
    assert (c2["x1"], c2["y1"], c2["z1"]) == (0.0, 0.0, 0.0)
    assert (c2["x2"], c2["y2"], c2["z2"]) == (10.0, 0.0, 0.0)
    assert c2["x3"] is None and c2["z4"] is None

    # type 3 should have x1..z3 and x4..z4 None
    c3 = coords[1]
    assert c3["line_type"] == 3
    assert c3["colour"] == "24"
    assert (c3["x3"], c3["y3"], c3["z3"]) == (0.0, 10.0, 0.0)
    assert c3["x4"] is None and c3["z4"] is None

    # type 4 should have all vertices
    c4 = coords[2]
    assert c4["line_type"] == 4
    assert c4["colour"] == "7"
    assert (c4["x4"], c4["y4"], c4["z4"]) == (0.0, 10.0, 0.0)


def test_geometry_line_types_filtering(tmp_path):
    dat = _write_dat(
        tmp_path,
        "3005.dat",
        "\n".join([
            "0 Some Part",
            "2 16 0 0 0  1 1 1",
            "3 24 0 0 0  1 0 0  0 1 0",
        ]) + "\n"
    )

    # only extract triangles (type 3)
    desc, coords = parse_dat_file(dat_path=dat, geometry_line_types=[3])

    assert desc["part_id"] == "3005"
    assert len(coords) == 1
    assert coords[0]["line_type"] == 3


def test_skips_invalid_and_malformed_lines(tmp_path):
    dat = _write_dat(
        tmp_path,
        "9999.dat",
        "\n".join([
            "",                          # empty
            "this is not ldraw",         # non-numeric first token
            "2 16 0 0 0  1 1",           # malformed type 2 (not enough tokens)
            "3 24 0 0 0  1 0 0  x y z",  # invalid float tokens -> skipped
            "0 Valid Name Line",
            "2 16 0 0 0  1 1 1",         # valid
        ]) + "\n"
    )

    desc, coords = parse_dat_file(dat_path=dat, geometry_line_types=[2, 3, 4])

    assert desc["part_id"] == "9999"
    assert desc["part_name"] == "Valid Name Line"
    assert len(coords) == 1
    assert coords[0]["line_type"] == 2
    assert coords[0]["x2"] == 1.0


def test_metadata_part_name_not_overwritten_by_keywords(tmp_path):
    dat = _write_dat(
        tmp_path,
        "1234.dat",
        "\n".join([
            "0 FILE something",
            "0 Name: 1234.dat",
            "0 Author: Someone",
            "0 !LICENSE CC0",
            "0 Actual Part Name",
            "0 Another Comment",  # should NOT overwrite part_name once set
        ]) + "\n"
    )

    desc, coords = parse_dat_file(dat_path=dat, geometry_line_types=[2, 3, 4])

    assert desc["part_name"] == "Actual Part Name"
    assert desc["author"] == "Someone"
    assert desc["license"] == "CC0"
    assert coords == []
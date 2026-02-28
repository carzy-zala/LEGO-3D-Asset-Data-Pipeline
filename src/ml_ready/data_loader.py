# data_loader.py — shows ML readiness of your pipeline output
import json
import numpy as np
from pathlib import Path
from config import GOLD_DIR, PARTS_TO_PROCESS

def load_part_as_point_cloud(part_id: str) -> np.ndarray:
    """
    Loads a gold JSON and converts geometry coordinates
    into a numpy point cloud array ready for ML training.

    Returns:
        numpy array of shape (N, 3) — N vertices with x, y, z
    """
    path = GOLD_DIR / "parts" / f"{part_id}.json"

    with open(path) as f:
        part = json.load(f)

    points = []
    for face in part["geometry"]["coordinates"]:
        for vertex in face["vertices"]:
            points.append([vertex["x"], vertex["y"], vertex["z"]])

    return np.array(points)


if __name__ == "__main__":
    for part_id in PARTS_TO_PROCESS:
        point_cloud = load_part_as_point_cloud(part_id)
        print(f"{part_id} → {point_cloud.shape} points")
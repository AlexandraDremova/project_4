from __future__ import annotations

from typing import Tuple

def geo_to_hex(lat: float, lon: float, resolution: float = 0.01) -> Tuple[int, int]:

    row = int(lat / resolution)
    col = int(lon / resolution)
    return row, col


def hex_id_str(lat: float, lon: float, resolution: float = 0.01) -> str:
    r, c = geo_to_hex(lat, lon, resolution=resolution)
    return f"{r}_{c}"

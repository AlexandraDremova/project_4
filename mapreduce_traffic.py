from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from hex_grid import hex_id_str


@dataclass
class SpeedAggregate:
    count: int = 0
    sum_speed: float = 0.0

    @property
    def avg_speed(self) -> float:
        if self.count == 0:
            return 0.0
        return self.sum_speed / self.count


def _time_bucket(timestamp: float, window_seconds: int = 300) -> int:
    return int(timestamp // window_seconds)


def map_reduce_sstable_dir(
    sstable_dir: str | Path,
    output_csv: str | Path,
    window_seconds: int = 300,
    resolution: float = 0.01,
) -> None:
    sstable_dir = Path(sstable_dir)
    output_csv = Path(output_csv)

    aggregates: Dict[Tuple[str, int], SpeedAggregate] = defaultdict(SpeedAggregate)

    for path in sstable_dir.glob('sstable_*.csv'):
        with path.open(encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                lat = float(row['lat'])
                lon = float(row['lon'])
                speed = float(row['speed'])
                ts = float(row['timestamp'])

                hex_id = hex_id_str(lat, lon, resolution=resolution)
                bucket = _time_bucket(ts, window_seconds=window_seconds)

                key = (hex_id, bucket)
                agg = aggregates[key]
                agg.count += 1
                agg.sum_speed += speed

    with output_csv.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['hex_id', 'time_bucket', 'avg_speed', 'count'])
        for (hex_id, bucket), agg in sorted(aggregates.items()):
            writer.writerow([hex_id, bucket, f"{agg.avg_speed:.3f}", agg.count])
          

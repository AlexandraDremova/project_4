from __future__ import annotations

import csv
import json
import os
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Tuple


@dataclass
class TelemetryRecord:
    vehicle_id: str
    timestamp: float  # unix time (seconds)
    lat: float
    lon: float
    speed: float


class WALWriter:
#Каждое событие телеметрии сначала пишется сюда, потом в in-memory буфер

    def __init__(self, wal_path: str | os.PathLike):
        self.path = Path(wal_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # открываем в append режиме:
        self._fh = self.path.open('a', encoding='utf-8')

    def append(self, record: TelemetryRecord) -> None:
        line = json.dumps(asdict(record), ensure_ascii=False)
        self._fh.write(line + '')
        self._fh.flush()

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass


class SSTableWriter:
    #Файл отсортирован по (vehicle_id, timestamp), что позволяет делать бинарный поиск

    def __init__(self, directory: str | os.PathLike):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)

    def write_sstable(self, records: List[TelemetryRecord]) -> Path:
        if not records:
            raise ValueError('No records to flush')

        # Сортируем по (vehicle_id, timestamp)
        records_sorted = sorted(records, key=lambda r: (r.vehicle_id, r.timestamp))

        ts = int(time.time() * 1000)
        path = self.dir / f'sstable_{ts}.csv'

        with path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['vehicle_id', 'timestamp', 'lat', 'lon', 'speed'])
            for r in records_sorted:
                writer.writerow([r.vehicle_id, f"{r.timestamp:.3f}", f"{r.lat:.6f}", f"{r.lon:.6f}", f"{r.speed:.3f}"])

        return path


class TelemetryBuffer:
    def __init__(
        self,
        wal_path: str | os.PathLike,
        sstable_dir: str | os.PathLike,
        buffer_seconds: int = 15,
        max_records: int = 100_000,
    ) -> None:
        self.wal = WALWriter(wal_path)
        self.sstable_writer = SSTableWriter(sstable_dir)
        self.buffer_seconds = buffer_seconds
        self.max_records = max_records
        self._records: List[TelemetryRecord] = []
        self._last_flush_time = time.time()

    def add_record(self, record: TelemetryRecord) -> None:
        # сначала WAL для durability:
        self.wal.append(record)
        # потом — in-memory буфер:
        self._records.append(record)

        now = time.time()
        if (
            now - self._last_flush_time >= self.buffer_seconds
            or len(self._records) >= self.max_records
        ):
            self.flush()

    def flush(self) -> Path | None:
        if not self._records:
            return None
        path = self.sstable_writer.write_sstable(self._records)
        self._records.clear()
        self._last_flush_time = time.time()
        return path

    def close(self) -> None:
        self.flush()
        self.wal.close()
      

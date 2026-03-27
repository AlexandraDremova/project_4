from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Iterator, Any


@dataclass
class TrafficEvent:
    timestamp: float
    event_type: str
    lat: float
    lon: float
    description: str = ""


class EventLogPartition:


    def __init__(self, path: str | os.PathLike):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open('a+b')

    def append(self, event: TrafficEvent) -> int:
        data = (json.dumps(asdict(event), ensure_ascii=False) + " ").encode('utf-8')
        self._fh.seek(0, os.SEEK_END)
        offset = self._fh.tell()
        self._fh.write(data)
        self._fh.flush()
        return offset

    def read_from(self, offset: int, max_events: int = 100) -> Iterator[tuple[int, TrafficEvent]]:
        with self.path.open('rb') as f:
            f.seek(offset)
            count = 0
            while count < max_events:
                line = f.readline()
                if not line:
                    break
                decoded = json.loads(line.decode('utf-8'))
                yield f.tell(), TrafficEvent(**decoded)
                count += 1


class EventLogBroker:
    #мини-брокер событий с партиционированием по ключу:
    def __init__(self, base_dir: str | os.PathLike, num_partitions: int = 4):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.num_partitions = num_partitions
        self.partitions: Dict[int, EventLogPartition] = {}
        for pid in range(num_partitions):
            path = self.base_dir / f'partition_{pid}.log'
            self.partitions[pid] = EventLogPartition(path)

    def _partition_for_key(self, key: int) -> EventLogPartition:
        pid = key % self.num_partitions
        return self.partitions[pid]

    def publish(self, key: int, event: TrafficEvent) -> tuple[int, int]:
        #возвращает (partition_id, offset)

        pid = key % self.num_partitions
        partition = self.partitions[pid]
        offset = partition.append(event)
        return pid, offset

    def subscribe(self, partition_id: int, offset: int = 0, max_events: int = 100) -> List[tuple[int, TrafficEvent]]:
        partition = self.partitions[partition_id]
        return list(partition.read_from(offset, max_events=max_events))
      from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Iterator, Any


@dataclass
class TrafficEvent:
    timestamp: float
    event_type: str
    lat: float
    lon: float
    description: str = ""


class EventLogPartition:


    def __init__(self, path: str | os.PathLike):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open('a+b')

    def append(self, event: TrafficEvent) -> int:
        data = (json.dumps(asdict(event), ensure_ascii=False) + " ").encode('utf-8')
        self._fh.seek(0, os.SEEK_END)
        offset = self._fh.tell()
        self._fh.write(data)
        self._fh.flush()
        return offset

    def read_from(self, offset: int, max_events: int = 100) -> Iterator[tuple[int, TrafficEvent]]:
        with self.path.open('rb') as f:
            f.seek(offset)
            count = 0
            while count < max_events:
                line = f.readline()
                if not line:
                    break
                decoded = json.loads(line.decode('utf-8'))
                yield f.tell(), TrafficEvent(**decoded)
                count += 1


class EventLogBroker:
    #мини-брокер событий с партиционированием по ключу:
    def __init__(self, base_dir: str | os.PathLike, num_partitions: int = 4):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.num_partitions = num_partitions
        self.partitions: Dict[int, EventLogPartition] = {}
        for pid in range(num_partitions):
            path = self.base_dir / f'partition_{pid}.log'
            self.partitions[pid] = EventLogPartition(path)

    def _partition_for_key(self, key: int) -> EventLogPartition:
        pid = key % self.num_partitions
        return self.partitions[pid]

    def publish(self, key: int, event: TrafficEvent) -> tuple[int, int]:
        #возвращает (partition_id, offset)

        pid = key % self.num_partitions
        partition = self.partitions[pid]
        offset = partition.append(event)
        return pid, offset

    def subscribe(self, partition_id: int, offset: int = 0, max_events: int = 100) -> List[tuple[int, TrafficEvent]]:
        partition = self.partitions[partition_id]
        return list(partition.read_from(offset, max_events=max_events))

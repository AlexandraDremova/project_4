from __future__ import annotations

import random
import time
from pathlib import Path

from event_log import EventLogBroker, TrafficEvent
from mapreduce_traffic import map_reduce_sstable_dir
from poi_index import InvertedIndex, POI
from telemetry_buffer import TelemetryBuffer, TelemetryRecord


BASE_DIR = Path('demo_data')
WAL_PATH = BASE_DIR / 'wal.log'
SSTABLE_DIR = BASE_DIR / 'sstables'
EVENT_LOG_DIR = BASE_DIR / 'events'


def demo_telemetry_and_sstable() -> None:
    print('DEMO: telemetry buffer + SSTable flush')
    buf = TelemetryBuffer(
        wal_path=WAL_PATH,
        sstable_dir=SSTABLE_DIR,
        buffer_seconds=2,
        max_records=50,
    )

    start_ts = time.time()
    for i in range(200):
        vehicle_id = f'car_{i % 50}'
        ts = start_ts + i * 0.1
        lat = 40.0 + random.random() * 0.1
        lon = -73.9 + random.random() * 0.1
        speed = random.uniform(0, 60)
        rec = TelemetryRecord(vehicle_id=vehicle_id, timestamp=ts, lat=lat, lon=lon, speed=speed)
        buf.add_record(rec)
        time.sleep(0.01)

    buf.close()
    print('SSTable files written to', SSTABLE_DIR)


def demo_mapreduce() -> None:
    print('DEMO: MapReduce heatmap')
    output = BASE_DIR / 'heatmap.csv'
    map_reduce_sstable_dir(SSTABLE_DIR, output_csv=output)
    print('Heatmap written to', output)


def demo_poi_index() -> None:
    print('DEMO: POI inverted index')
    idx = InvertedIndex()
    pois = [
        POI(1, 'АЗС Лукойл', 'gas_station', 43.6, 39.7),
        POI(2, 'Городская больница №1', 'hospital', 43.6, 39.75),
        POI(3, 'Кафе у моря', 'cafe', 43.59, 39.72),
    ]
    for p in pois:
        idx.add_poi(p)

    res = idx.search('АЗС')
    print('Search "АЗС":', [p.name for p in res])
    res = idx.search('больница')
    print('Search "больница":', [p.name for p in res])


def demo_event_log() -> None:
    print('DEMO: event log with partitions')
    broker = EventLogBroker(EVENT_LOG_DIR, num_partitions=4)

    # публикуем несколько событий в разные зоны (partition_key = zone_id):
    events = [
        (10, TrafficEvent(time.time(), 'ACCIDENT', 43.6, 39.7, 'Столкновение в правом ряду')),
        (11, TrafficEvent(time.time(), 'ROAD_CLOSED', 43.61, 39.71, 'Перекрытие из-за ремонта')),
        (22, TrafficEvent(time.time(), 'ACCIDENT', 43.62, 39.72, 'Столкновение в левом ряду')),
    ]
    for zone_id, ev in events:
        pid, offset = broker.publish(zone_id, ev)
        print(f'Published to partition {pid}, offset {offset}')

    print('Read from partition 0:')
    for off, ev in broker.subscribe(0, offset=0):
        print(off, ev)


if __name__ == '__main__':
    BASE_DIR.mkdir(exist_ok=True)
    demo_telemetry_and_sstable()
    demo_mapreduce()
    demo_poi_index()
    demo_event_log()

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Set


TOKEN_RE = re.compile(r"[a-zA-Zа-яА-Я0-9]+", re.UNICODE)


@dataclass
class POI:
    poi_id: int
    name: str
    poi_type: str
    lat: float
    lon: float


class InvertedIndex:
    def __init__(self) -> None:
        self._postings: Dict[str, List[int]] = {}
        self._poi_by_id: Dict[int, POI] = {}

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        text = text.lower()
        return TOKEN_RE.findall(text)

    def add_poi(self, poi: POI) -> None:
        if poi.poi_id in self._poi_by_id:
            raise ValueError(f"POI with id {poi.poi_id} already exists")
        self._poi_by_id[poi.poi_id] = poi

        terms: Set[str] = set()
        for field in (poi.name, poi.poi_type):
            for tok in self._tokenize(field):
                terms.add(tok)

        for term in terms:
            postings = self._postings.setdefault(term, [])
            # вставка с сохранением сортировки по poi_id:
            if not postings or postings[-1] < poi.poi_id:
                postings.append(poi.poi_id)
            else:
                for i, existing in enumerate(postings):
                    if poi.poi_id < existing:
                        postings.insert(i, poi.poi_id)
                        break

    def search(self, query: str) -> List[POI]:
        tokens = [t for t in self._tokenize(query) if t]
        if not tokens:
            return []

        postings_lists: List[List[int]] = []
        for term in tokens:
            postings = self._postings.get(term)
            if not postings:
                return []
            postings_lists.append(postings)

        # пересечение отсортированных списков:
        result_ids = set(postings_lists[0])
        for pl in postings_lists[1:]:
            result_ids &= set(pl)

        return [self._poi_by_id[pid] for pid in sorted(result_ids)]


def load_poi_from_csv(path: str | Path, index: InvertedIndex) -> None:
    path = Path(path)
    with path.open(encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            poi = POI(
                poi_id=int(row['poi_id']),
                name=row['name'],
                poi_type=row.get('type', ''),
                lat=float(row['lat']),
                lon=float(row['lon']),
            )
            index.add_poi(poi)
          

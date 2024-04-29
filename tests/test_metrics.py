import dataclasses
import functools
import json
import os
from typing import Union

import pytest
from sqlalchemy import select

from src.db import build_connection_url, create_engine
from src.models import AgeMetrics, GenderMetrics, GeoMetrics, Metrics

Number = Union[int, float]


@functools.lru_cache(maxsize=None)
def num_sim(n1: Number, n2: Number) -> Number:
    if n1 == 0 and n2 == 0:
        return 1
    return 1 - abs(n1 - n2) / (n1 + n2)


@dataclasses.dataclass
class TargetMetrics:
    campaign_id: int
    impressions: Number
    cost: Number
    views: Number
    start_date: str
    end_date: str


project_dir = os.path.dirname(os.path.dirname(__file__))
os.environ["PROJECT_DIR"] = project_dir

tests_dir = os.path.join(project_dir, "tests")

connection_url = build_connection_url()
engine = create_engine(connection_url, echo=False)

with open(os.path.join(tests_dir, "data", "target_metrics.json")) as f:
    target_metrics = [TargetMetrics(**row) for row in json.load(f)]


@pytest.mark.parametrize(
    "target, table",
    [
        (target, table)
        for target in target_metrics
        for table in [Metrics, GenderMetrics, AgeMetrics, GeoMetrics]
    ],
)
def test_metrics(
    target: TargetMetrics, table: Union[Metrics, GenderMetrics, AgeMetrics, GeoMetrics]
):
    with engine.begin() as connection:
        rows = connection.execute(
            select(
                table.campaign_id,
                table.impressions,
                table.cost_micros,
                table.cost,
                table.video_views,
            ).where(
                table.campaign_id == target.campaign_id,
                table.date >= target.start_date,
                table.date <= target.end_date,
            )
        ).fetchall()

        metrics = {
            "impressions": (target.impressions, sum(row.impressions for row in rows)),
            "cost_micros": (target.cost, sum(row[2] for row in rows) / 1_000_000),
            "cost": (target.cost, sum(row[3] for row in rows)),
            "video_views": (target.views, sum(row[4] for row in rows)),
        }

        for name, (target_value, total_value) in metrics.items():
            similarity = num_sim(target_value, total_value)
            assert round(similarity, 2) == 1.0, {
                "name": name,
                "campaign_id": target.campaign_id,
                "target": target_value,
                name: total_value,
            }

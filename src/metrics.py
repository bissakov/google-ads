import ctypes
import dataclasses
import logging
from datetime import date as dt
from datetime import datetime
from enum import Enum
from typing import Any, Callable, List, Mapping, Sequence, Union

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.v16.services.types.google_ads_service import GoogleAdsRow

from src.account import GAccount
from src.error_handler import handle_google_ads_exception


class GMetricsType(Enum):
    GENERAL = "GGeneralMetrics"
    GENDER = "GGenderMetrics"
    AGE = "GAgeMetrics"
    GEO = "GGeoMetrics"


class GMetricsFactory:
    @staticmethod
    def create_metrics(metric_type: GMetricsType, **kwargs):
        if metric_type == GMetricsType.GENERAL:
            return GGeneralMetrics(**kwargs)
        elif metric_type == GMetricsType.GENDER:
            return GGenderMetrics(**kwargs)
        elif metric_type == GMetricsType.AGE:
            return GAgeMetrics(**kwargs)
        elif metric_type == GMetricsType.GEO:
            return GGeoMetrics(**kwargs)
        else:
            raise ValueError(f"Invalid metric type: {metric_type}")


@dataclasses.dataclass
class GMetrics:
    average_cpv: float
    average_cpm: float
    cost_micros: int
    cost: float
    impressions: int
    interactions: int
    interaction_rate: float
    average_cost: float
    conversions: float
    cost_per_conversion: float
    conversions_from_interactions_rate: float
    clicks: int
    video_views: int
    video_view_rate: float
    ctr: float

    def __iter__(self):
        return iter(dataclasses.astuple(self))

    def to_dict(self) -> Mapping[str, Any]:
        self_dict = dataclasses.asdict(self)
        if "date" in self_dict and isinstance(self_dict["date"], dt):
            self_dict["date"] = self_dict["date"].isoformat()
        return self_dict


@dataclasses.dataclass
class GGeneralMetrics(GMetrics):
    id: int
    campaign_id: int
    device: str
    date: Union[dt, str]
    engagement_rate: float

    def __hash__(self):
        return hash(self.id)

    def __post_init__(self):
        if isinstance(self.date, str):
            self.date = datetime.fromisoformat(self.date).date()


@dataclasses.dataclass
class GGenderMetrics(GMetrics):
    id: int
    campaign_id: int
    ad_group_id: int
    gender: str
    device: str
    date: Union[dt, str]
    engagement_rate: float

    def __hash__(self):
        return hash(self.id)

    def __post_init__(self):
        if isinstance(self.date, str):
            self.date = datetime.fromisoformat(self.date).date()


@dataclasses.dataclass
class GAgeMetrics(GMetrics):
    id: int
    campaign_id: int
    ad_group_id: int
    age_range: str
    device: str
    date: Union[dt, str]
    engagement_rate: float

    def __hash__(self):
        return hash(self.id)

    def __post_init__(self):
        if isinstance(self.date, str):
            self.date = datetime.fromisoformat(self.date).date()


@dataclasses.dataclass
class GGeoMetrics(GMetrics):
    id: int
    campaign_id: int
    country_id: int
    device: str
    date: Union[dt, str]

    def __hash__(self):
        return hash(self.id)

    def __post_init__(self):
        if isinstance(self.date, str):
            self.date = datetime.fromisoformat(self.date).date()


Metrics = Union[GGeneralMetrics, GGenderMetrics, GAgeMetrics, GGeoMetrics]


def handle_shared_metrics(row: GoogleAdsRow) -> Mapping[str, Any]:
    cost_micros = row.metrics.cost_micros
    cost = round(cost_micros / 1_000_000, 2)

    return {
        "average_cpv": row.metrics.average_cpv,
        "average_cpm": row.metrics.average_cpm,
        "cost_micros": cost_micros,
        "cost": cost,
        "impressions": row.metrics.impressions,
        "interactions": row.metrics.interactions,
        "interaction_rate": row.metrics.interaction_rate,
        "average_cost": row.metrics.average_cost,
        "conversions": row.metrics.conversions,
        "cost_per_conversion": row.metrics.cost_per_conversion,
        "conversions_from_interactions_rate": row.metrics.conversions_from_interactions_rate,
        "clicks": row.metrics.clicks,
        "video_views": row.metrics.video_views,
        "video_view_rate": row.metrics.video_view_rate,
        "ctr": row.metrics.ctr,
    }


def handle_general_metrics(row: GoogleAdsRow) -> GGeneralMetrics:
    campaign_id = row.campaign.id
    device = row.segments.device.name
    date = datetime.strptime(row.segments.date, "%Y-%m-%d").date()
    metrics_id = ctypes.c_uint32(hash(f"{campaign_id}_{device}_{date}")).value

    shared_metrics = handle_shared_metrics(row)

    return GGeneralMetrics(
        id=metrics_id,
        campaign_id=campaign_id,
        device=device,
        date=date,
        engagement_rate=row.metrics.engagement_rate,
        **shared_metrics,
    )


def handle_gender_metrics(row: GoogleAdsRow) -> GGenderMetrics:
    campaign_id = row.campaign.id
    ad_group_id = row.ad_group.id
    device = row.segments.device.name
    date = datetime.strptime(row.segments.date, "%Y-%m-%d").date()
    gender = row.ad_group_criterion.gender.type_.name
    gender_metrics_id = ctypes.c_uint32(
        hash(f"{campaign_id}_{ad_group_id}_{device}_{date}_{gender}")
    ).value

    shared_metrics = handle_shared_metrics(row)

    return GGenderMetrics(
        id=gender_metrics_id,
        campaign_id=campaign_id,
        device=device,
        ad_group_id=ad_group_id,
        gender=gender,
        date=date,
        engagement_rate=row.metrics.engagement_rate,
        **shared_metrics,
    )


def handle_age_metrics(row: GoogleAdsRow) -> GAgeMetrics:
    campaign_id = row.campaign.id
    ad_group_id = row.ad_group.id
    device = row.segments.device.name
    date = datetime.strptime(row.segments.date, "%Y-%m-%d").date()
    age_range = row.ad_group_criterion.age_range.type_.name
    age_metrics_id = ctypes.c_uint32(
        hash(f"{campaign_id}_{ad_group_id}_{device}_{date}_{age_range}")
    ).value

    shared_metrics = handle_shared_metrics(row)

    return GAgeMetrics(
        id=age_metrics_id,
        campaign_id=campaign_id,
        ad_group_id=ad_group_id,
        age_range=age_range,
        device=device,
        date=date,
        engagement_rate=row.metrics.engagement_rate,
        **shared_metrics,
    )


def handle_geo_metrics(row: GoogleAdsRow) -> GGeoMetrics:
    campaign_id = row.campaign.id
    date = row.segments.date
    country_id = row.geographic_view.country_criterion_id
    device = row.segments.device.name
    geo_metrics_id = ctypes.c_uint32(
        hash(f"{campaign_id}_{device}_{date}_{country_id}")
    ).value

    shared_metrics = handle_shared_metrics(row)

    return GGeoMetrics(
        id=geo_metrics_id,
        campaign_id=campaign_id,
        country_id=country_id,
        date=date,
        device=device,
        **shared_metrics,
    )


@handle_google_ads_exception
def _fetch_metrics(
    client: GoogleAdsClient,
    ga_query: str,
    accounts: List[GAccount],
    handle_metrics: Callable[[GoogleAdsRow], Metrics],
) -> List[Metrics]:
    service = client.get_service("GoogleAdsService")

    metrics: List[Metrics] = []

    for account in accounts:
        if account.manager:
            continue

        account_id = account.id
        stream = service.search_stream(customer_id=str(account_id), query=ga_query)

        metrics.extend(
            [handle_metrics(row) for batch in stream for row in batch.results]
        )

    return metrics


def generate_query(
    fields: List[str], table: str, condition: str, order_by: List[str]
) -> str:
    metrics = [
        "metrics.average_cpv",
        "metrics.average_cpm",
        "metrics.cost_micros",
        "metrics.impressions",
        "metrics.interactions",
        "metrics.interaction_rate",
        "metrics.average_cost",
        "metrics.conversions",
        "metrics.cost_per_conversion",
        "metrics.conversions_from_interactions_rate",
        "metrics.clicks",
        "metrics.video_views",
        "metrics.video_view_rate",
        "metrics.ctr",
    ]

    if table != "geographic_view":
        fields.append("metrics.engagement_rate")

    select_fields = ", ".join(fields + metrics)
    order_by_stmts = ", ".join(order_by)

    query = (
        f"SELECT {select_fields} "
        f"FROM {table} "
        f"WHERE {condition} "
        f"ORDER BY {order_by_stmts}"
    )

    return query


def fetch_general_metrics(
    client: GoogleAdsClient, accounts: List[GAccount], condition: str
) -> List[GGeneralMetrics]:
    logging.info("Fetching general metrics...")

    general_query = generate_query(
        fields=["campaign.id", "segments.date", "segments.device"],
        table="campaign",
        condition=condition,
        order_by=["segments.date ASC"],
    )

    logging.debug(f"General query: {general_query}")

    general_metrics = _fetch_metrics(
        client, general_query, accounts, handle_general_metrics
    )

    logging.info("General metrics fetched.")

    return general_metrics


def fetch_gender_metrics(
    client: GoogleAdsClient, accounts: List[GAccount], condition: str
) -> List[GGenderMetrics]:
    gender_query = generate_query(
        fields=[
            "gender_view.resource_name",
            "campaign.id",
            "ad_group.id",
            "ad_group_criterion.gender.type",
            "segments.date",
            "segments.device",
        ],
        table="gender_view",
        condition=condition,
        order_by=["segments.date ASC"],
    )

    gender_metrics = _fetch_metrics(
        client, gender_query, accounts, handle_gender_metrics
    )

    return gender_metrics


def fetch_age_metrics(
    client: GoogleAdsClient, accounts: List[GAccount], condition: str
) -> List[GAgeMetrics]:
    age_query = generate_query(
        fields=[
            "age_range_view.resource_name",
            "campaign.id",
            "ad_group.id",
            "ad_group_criterion.age_range.type",
            "segments.date",
            "segments.device",
        ],
        table="age_range_view",
        condition=condition,
        order_by=["segments.date ASC"],
    )

    age_metrics = _fetch_metrics(client, age_query, accounts, handle_age_metrics)

    return age_metrics


def fetch_geo_metrics(
    client: GoogleAdsClient, accounts: List[GAccount], condition: str
) -> List[GGeoMetrics]:
    geo_query = generate_query(
        fields=[
            "geographic_view.country_criterion_id",
            "campaign.id",
            "segments.date",
            "segments.device",
        ],
        table="geographic_view",
        condition=condition,
        order_by=["segments.date ASC"],
    )

    geo_metrics = _fetch_metrics(client, geo_query, accounts, handle_geo_metrics)

    return geo_metrics


def fetch_metrics(
    g_metrics_type: GMetricsType,
    client: GoogleAdsClient,
    accounts: List[GAccount],
    condition: str,
) -> Sequence[Union[GGeneralMetrics, GGenderMetrics, GAgeMetrics, GGeoMetrics]]:
    metrics: Sequence[Union[GGeneralMetrics, GGenderMetrics, GAgeMetrics, GGeoMetrics]]
    if g_metrics_type == GMetricsType.GENERAL:
        metrics = fetch_general_metrics(client, accounts, condition)
    elif g_metrics_type == GMetricsType.GENDER:
        metrics = fetch_gender_metrics(client, accounts, condition)
    elif g_metrics_type == GMetricsType.AGE:
        metrics = fetch_age_metrics(client, accounts, condition)
    elif g_metrics_type == GMetricsType.GEO:
        metrics = fetch_geo_metrics(client, accounts, condition)
    else:
        raise ValueError(f"Invalid metrics type: {g_metrics_type}")

    return metrics


#  INFO: Don't use this function in production. It's only for testing purposes.
def main() -> None:
    import json
    import os

    import rich

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.environ["PROJECT_DIR"] = project_dir
    logging.info(f"PROJECT_DIR: {project_dir}")

    client = GoogleAdsClient.load_from_storage(
        os.path.join(project_dir, "google-ads.yaml")
    )

    with open(r"D:\Work\google_ads\reports\2024-04\accounts.json", "r") as f:
        accounts = [GAccount(**account) for account in json.load(f)]

    metrics = fetch_metrics(
        GMetricsType.GENERAL,
        client,
        accounts,
        "segments.date DURING TODAY",
    )

    rich.print(metrics)


if __name__ == "__main__":
    main()

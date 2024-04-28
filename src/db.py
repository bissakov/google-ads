import dataclasses
import json
import logging
import os
from typing import Union

from sqlalchemy import create_engine as _create_engine
from sqlalchemy import insert, select, update
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql import Insert, Update

from src.account import GAccount
from src.ad import GAd
from src.ad_group import GAdGroup
from src.campaign import GCampaign
from src.metrics import GMetricsFactory, GMetricsType
from src.models import Account, Ad, AdGroup, Base, Campaign, MetricsFactory, MetricsType
from src.utils import batched

logger = logging.getLogger(__name__)
BATCH_SIZE = 2000


def build_connection_url() -> str:
    db_type = os.getenv("DB_TYPE")
    assert db_type in ["SQL Server", "SQLite"], "Invalid DB_TYPE."

    if db_type == "SQL Server":
        logger.warning("Production mode")

        user = os.getenv("UID")
        password = os.getenv("PWD")
        host = os.getenv("SERVER")
        port = os.getenv("PORT")
        database = os.getenv("DATABASE")
        driver = os.getenv("DRIVER")

        assert all(
            [user, password, host, port, database, driver]
        ), "Missing environment variables."

        connection_url = "mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver={driver}".format(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database,
            driver=driver,
        )
    else:
        logger.debug("Development mode")

        project_dir = os.getenv("PROJECT_DIR")
        assert project_dir, "Missing PROJECT_DIR environment variable."

        db_path = os.path.join(project_dir, "google.db")
        connection_url = f"sqlite:///{db_path}"

    logger.debug(f"Connection URL: {connection_url}")
    return connection_url


def create_engine(connection_url: str, echo: bool) -> Engine:
    if "sqlite" in connection_url:
        engine = _create_engine(connection_url, future=True, echo=echo)
    else:
        engine = _create_engine(
            connection_url, future=True, use_setinputsizes=False, echo=echo
        )
    return engine


def insert_accounts(current_day_report_dir: str, engine: Engine) -> None:
    accounts_json = os.path.join(current_day_report_dir, "accounts.json")
    with open(accounts_json, "r") as file:
        accounts = iter(GAccount(**account) for account in json.load(file))

    with engine.begin() as connection:
        for record in accounts:
            account = Account(**dataclasses.asdict(record))

            logger.debug(f"Inserting account: {account}")

            stmt: Union[Insert, Update]
            if connection.execute(
                select(Account.id).where(Account.id == account.id)
            ).fetchone():
                stmt = (
                    update(Account)
                    .where(Account.id == account.id)
                    .values(**account.to_dict(exclude_cols=["id"]))
                )
            else:
                stmt = insert(Account).values(**account.to_dict())
            connection.execute(stmt)


def insert_campaigns(current_day_report_dir: str, engine: Engine) -> None:
    campaigns_json = os.path.join(current_day_report_dir, "campaigns.json")
    with open(campaigns_json, "r") as file:
        campaigns = iter(GCampaign(**campaign) for campaign in json.load(file))

    with engine.begin() as connection:
        for record in campaigns:
            campaign = Campaign(**dataclasses.asdict(record))
            logger.debug(f"Inserting campaign: {campaign}")

            stmt: Union[Insert, Update]
            if connection.execute(
                select(Campaign.id).where(Campaign.id == campaign.id)
            ).fetchone():
                stmt = (
                    update(Campaign)
                    .where(Campaign.id == campaign.id)
                    .values(**campaign.to_dict(exclude_cols=["id"]))
                )
            else:
                stmt = insert(Campaign).values(**campaign.to_dict())
            connection.execute(stmt)


def insert_ad_groups(current_day_report_dir: str, engine: Engine) -> None:
    ad_groups_json = os.path.join(current_day_report_dir, "ad_groups.json")
    with open(ad_groups_json, "r") as file:
        ad_groups = iter(GAdGroup(**ad_group) for ad_group in json.load(file))

    with engine.begin() as connection:
        for record in ad_groups:
            ad_group = AdGroup(**dataclasses.asdict(record))
            logger.debug(f"Inserting ad_group: {ad_group}")

            stmt: Union[Insert, Update]
            if connection.execute(
                select(AdGroup.id).where(AdGroup.id == ad_group.id)
            ).fetchone():
                stmt = (
                    update(AdGroup)
                    .where(AdGroup.id == ad_group.id)
                    .values(**ad_group.to_dict(exclude_cols=["id"]))
                )
            else:
                stmt = insert(AdGroup).values(**ad_group.to_dict())
            connection.execute(stmt)


def insert_ads(current_day_report_dir: str, engine: Engine) -> None:
    ads_json = os.path.join(current_day_report_dir, "ads.json")
    with open(ads_json, "r") as file:
        ads = iter(GAd(**ad) for ad in json.load(file))

    with engine.begin() as connection:
        for record in ads:
            ad = Ad(**dataclasses.asdict(record))
            logger.debug(f"Inserting ad: {ad}")

            stmt: Union[Insert, Update]
            if connection.execute(select(Ad.id).where(Ad.id == ad.id)).fetchone():
                stmt = (
                    update(Ad)
                    .where(Ad.id == ad.id)
                    .values(**ad.to_dict(exclude_cols=["id"]))
                )
            else:
                stmt = insert(Ad).values(**ad.to_dict())
            connection.execute(stmt)


def insert_metrics(
    json_report: str,
    engine: Engine,
    g_metrics_type: GMetricsType,
    db_metrics_type: MetricsType,
) -> None:
    with open(json_report, "r") as file:
        g_metrics = [
            GMetricsFactory.create_metrics(g_metrics_type, **metric)
            for metric in json.load(file)
        ]

    DBMetrics = MetricsFactory.create_metrics(db_metrics_type)

    batch_count = len(g_metrics) // BATCH_SIZE + 1
    for idx, batch in enumerate(batched(g_metrics, BATCH_SIZE), start=1):
        with engine.begin() as connection:
            for record in batch:
                metrics = DBMetrics(**dataclasses.asdict(record))

                stmt: Union[Insert, Update]
                if connection.execute(
                    select(DBMetrics.id).where(DBMetrics.id == metrics.id)
                ).fetchone():
                    stmt = (
                        update(DBMetrics)
                        .where(DBMetrics.id == metrics.id)
                        .values(**metrics.to_dict(exclude_cols=["id"]))
                    )
                else:
                    stmt = insert(DBMetrics).values(**metrics.to_dict())
                connection.execute(stmt)
            logger.debug(
                f"{db_metrics_type.value} - Batch {idx}/{batch_count} inserted."
            )


def populate_db(current_day_report_dir: str) -> None:
    connection_url = build_connection_url()
    engine = create_engine(connection_url, echo=False)

    Base.metadata.create_all(engine)

    logger.info("Starting DB population...")

    insert_accounts(current_day_report_dir, engine)
    insert_campaigns(current_day_report_dir, engine)
    insert_ad_groups(current_day_report_dir, engine)
    insert_ads(current_day_report_dir, engine)

    general_metrics_json = os.path.join(current_day_report_dir, "general_metrics.json")
    gender_metrics_json = os.path.join(current_day_report_dir, "gender_metrics.json")
    age_metrics_json = os.path.join(current_day_report_dir, "age_metrics.json")
    geo_metrics_json = os.path.join(current_day_report_dir, "geo_metrics.json")

    insert_metrics(
        general_metrics_json, engine, GMetricsType.GENERAL, MetricsType.GENERAL
    )
    insert_metrics(gender_metrics_json, engine, GMetricsType.GENDER, MetricsType.GENDER)
    insert_metrics(age_metrics_json, engine, GMetricsType.AGE, MetricsType.AGE)
    insert_metrics(geo_metrics_json, engine, GMetricsType.GEO, MetricsType.GEO)

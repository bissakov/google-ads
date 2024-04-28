import logging
import os
from abc import abstractmethod
from enum import Enum
from typing import Any, Dict

from sqlalchemy import (
    NVARCHAR,
    BigInteger,
    Column,
    Date,
    Float,
    ForeignKey,
    Integer,
    MetaData,
)
from sqlalchemy.orm import DeclarativeBase, relationship

db_type = os.getenv("DB_TYPE")
schema = "dbo" if db_type and db_type == "SQL Server" else None
logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    __abstract__ = True
    metadata = MetaData(schema=schema)

    @property
    @abstractmethod
    def id(self) -> Any:
        pass

    @property
    @abstractmethod
    def name(self) -> Any:
        pass

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return bool(self.id == other.id)

    def __hash__(self) -> int:
        return hash(self.id)

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        if self.name:
            return f'<{class_name} {self.id} "{self.name}">'
        else:
            return f"<{class_name} {self.id}>"

    def to_dict(self) -> Dict[str, Any]:
        class_dict = {}
        for key, value in self.__dict__.items():
            if key.startswith("_"):
                continue
            class_dict[key] = value
        return class_dict


class GeoTarget(Base):
    __tablename__ = "GeoTargets"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(NVARCHAR(128))
    canonical_name = Column(NVARCHAR(128))
    parent_id = Column(BigInteger)
    country_code = Column(NVARCHAR(128))
    target_type = Column(NVARCHAR(128))
    status = Column(NVARCHAR(128))

    geo_metrics = relationship("GeoMetrics", back_populates="geo_target")


class Account(Base):
    __tablename__ = "Accounts"
    id = Column(BigInteger, primary_key=True, index=True)
    name = Column(NVARCHAR(128))
    resource_name = Column(NVARCHAR(128))
    account_customer = Column(NVARCHAR(128))
    manager = Column(Integer)
    currency_code = Column(NVARCHAR(128))
    level = Column(Integer)
    time_zone = Column(NVARCHAR(128))

    campaigns = relationship("Campaign", back_populates="account")
    ad_groups = relationship("AdGroup", back_populates="account")
    ads = relationship("Ad", back_populates="account")


class Campaign(Base):
    __tablename__ = "Campaigns"
    id = Column(BigInteger, primary_key=True, index=True)
    account_id = Column(BigInteger, ForeignKey("Accounts.id"))
    name = Column(NVARCHAR(128))
    resource_name = Column(NVARCHAR(128))
    status = Column(NVARCHAR(128))
    advertising_channel_type = Column(NVARCHAR(128))
    advertising_channel_sub_type = Column(NVARCHAR(128))
    start_date = Column(NVARCHAR(128))
    end_date = Column(NVARCHAR(128))

    account = relationship("Account", back_populates="campaigns")
    ad_groups = relationship("AdGroup", back_populates="campaign")
    ads = relationship("Ad", back_populates="campaign")
    metrics = relationship("Metrics", back_populates="campaign")
    gender_metrics = relationship("GenderMetrics", back_populates="campaign")
    age_metrics = relationship("AgeMetrics", back_populates="campaign")
    geo_metrics = relationship("GeoMetrics", back_populates="campaign")


class AdGroup(Base):
    __tablename__ = "AdGroups"
    id = Column(BigInteger, primary_key=True, index=True)
    campaign_id = Column(BigInteger, ForeignKey("Campaigns.id"))
    account_id = Column(BigInteger, ForeignKey("Accounts.id"))
    name = Column(NVARCHAR(128))
    resource_name = Column(NVARCHAR(128))
    status = Column(NVARCHAR(128))

    campaign = relationship("Campaign", back_populates="ad_groups")
    account = relationship("Account", back_populates="ad_groups")
    ads = relationship("Ad", back_populates="ad_group")
    gender_metrics = relationship("GenderMetrics", back_populates="ad_group")
    age_metrics = relationship("AgeMetrics", back_populates="ad_group")


class Ad(Base):
    __tablename__ = "Ads"
    id = Column(BigInteger, primary_key=True, index=True)
    campaign_id = Column(BigInteger, ForeignKey("Campaigns.id"))
    ad_group_id = Column(BigInteger, ForeignKey("AdGroups.id"))
    account_id = Column(BigInteger, ForeignKey("Accounts.id"))
    name = Column(NVARCHAR(128))
    resource_name = Column(NVARCHAR(128))
    status = Column(NVARCHAR(128))

    campaign = relationship("Campaign", back_populates="ads")
    account = relationship("Account", back_populates="ads")
    ad_group = relationship("AdGroup", back_populates="ads")


class MetricsType(Enum):
    GENERAL = "Metrics"
    GENDER = "GenderMetrics"
    AGE = "AgeMetrics"
    GEO = "GeoMetrics"


class MetricsFactory:
    @staticmethod
    def create_metrics(metrics_type: MetricsType):
        if metrics_type == MetricsType.GENERAL:
            return Metrics
        elif metrics_type == MetricsType.GENDER:
            return GenderMetrics
        elif metrics_type == MetricsType.AGE:
            return AgeMetrics
        elif metrics_type == MetricsType.GEO:
            return GeoMetrics
        else:
            raise ValueError(f"Invalid metrics type: {metrics_type}")


class Metrics(Base):
    __tablename__ = "Metrics"
    id = Column(BigInteger, primary_key=True, index=True)
    campaign_id = Column(BigInteger, ForeignKey("Campaigns.id"))
    device = Column(NVARCHAR(128))
    date = Column(Date)
    average_cpv = Column(Float)
    average_cpm = Column(Float)
    cost_micros = Column(Integer)
    cost = Column(Float)
    impressions = Column(Integer)
    interactions = Column(Integer)
    interaction_rate = Column(Float)
    average_cost = Column(Float)
    conversions = Column(Float)
    cost_per_conversion = Column(Float)
    conversions_from_interactions_rate = Column(Float)
    clicks = Column(Integer)
    engagement_rate = Column(Float)
    video_views = Column(Integer)
    video_view_rate = Column(Float)
    ctr = Column(Float)

    campaign = relationship("Campaign", back_populates="metrics")


class GenderMetrics(Base):
    __tablename__ = "GenderMetrics"
    id = Column(BigInteger, primary_key=True, index=True)
    campaign_id = Column(BigInteger, ForeignKey("Campaigns.id"))
    ad_group_id = Column(BigInteger, ForeignKey("AdGroups.id"))
    gender = Column(NVARCHAR(128))
    device = Column(NVARCHAR(128))
    date = Column(Date)
    average_cpv = Column(Float)
    average_cpm = Column(Float)
    cost_micros = Column(Integer)
    cost = Column(Float)
    impressions = Column(Integer)
    interactions = Column(Integer)
    interaction_rate = Column(Float)
    average_cost = Column(Float)
    conversions = Column(Float)
    cost_per_conversion = Column(Float)
    conversions_from_interactions_rate = Column(Float)
    clicks = Column(Integer)
    engagement_rate = Column(Float)
    video_views = Column(Integer)
    video_view_rate = Column(Float)
    ctr = Column(Float)

    campaign = relationship("Campaign", back_populates="gender_metrics")
    ad_group = relationship("AdGroup", back_populates="gender_metrics")


class AgeMetrics(Base):
    __tablename__ = "AgeMetrics"
    id = Column(BigInteger, primary_key=True, index=True)
    campaign_id = Column(BigInteger, ForeignKey("Campaigns.id"))
    ad_group_id = Column(BigInteger, ForeignKey("AdGroups.id"))
    age_range = Column(NVARCHAR(128))
    device = Column(NVARCHAR(128))
    date = Column(Date)
    average_cpv = Column(Float)
    average_cpm = Column(Float)
    cost_micros = Column(Integer)
    cost = Column(Float)
    impressions = Column(Integer)
    interactions = Column(Integer)
    interaction_rate = Column(Float)
    average_cost = Column(Float)
    conversions = Column(Float)
    cost_per_conversion = Column(Float)
    conversions_from_interactions_rate = Column(Float)
    clicks = Column(Integer)
    engagement_rate = Column(Float)
    video_views = Column(Integer)
    video_view_rate = Column(Float)
    ctr = Column(Float)

    campaign = relationship("Campaign", back_populates="age_metrics")
    ad_group = relationship("AdGroup", back_populates="age_metrics")


class GeoMetrics(Base):
    __tablename__ = "GeoMetrics"
    id = Column(BigInteger, primary_key=True, index=True)
    campaign_id = Column(BigInteger, ForeignKey("Campaigns.id"))
    country_id = Column(BigInteger, ForeignKey("GeoTargets.id"))
    device = Column(NVARCHAR(128))
    date = Column(Date)
    average_cpv = Column(Float)
    average_cpm = Column(Float)
    cost_micros = Column(Integer)
    cost = Column(Float)
    impressions = Column(Integer)
    interactions = Column(Integer)
    interaction_rate = Column(Float)
    average_cost = Column(Float)
    conversions = Column(Float)
    cost_per_conversion = Column(Float)
    conversions_from_interactions_rate = Column(Float)
    clicks = Column(Integer)
    video_views = Column(Integer)
    video_view_rate = Column(Float)
    ctr = Column(Float)

    campaign = relationship("Campaign", back_populates="geo_metrics")
    geo_target = relationship("GeoTarget", back_populates="geo_metrics")

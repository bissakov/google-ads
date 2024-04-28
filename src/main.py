import json
import logging
import os
import sys
from dataclasses import asdict
from datetime import datetime
from typing import Any, List, Mapping

import pytz
from google.ads.googleads.client import GoogleAdsClient

from src.account import account_structure
from src.ad import ad_structure
from src.ad_group import ad_group_structure
from src.campaign import campaign_structure
from src.db import populate_db
from src.geo_target import fetch_latest_geo_targets
from src.metrics import GMetricsType, fetch_metrics

if sys.version_info < (3,):
    raise Exception("Python 2 is not supported")


def save(data: List[Mapping[str, Any]], path: str) -> None:
    if os.path.exists(path):
        logging.warning(f"File already exists: {path} - overwriting")
    else:
        logging.info(f"Saving to {path}")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def collect_data(
    client: GoogleAdsClient, root_account_id: str, reports_dir: str
) -> None:
    accounts = account_structure(client, root_account_id)
    logging.info(f"Collected {len(accounts)} accounts")
    save(
        [asdict(account) for account in accounts],
        os.path.join(reports_dir, "accounts.json"),
    )

    account_ids = [account.id for account in accounts]

    campaigns = campaign_structure(client, account_ids)
    logging.info(f"Collected {len(campaigns)} campaigns")
    save(
        [asdict(campaign) for campaign in campaigns],
        os.path.join(reports_dir, "campaigns.json"),
    )

    ad_groups = ad_group_structure(client, account_ids)
    logging.info(f"Collected {len(ad_groups)} ad groups")
    save(
        [asdict(ad_group) for ad_group in ad_groups],
        os.path.join(reports_dir, "ad_groups.json"),
    )

    ads = ad_structure(client, account_ids)
    logging.info(f"Collected {len(ads)} ads")
    save([asdict(ad) for ad in ads], os.path.join(reports_dir, "ads.json"))

    #  NOTE: Change the date range as needed
    condition = "segments.date DURING TODAY"

    general_metrics = fetch_metrics(
        GMetricsType.GENERAL,
        client,
        accounts,
        condition,
    )
    logging.info(f"Collected {len(general_metrics)} general metrics")
    save(
        [metric.to_dict() for metric in general_metrics],
        os.path.join(reports_dir, "general_metrics.json"),
    )

    gender_metrics = fetch_metrics(GMetricsType.GENDER, client, accounts, condition)
    logging.info(f"Collected {len(gender_metrics)} gender metrics")
    save(
        [metric.to_dict() for metric in gender_metrics],
        os.path.join(reports_dir, "gender_metrics.json"),
    )

    age_metrics = fetch_metrics(GMetricsType.AGE, client, accounts, condition)
    logging.info(f"Collected {len(age_metrics)} age metrics")
    save(
        [metric.to_dict() for metric in age_metrics],
        os.path.join(reports_dir, "age_metrics.json"),
    )

    geo_metrics = fetch_metrics(GMetricsType.GEO, client, accounts, condition)
    logging.info(f"Collected {len(geo_metrics)} geo metrics")
    save(
        [metric.to_dict() for metric in geo_metrics],
        os.path.join(reports_dir, "geo_metrics.json"),
    )


def main() -> None:
    logging.info("Starting the main process...")
    today = datetime.now(pytz.timezone("Asia/Almaty"))
    year_month = today.strftime("%Y-%m")

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.environ["PROJECT_DIR"] = project_dir
    logging.info(f"PROJECT_DIR: {project_dir}")

    fetch_latest_geo_targets()

    parent_reports_dir = os.path.join(project_dir, "reports")
    os.makedirs(parent_reports_dir, exist_ok=True)
    logging.info(f"Parent reports directory: {parent_reports_dir}")

    current_day_reports_dir = os.path.join(parent_reports_dir, year_month)
    os.makedirs(current_day_reports_dir, exist_ok=True)
    logging.info(f"Current day reports directory: {current_day_reports_dir}")

    client = GoogleAdsClient.load_from_storage(
        os.path.join(project_dir, "google-ads.yaml")
    )
    logging.info("Google Ads client loaded")
    root_account_id = "4091725735"
    logging.info(f"Root account ID: {root_account_id}")

    collect_data(client, root_account_id, current_day_reports_dir)

    populate_db(current_day_reports_dir)


if __name__ == "__main__":
    main()

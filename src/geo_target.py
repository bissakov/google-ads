import logging
import os
import urllib.parse
import zipfile
from typing import Union

import bs4
import pandas as pd
import requests
from sqlalchemy import insert, select, update
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql import Insert, Update

from src.db import build_connection_url, create_engine
from src.models import GeoTarget
from src.utils import batched


def get_geo_targets_zip_url(base_url: str) -> str:
    logging.info("Getting geo targets zip URL...")

    doc_endpoint = "/google-ads/api/data/geotargets"
    url = urllib.parse.urljoin(base_url, doc_endpoint)
    response = requests.get(url)
    response.raise_for_status()

    strainer = bs4.SoupStrainer(
        name="div", attrs={"class": lambda cl: "devsite-article-body" in cl.split()}
    )
    soup = bs4.BeautifulSoup(response.text, "html.parser", parse_only=strainer)
    assert len(soup) > 0, "No content found"

    anchor_parent = soup.find("h2", attrs={"id": "download_csv_of_geo_targets"})
    assert anchor_parent is not None, "Anchor parent not found"

    anchor = anchor_parent.find_next("a")
    assert anchor is not None, "Anchor not found"
    assert isinstance(anchor, bs4.Tag), "Anchor is not a tag"

    rel_download_url = anchor.get("href")
    assert rel_download_url is not None, "Download URL not found"
    assert isinstance(rel_download_url, str), "Download URL is not a string"

    download_url = urllib.parse.urljoin(base_url, rel_download_url)

    logging.info(f"Geo targets zip URL: {download_url}")
    return download_url


def download_geo_targets_zip(download_url: str, output_path: str) -> None:
    logging.info("Downloading geo targets zip...")

    response = requests.get(download_url)
    response.raise_for_status()

    if os.path.exists(output_path):
        logging.warning(f"File already exists: {output_path} - overwriting")

    with open(output_path, "wb") as f:
        f.write(response.content)

    logging.info(f"Geo targets zip saved to {output_path}")


def extract_geo_targets_zip(zip_file_path: str, output_dir: str) -> None:
    logging.info("Extracting geo targets zip...")

    if not os.path.exists(output_dir):
        logging.warning(f"Output directory does not exist: {output_dir} - creating")
        os.makedirs(output_dir)

    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(output_dir)

    logging.info(f"Geo targets extracted to {output_dir}")


def parse_csv(resources_dir: str) -> pd.DataFrame:
    logging.info("Parsing geo targets CSV...")

    csv_file_path = next(
        (
            os.path.join(resources_dir, f)
            for f in os.listdir(resources_dir)
            if f.endswith(".csv")
        ),
        None,
    )
    assert csv_file_path is not None, "CSV file not found"

    logging.info(f"Latest geo targets fetched and saved to {csv_file_path}")

    df = pd.read_csv(
        csv_file_path,
        dtype={
            "Criteria ID": "Int64",
            "Name": "string",
            "Canonical Name": "string",
            "Parent ID": "Int64",
            "Country Code": "string",
            "Target Type": "string",
            "Status": "string",
        },
    )

    df = df.rename(
        columns={
            "Criteria ID": "id",
            "Name": "name",
            "Canonical Name": "canonical_name",
            "Parent ID": "parent_id",
            "Country Code": "country_code",
            "Target Type": "target_type",
            "Status": "status",
        }
    )
    logging.info(f"Geo targets CSV parsed to DataFrame: {df.shape}")

    return df


def insert_geo_targets(df: pd.DataFrame) -> None:
    logging.info("Saving geo targets to database...")

    connection_url = build_connection_url()

    logging.info("Creating database engine...")
    engine: Engine = create_engine(connection_url, echo=False)
    logging.info("Database engine created")

    GeoTarget.__table__.create(bind=engine, checkfirst=True)

    logging.info("Checking for existing geo targets...")
    with engine.connect() as connection:
        existing_ids = {
            row[0] for row in connection.execute(select(GeoTarget.id)).fetchall()
        }

    df = df[~df["id"].isin(existing_ids)]
    logging.info(f"New geo targets to insert: {df.shape}")

    if df.empty:
        logging.info("No new geo targets to insert")
        return

    logging.info("Inserting geo targets...")

    batch_size = 25000
    records = df.to_dict(orient="records")
    batch_count = len(records) // batch_size + 1
    for idx, batch in enumerate(batched(records, batch_size), start=1):
        with engine.begin() as connection:
            connection.execute(insert(GeoTarget), batch)
        logging.debug(f"GeoTarget - Batch {idx}/{batch_count} inserted.")

    logging.info("Geo targets saved to database")


def insert_restricted_locations() -> None:
    restricted_geo_targets = [
        GeoTarget(
            id=21120,
            name="Crimea",
            canonical_name="Crimea",
            parent_id=None,
            country_code="UA",
            target_type="Country",
            status="Active",
        ),
        GeoTarget(
            id=2192,
            name="Cuba",
            canonical_name="Cuba",
            parent_id=None,
            country_code="CU",
            target_type="Country",
            status="Active",
        ),
        GeoTarget(
            id=21113,
            name="So-called Donetsk People's Republic (DNR)",
            canonical_name="So-called Donetsk People's Republic (DNR)",
            parent_id=None,
            country_code="UA",
            target_type="Country",
            status="Active",
        ),
        GeoTarget(
            id=21111,
            name="So-called Luhansk People's Republic (LNR)",
            canonical_name="So-called Luhansk People's Republic (LNR)",
            parent_id=None,
            country_code="UA",
            target_type="Country",
            status="Active",
        ),
        GeoTarget(
            id=2364,
            name="Iran",
            canonical_name="Iran",
            parent_id=None,
            country_code="IR",
            target_type="Country",
            status="Active",
        ),
        GeoTarget(
            id=2770,
            name="North Korea",
            canonical_name="North Korea",
            parent_id=None,
            country_code="KP",
            target_type="Country",
            status="Active",
        ),
        GeoTarget(
            id=2760,
            name="Syria",
            canonical_name="Syria",
            parent_id=None,
            country_code="SY",
            target_type="Country",
            status="Active",
        ),
        GeoTarget(
            id=2408,
            name="North Korea",
            canonical_name="North Korea",
            parent_id=None,
            country_code="KP",
            target_type="Country",
            status="Active",
        ),
        GeoTarget(
            id=2760,
            name="Syria",
            canonical_name="Syria",
            parent_id=None,
            country_code="SY",
            target_type="Country",
            status="Active",
        ),
    ]

    logging.info("Saving restricted geo targets to database...")

    connection_url = build_connection_url()

    logging.info("Creating database engine...")
    engine: Engine = create_engine(connection_url, echo=False)
    logging.info("Database engine created")

    with engine.begin() as connection:
        for geo_target in restricted_geo_targets:
            stmt: Union[Insert, Update]
            if connection.execute(
                select(GeoTarget.id).where(GeoTarget.id == geo_target.id)
            ).fetchone():
                stmt = (
                    update(GeoTarget)
                    .where(GeoTarget.id == geo_target.id)
                    .values(
                        name=geo_target.name,
                        canonical_name=geo_target.canonical_name,
                        parent_id=geo_target.parent_id,
                        country_code=geo_target.country_code,
                        target_type=geo_target.target_type,
                        status=geo_target.status,
                    )
                )
            else:
                stmt = insert(GeoTarget).values(
                    id=geo_target.id,
                    name=geo_target.name,
                    canonical_name=geo_target.canonical_name,
                    parent_id=geo_target.parent_id,
                    country_code=geo_target.country_code,
                    target_type=geo_target.target_type,
                    status=geo_target.status,
                )
            connection.execute(stmt)
    logging.info("Restricted geo targets saved to database")


def fetch_latest_geo_targets() -> None:
    logging.info("Fetching latest geo targets...")

    project_dir = os.getenv("PROJECT_DIR")

    logging.info(f"PROJECT_DIR: {project_dir}")
    if project_dir is None:
        logging.warning("PROJECT_DIR not set. Using current working directory.")
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        os.environ["PROJECT_DIR"] = project_dir
        logging.info(f"PROJECT_DIR: {project_dir}")

    resources_dir = os.path.join(project_dir, "resources")
    os.makedirs(resources_dir, exist_ok=True)
    logging.info(f"Resources directory: {resources_dir}")

    base_url = "https://developers.google.com"
    download_url = get_geo_targets_zip_url(base_url)

    zip_file_name = download_url.split("/")[-1]
    zip_file_path = os.path.join(resources_dir, zip_file_name)

    download_geo_targets_zip(download_url, zip_file_path)
    extract_geo_targets_zip(zip_file_path, resources_dir)

    df = parse_csv(resources_dir)
    insert_geo_targets(df)
    insert_restricted_locations()


if __name__ == "__main__":
    fetch_latest_geo_targets()

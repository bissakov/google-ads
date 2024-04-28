import logging
import os
import urllib.parse
import zipfile
from typing import Any, Dict, Hashable, Union

import bs4
import pandas as pd
import requests
from sqlalchemy import insert, select, update
from sqlalchemy.engine.base import Connection, Engine
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


def insert_geo_target(connection: Connection, row: Dict[Hashable, Any]) -> None:
    stmt: Union[Insert, Update]
    if connection.execute(
        select(GeoTarget.id).where(GeoTarget.id == row["id"])
    ).fetchone():
        stmt = (
            update(GeoTarget)
            .where(GeoTarget.id == row["id"])
            .values(
                name=row["name"],
                canonical_name=row["canonical_name"],
                parent_id=row["parent_id"],
                country_code=row["country_code"],
                target_type=row["target_type"],
                status=row["status"],
            )
        )
    else:
        stmt = insert(GeoTarget).values(
            id=row["id"],
            name=row["name"],
            canonical_name=row["canonical_name"],
            parent_id=row["parent_id"],
            country_code=row["country_code"],
            target_type=row["target_type"],
            status=row["status"],
        )
    connection.execute(stmt)


def insert_geo_targets(df: pd.DataFrame) -> None:
    logging.info("Saving geo targets to database...")

    connection_url = build_connection_url()

    logging.info("Creating database engine...")
    engine: Engine = create_engine(connection_url, echo=False)
    logging.info("Database engine created")

    GeoTarget.__table__.create(bind=engine, checkfirst=True)

    logging.info("Inserting geo targets...")

    batch_size = 25000
    for batch in batched(df.to_dict(orient="records"), batch_size):
        with engine.begin() as connection:
            for row in batch:
                insert_geo_target(connection, row)
        logging.info(
            f"{batch_size} geo targets inserted or updated. Committing transaction..."
        )

    logging.info("Geo targets saved to database")


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


if __name__ == "__main__":
    fetch_latest_geo_targets()

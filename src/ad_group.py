import dataclasses
import logging
from typing import List

from google.ads.googleads.client import GoogleAdsClient

from src.error_handler import handle_google_ads_exception


@dataclasses.dataclass
class GAdGroup:
    id: int
    campaign_id: int
    account_id: int
    name: str
    resource_name: str
    status: str

    def __iter__(self):
        return iter(dataclasses.astuple(self))


@handle_google_ads_exception
def ad_group_structure(
    client: GoogleAdsClient, account_ids: List[int]
) -> List[GAdGroup]:
    ad_groups: List[GAdGroup] = []

    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT 
          ad_group.id, 
          campaign.id,
          ad_group.name, 
          ad_group.resource_name, 
          ad_group.status 
        FROM ad_group 
        ORDER BY ad_group.id
    """

    for account_id in account_ids:
        logging.info(f"Fetching ad groups for account '{account_id}'")

        stream = ga_service.search_stream(customer_id=str(account_id), query=query)

        for batch in stream:
            for row in batch.results:
                ad_groups.append(
                    GAdGroup(
                        id=row.ad_group.id,
                        campaign_id=row.campaign.id,
                        account_id=account_id,
                        name=row.ad_group.name,
                        resource_name=row.ad_group.resource_name,
                        status=row.ad_group.status.name,
                    )
                )

    return ad_groups

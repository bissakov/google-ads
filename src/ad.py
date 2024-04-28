import dataclasses
import logging
from typing import List

from google.ads.googleads.client import GoogleAdsClient

from src.error_handler import handle_google_ads_exception


@dataclasses.dataclass
class GAd:
    id: int
    ad_group_id: int
    campaign_id: int
    account_id: int
    name: str
    resource_name: str
    status: str

    def __iter__(self):
        return iter(dataclasses.astuple(self))


@handle_google_ads_exception
def ad_structure(client: GoogleAdsClient, account_ids: List[int]) -> List[GAd]:
    ads: List[GAd] = []

    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT 
          ad_group_ad.ad.id, 
          ad_group.id, 
          campaign.id, 
          ad_group_ad.ad.image_ad.name, 
          ad_group_ad.ad.resource_name, 
          ad_group_ad.status 
        FROM ad_group_ad 
        ORDER BY ad_group_ad.ad.id
    """

    for account_id in account_ids:
        logging.info(f"Fetching ad groups for account '{account_id}'")

        stream = ga_service.search_stream(customer_id=str(account_id), query=query)

        for batch in stream:
            for row in batch.results:
                ads.append(
                    GAd(
                        id=row.ad_group_ad.ad.id,
                        ad_group_id=row.ad_group.id,
                        campaign_id=row.campaign.id,
                        account_id=account_id,
                        name=row.ad_group_ad.ad.name,
                        resource_name=row.ad_group_ad.ad.resource_name,
                        status=row.ad_group_ad.status.name,
                    )
                )

    return ads

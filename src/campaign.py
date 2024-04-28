import dataclasses
from typing import List

from google.ads.googleads.client import GoogleAdsClient

from src.error_handler import handle_google_ads_exception


@dataclasses.dataclass
class GCampaign:
    id: int
    account_id: int
    name: str
    resource_name: str
    status: str
    advertising_channel_type: str
    advertising_channel_sub_type: str
    start_date: str
    end_date: str

    def __iter__(self):
        return iter(dataclasses.astuple(self))


@handle_google_ads_exception
def campaign_structure(
    client: GoogleAdsClient, account_ids: List[int]
) -> List[GCampaign]:
    campaigns: List[GCampaign] = []

    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          campaign.id,
          campaign.name,
          campaign.resource_name,
          campaign.status,
          campaign.advertising_channel_type,
          campaign.advertising_channel_sub_type,
          campaign.start_date,
          campaign.end_date
        FROM campaign
        ORDER BY campaign.id
    """

    for account_id in account_ids:
        stream = ga_service.search_stream(customer_id=str(account_id), query=query)

        for batch in stream:
            for row in batch.results:
                campaigns.append(
                    GCampaign(
                        id=row.campaign.id,
                        account_id=account_id,
                        name=row.campaign.name,
                        resource_name=row.campaign.resource_name,
                        status=row.campaign.status.name,
                        advertising_channel_type=row.campaign.advertising_channel_type.name,
                        advertising_channel_sub_type=row.campaign.advertising_channel_sub_type.name,
                        start_date=row.campaign.start_date,
                        end_date=row.campaign.end_date,
                    )
                )

    return campaigns

import dataclasses
from typing import Dict, List, Union

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.v16.resources.types.customer_client import CustomerClient

from src.error_handler import handle_google_ads_exception


@dataclasses.dataclass
class GAccount:
    id: int
    name: str
    resource_name: str
    account_customer: str
    manager: bool
    currency_code: str
    level: int
    time_zone: str

    def __iter__(self):
        return iter(dataclasses.astuple(self))


@handle_google_ads_exception
def account_structure(
    client: GoogleAdsClient, login_account_id: Union[str, int]
) -> List[GAccount]:
    accounts: List[GAccount] = []

    if isinstance(login_account_id, str):
        login_account_id = int(login_account_id)

    googleads_service = client.get_service("GoogleAdsService")

    processed_account_ids = [login_account_id]

    query = """
        SELECT
          customer_client.client_customer,
          customer_client.level,
          customer_client.manager,
          customer_client.descriptive_name,
          customer_client.currency_code,
          customer_client.time_zone,
          customer_client.id
        FROM customer_client
        WHERE customer_client.level <= 1
    """

    for processed_account_id in processed_account_ids:
        unprocessed_account_ids: List[int] = [processed_account_id]
        customer_ids_to_child_accounts: Dict[int, List[CustomerClient]] = {}
        root_account_client = None

        while unprocessed_account_ids:
            account_id = int(unprocessed_account_ids.pop(0))
            response = googleads_service.search(
                customer_id=str(account_id), query=query
            )

            assert response is not None

            for googleads_row in response:
                account_client = googleads_row.customer_client

                if account_client.level == 0:
                    if root_account_client is None:
                        root_account_client = account_client
                    continue

                if account_id not in customer_ids_to_child_accounts:
                    customer_ids_to_child_accounts[account_id] = []

                customer_ids_to_child_accounts[account_id].append(account_client)

                if account_client.manager:
                    if (
                        account_client.id not in customer_ids_to_child_accounts
                        and account_client.level == 1
                    ):
                        unprocessed_account_ids.append(account_client.id)

        if root_account_client is not None:
            accounts.extend(
                parse_account_hierarchy(
                    root_account_client, customer_ids_to_child_accounts
                )
            )
        else:
            raise ValueError("No root customer found in the hierarchy.")
    return accounts


def parse_account_hierarchy(
    account_client: CustomerClient,
    account_ids_to_child_accounts: Dict[int, List[CustomerClient]],
    depth: int = 0,
) -> List[GAccount]:
    accounts: List[GAccount] = []

    account_id = account_client.id
    accounts.append(
        GAccount(
            id=account_id,
            name=account_client.descriptive_name,
            resource_name=account_client.resource_name,
            account_customer=account_client.client_customer,
            manager=account_client.manager,
            currency_code=account_client.currency_code,
            level=account_client.level,
            time_zone=account_client.time_zone,
        )
    )

    if account_id in account_ids_to_child_accounts:
        for child_account in account_ids_to_child_accounts[account_id]:
            accounts.extend(
                parse_account_hierarchy(
                    child_account, account_ids_to_child_accounts, depth + 1
                )
            )

    return accounts

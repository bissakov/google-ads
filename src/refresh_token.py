import argparse
import os
import sys

import dotenv
from google_auth_oauthlib.flow import InstalledAppFlow
from oauthlib.oauth2.rfc6749.errors import InvalidGrantError

dotenv.load_dotenv()

DEFAULT_CLIENT_ID = os.getenv("DEFAULT_CLIENT_ID")
assert DEFAULT_CLIENT_ID, "Missing DEFAULT_CLIENT_ID environment variable."

DEFAULT_CLIENT_SECRET = os.getenv("DEFAULT_CLIENT_SECRET")
assert DEFAULT_CLIENT_SECRET, "Missing DEFAULT_CLIENT_SECRET environment variable."

SCOPE = "https://www.googleapis.com/auth/adwords"

_REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"

parser = argparse.ArgumentParser(
    description="Generates a refresh token with " "the provided credentials."
)
parser.add_argument(
    "--client_id",
    default=DEFAULT_CLIENT_ID,
    help="Client Id retrieved from the Developer's Console.",
)
parser.add_argument(
    "--client_secret",
    default=DEFAULT_CLIENT_SECRET,
    help="Client Secret retrieved from the Developer's " "Console.",
)
parser.add_argument(
    "--additional_scopes",
    default=None,
    help="Additional scopes to apply when generating the "
    "refresh token. Each scope should be separated by a comma.",
)


class ClientConfigBuilder(object):
    """Helper class used to build a client config dict used in the OAuth 2.0 flow."""

    _DEFAULT_AUTH_URI = "https://accounts.google.com/o/oauth2/auth"
    _DEFAULT_TOKEN_URI = "https://accounts.google.com/o/oauth2/token"
    CLIENT_TYPE_WEB = "web"
    CLIENT_TYPE_INSTALLED_APP = "installed"

    def __init__(
        self,
        client_type=None,
        client_id=None,
        client_secret=None,
        auth_uri=_DEFAULT_AUTH_URI,
        token_uri=_DEFAULT_TOKEN_URI,
    ):
        self.client_type = client_type
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_uri = auth_uri

        self.token_uri = token_uri

    def build(self):
        """Builds a client config dictionary used in the OAuth 2.0 flow."""
        if all(
            (
                self.client_type,
                self.client_id,
                self.client_secret,
                self.auth_uri,
                self.token_uri,
            )
        ):
            client_config = {
                self.client_type: {
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "auth_uri": self.auth_uri,
                    "token_uri": self.token_uri,
                }
            }
        else:
            raise ValueError("Required field is missing.")

        return client_config


def main(client_id, client_secret, scopes):
    """Retrieve and display the access and refresh token."""
    client_config = ClientConfigBuilder(
        client_type=ClientConfigBuilder.CLIENT_TYPE_WEB,
        client_id=client_id,
        client_secret=client_secret,
    )

    flow = InstalledAppFlow.from_client_config(client_config.build(), scopes=scopes)
    flow.redirect_uri = _REDIRECT_URI

    auth_url, _ = flow.authorization_url(prompt="consent")

    print(
        "Log into the Google Account you use to access your AdWords account "
        "and go to the following URL: \n%s\n" % auth_url
    )
    print("After approving the token enter the verification code (if specified).")

    code = input("Code: ").strip()

    try:
        flow.fetch_token(code=code)
    except InvalidGrantError as ex:
        print("Authentication has failed: %s" % ex)
        sys.exit(1)

    print("Access token: %s" % flow.credentials.token)
    print("Refresh token: %s" % flow.credentials.refresh_token)


if __name__ == "__main__":
    args = parser.parse_args()
    configured_scopes = [SCOPE]
    if not (
        any([args.client_id, DEFAULT_CLIENT_ID])
        and any([args.client_secret, DEFAULT_CLIENT_SECRET])
    ):
        raise AttributeError("No client_id or client_secret specified.")
    if args.additional_scopes:
        configured_scopes.extend(args.additional_scopes.replace(" " "", "").split(","))
    main(args.client_id, args.client_secret, configured_scopes)

import logging
import os
import pickle
import os.path
from dataclasses import dataclass
from enum import Enum
from typing import List

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


from googleapiwrapper.common import ServiceType
from googleapiwrapper.utils import CommonUtils


LOG = logging.getLogger(__name__)


class CredentialsFileType(Enum):
    CLIENT_SECRET = "client-secret"
    TOKEN_PICKLE = "token-pickle"


@dataclass
class AuthedSession:
    authed_creds: Credentials
    user_email: str
    user_name: str
    project_name: str


class GoogleApiAuthorizer:
    DEFAULT_SCOPES = [
        "https://accounts.google.com/o/oauth2/token",
    ]
    # TODO If modifying these scopes, delete the file token.pickle.
    DEFAULT_WEBSERVER_PORT = 49555

    def __init__(
        self, service_type: ServiceType,
        secret_basedir: str, project_name: str,
        account_email: str, scopes: List[str] = None,
        server_port: int = DEFAULT_WEBSERVER_PORT,
    ):
        self.account_email = account_email
        self.secret_basedir = secret_basedir
        self.service_type = service_type
        self.project_name = project_name
        self._set_scopes(scopes)
        self.server_port = server_port
        self.token_full_path = self._get_file_full_path(cred_file_type=CredentialsFileType.TOKEN_PICKLE)
        self.credentials_full_path = self._get_file_full_path(cred_file_type=CredentialsFileType.CLIENT_SECRET)
        LOG.info(
            f"Configuration of {type(self).__name__}:\n"
            f"Secret basedir: {self.secret_basedir}\n"
            f"Project name: {self.project_name}\n"
            f"Account email: {self.account_email}\n"
            f"Scopes: {self.scopes}\n"
            f"Server port: {self.server_port}\n"
            f"Token file path (read/write): {self.token_full_path}\n"
            f"Credentials file path (read-only): {self.credentials_full_path}\n"
        )

    def _get_file_full_path(self, cred_file_type: CredentialsFileType):
        account_dirname = CommonUtils.convert_email_address_to_dirname(self.account_email)
        if cred_file_type == CredentialsFileType.CLIENT_SECRET:
            return os.path.join(self.secret_basedir, self.project_name, f"client_secret_{account_dirname}.json")
        elif cred_file_type == CredentialsFileType.TOKEN_PICKLE:
            return os.path.join(
                self.secret_basedir,
                self.project_name,
                "tokenpickles",
                f"token_{self.project_name}_{account_dirname}.pickle",
            )

    def _set_scopes(self, scopes):
        self.scopes = scopes
        if self.scopes is None:
            self.scopes = self.service_type.default_scopes

        # https://stackoverflow.com/a/51643134/1106893
        os.environ["OAUTHLIB_RELAX_TOKEN_SCOPE"] = "1"
        self.scopes.extend(self.DEFAULT_SCOPES)

    def authorize(self) -> AuthedSession:
        authed_session: AuthedSession = self._load_token()

        locals = vars()
        del locals["authed_session"]
        LOG.debug("Session details: %s", locals)

        if not authed_session:
            authed_session = self._handle_login(authed_session)

        authed_session_authed_creds = authed_session.authed_creds
        creds_valid = authed_session.authed_creds.valid
        if not authed_session or not authed_session_authed_creds or not creds_valid:
            # If there are no (valid) credentials available, let the user log in.
            authed_session = self._handle_login(authed_session)
        return authed_session

    def _load_token(self) -> AuthedSession:
        """
        The file token.pickle stores the user's access and refresh tokens, and is
        created automatically when the authorization flow completes for the first
        time.
        """
        authed_session: AuthedSession or None = None
        LOG.debug("Loading token from file: %s", self.token_full_path)
        if os.path.exists(self.token_full_path):
            with open(self.token_full_path, "rb") as token:
                authed_session = pickle.load(token)
        return authed_session

    def _handle_login(self, authed_session: AuthedSession) -> AuthedSession:
        if authed_session:
            creds = authed_session.authed_creds
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(self.credentials_full_path, self.scopes)
            authed_creds: Credentials = flow.run_local_server(port=self.server_port, prompt="consent")

            session = flow.authorized_session()
            profile_info = session.get("https://www.googleapis.com/userinfo/v2/me").json()
            authed_session = AuthedSession(authed_creds, profile_info["email"], profile_info["name"], self.project_name)
        # Save the credentials for the next run
        self._write_token(authed_session)
        return authed_session

    def _write_token(self, authed_session: AuthedSession):
        os.makedirs(os.path.dirname(self.token_full_path), exist_ok=True)
        with open(self.token_full_path, "wb") as token:
            pickle.dump(authed_session, token)

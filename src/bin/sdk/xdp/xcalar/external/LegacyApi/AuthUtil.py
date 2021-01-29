# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import os
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import http.cookiejar
from urllib.parse import urlparse, urlunparse

from xcalar.compute.util.utils import XcUtil

from .Env import (XcalarServiceSessionPath, XcalarLoginPath,
                  XcalarSessionStatusPath)

DEFAULT_JAR_FILE = "~/.xcalar/cookie.txt"
COOKIE_JAR_FILE = os.getenv('XCE_COOKIE_JAR_FILE', DEFAULT_JAR_FILE)
XCE_COOKIE_NAME = 'connect.sid'


class Authenticate:
    login_retries = XcUtil.MAX_RETRIES

    def __init__(self,
                 url=None,
                 client_secrets=None,
                 client_secrets_file=None,
                 bypass_proxy=False,
                 client_token=None,
                 session_type='api',
                 save_cookies=False):

        self._using_proxy = not bypass_proxy
        self._get_cluster_params(url, session_type)
        self._session = requests.Session()
        retry = Retry(
            total=7,
            backoff_factor=0.5,
            method_whitelist=('GET', 'POST'),
            status_forcelist=(502, ))    # retry for 60s
        self._session.mount('http://', HTTPAdapter(max_retries=retry))

        # Specifying client_token has precedence over client_secrets
        self.client_token = client_token
        self.username = None
        self._client_secrets = None
        self._cookies = {}
        self._session_type = session_type
        self.save_cookies = save_cookies

        if not client_token and self._using_proxy:
            self._find_user_credentials(client_secrets, client_secrets_file)
        # if client token is avilable get user details
        if self.client_token:
            session_status = self._get_session_status()
            if 'username' not in session_status or session_status.get(
                    'username') is None:
                raise ValueError("Client token is not valid!")
            self.username = session_status.get('username')

        self.username = self.username.lower() if self.username else None
        # this is needed if environment variable is set programatically,
        # after this file is imported
        global COOKIE_JAR_FILE
        COOKIE_JAR_FILE = os.getenv('XCE_COOKIE_JAR_FILE', DEFAULT_JAR_FILE)

    def login(self):
        errMsg = "Authentication failed. Generate a new client_token, as follows:\n1) Click the cell containing your connection snippet.\n2) From the CODE SNIPPETS menu, select Connect to Xcalar Workbook.\n3) Click the cell containing your original connection snippet.\n4) From the Edit menu, select Delete Cells."
        headers = {'Content-type': 'application/json'}
        response = {}
        if self.client_token is not None:
            # Don't authenticate via the login server.  Just use the auth token
            # that was passed in.
            status_url = urlunparse(
                self.parsed_url._replace(path=XcalarServiceSessionPath))
            try:
                response = self._session.post(
                    url=status_url,
                    data=json.dumps({
                        'token': self.client_token,
                        'sessionType': self._session_type
                    }),
                    headers=headers,
                    verify=self.verifySsl)
            except Exception as e:
                raise RuntimeError("Connection Error") from e
            if (response.status_code != 200):
                print(errMsg)
                response.raise_for_status()
        elif self._client_secrets is not None:
            login_url = urlunparse(
                self.parsed_url._replace(path=XcalarLoginPath))
            response = XcUtil.retryer(
                self._do_login,
                login_url,
                headers,
                retry_exceptions=[json.decoder.JSONDecodeError],
                max_retries=self.login_retries)
        else:
            raise RuntimeError(errMsg)

        self._cookies = response.cookies.get_dict()
        if XCE_COOKIE_NAME not in self._cookies:
            raise RuntimeError(
                "Cookie {} not found error".format(XCE_COOKIE_NAME))
        self.client_token = self._cookies[XCE_COOKIE_NAME]
        self._save_cookie(response.cookies)

    @property
    def cookies(self):
        return self._cookies

    def is_session_expired(self):
        if self._using_proxy and (
                self.client_token is None
                or self._get_session_status().get("loggedIn", False) is False):
            # session expired, remove the client_token, to authenticate with
            # credentials again
            self.client_token = None
            return True
        return False

    def read_token_from_cookie_file(self):
        cookie_full_path = os.path.expanduser(COOKIE_JAR_FILE)
        if not os.path.exists(cookie_full_path):
            return
        cookie_jar = self._load_cookie_jar()
        for cookie in cookie_jar:
            if cookie.name == XCE_COOKIE_NAME:
                return cookie.value

    def get_cookie_string(self):
        return ''.join("{}={};".format(name, value)
                       for name, value in self._cookies.items())

    # this method will be getting all xcalar non-credential parameters
    # like url, session type. Will be updated to support multiple environments.
    def _get_cluster_params(self, url, session_type):
        # Allow the user to pass in a custom url; otherwise select smart defaults
        if url is None:
            url = "https://localhost"
            if 'XCE_HTTPS_PORT' in os.environ:
                url = "https://localhost:{}".format(
                    os.environ["XCE_HTTPS_PORT"])

        self.parsed_url = urlparse(url)
        if self.parsed_url.path:
            print("warn: url path {} provided will be ignored.".format(
                self.parsed_url.path))
        if self.parsed_url.hostname == "localhost":
            self.verifySsl = False
        else:
            self.verifySsl = os.environ.get("XLR_PYSDK_VERIFY_SSL_CERT",
                                            "true") != "false"
        # get session type
        self._session_type = session_type

    # validates the session token, also updates the client_token
    def _get_session_status(self):
        session_status_url = urlunparse(
            self.parsed_url._replace(path=XcalarSessionStatusPath))
        cookie = {XCE_COOKIE_NAME: self.client_token}
        status = {}
        try:
            r = self._session.get(
                url=session_status_url,
                cookies=cookie,
                headers={'Content-Type': 'application/json'},
                verify=False)
            self._cookies = r.cookies.get_dict()
            self.client_token = self._cookies[XCE_COOKIE_NAME]
            self._save_cookie(r.cookies)
            status = r.json()
        except Exception as e:
            # failing silently here, don't want to stop creating client object
            pass
        return status

    def _find_user_credentials(self, client_secrets, client_secrets_file):
        if client_secrets:
            self.username = client_secrets.get('xiusername', None)
            try:
                self._client_secrets = json.dumps(client_secrets)
            except ValueError as ex:
                raise ValueError(
                    "client_secrets should be a valid json object") from ex
            return

        if client_secrets_file:
            path = client_secrets_file
        else:
            path = (os.environ["XLRDIR"] +
                    "/src/bin/sdk/xdp/xcalar/external/client_secret.json")
        if os.path.exists(path) or client_secrets_file is not None:
            try:
                with open(path, 'r') as f:
                    _client_secrets = json.load(f)
                self.username = _client_secrets.get('xiusername', None)
                self._client_secrets = json.dumps(_client_secrets)
            except (OSError, IOError) as ex:
                raise ValueError("Failed to load client_secrets_file at {}".
                                 format(path)) from ex
            except ValueError as ex1:
                raise ValueError(
                    "client_secrets_file should contain a valid json type credentials"
                ) from ex1
            return

        # read token from cookie if exists
        self.client_token = self.read_token_from_cookie_file()

        # not able to get client token or user credentials so raise error
        if not self.client_token:
            raise ValueError("Must specify one of the following kwargs:"
                             " client_secrets_file, client_secrets, or "
                             "client_token.")

    def _save_cookie(self, cookies):
        if not self.save_cookies:
            return
        cookie_full_path = os.path.expanduser(COOKIE_JAR_FILE)
        cookie_jar = self._load_cookie_jar()
        if not os.path.exists(cookie_full_path):
            os.makedirs(os.path.dirname(cookie_full_path), exist_ok=True)
            # creates a empty cookie file
            cookie_jar.save()
        for cookie in cookies:
            args = dict(vars(cookie).items())
            args['rest'] = args['_rest']
            del args['_rest']
            c = http.cookiejar.Cookie(**args)
            cookie_jar.set_cookie(c)
        cookie_jar.save()

    def _load_cookie_jar(self):
        cookie_full_path = os.path.expanduser(COOKIE_JAR_FILE)
        cookie_jar = http.cookiejar.LWPCookieJar(cookie_full_path)
        try:
            cookie_jar.load()
        except Exception as ex:
            # some error loading the cookie jar will return empty jar
            # don't fail as
            pass
        return cookie_jar

    def _do_login(self, login_url, headers):
        response = None
        try:
            response = self._session.post(
                url=login_url,
                data=self._client_secrets,
                headers=headers,
                verify=self.verifySsl)
        except Exception as e:
            raise RuntimeError("Connection Error") from e
        response.raise_for_status()

        if (not json.loads(response.content)["isValid"]):
            raise RuntimeError("Incorrect login details")
        return response

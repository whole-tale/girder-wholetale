import urllib.parse
from typing import ClassVar

import requests
from girder.api.rest import getApiUrl
from girder.exceptions import GirderException, RestException
from girder.models.setting import Setting
from girder.models.user import User
from girder.settings import SettingKey
from girder_oauth.providers.base import ProviderBase
from girder_oauth.settings import PluginSettings


class ORCID(ProviderBase):
    _PROVIDER_ID = "orcid"
    _EXTERNAL_NAME = "ORCID"
    _CLIENT_ID_SETTING = "oauth.orcid_client_id"
    _CLIENT_SECRET_SETTING = "oauth.orcid_client_secret"
    _ID_DOMAIN = "orcid.org"  # used for the placeholder email
    _AUTH_URL = "https://orcid.org/oauth/authorize"
    _AUTH_SCOPES: ClassVar[list[str]] = ["/authenticate"]
    _TOKEN_URL = "https://orcid.org/oauth/token"
    _REVOKE_URL = "https://orcid.org/oauth/revoke"
    _API_USER_URL = "https://pub.orcid.org/v3.0/{orcid}{path}"

    # header for user: application/vnd.orcid+json

    @classmethod
    def getProviderName(cls, external=False):
        if external:
            return cls._EXTERNAL_NAME
        else:
            return cls._PROVIDER_ID

    def getClientIdSetting(self):
        return Setting().get(self._CLIENT_ID_SETTING)

    def getClientSecretSetting(self):
        return Setting().get(self._CLIENT_SECRET_SETTING)

    @classmethod
    def getUrl(cls, state):
        clientId = Setting().get(cls._CLIENT_ID_SETTING)

        if not clientId:
            raise GirderException(
                f"No {cls.getProviderName(external=True)} client ID setting is present."
            )

        callbackUrl = f"{getApiUrl()}/oauth/{cls.getProviderName()}/callback"

        query = urllib.parse.urlencode(
            {
                "client_id": clientId,
                "response_type": "code",
                "redirect_uri": callbackUrl,
                "state": state,
                "scope": " ".join(cls._AUTH_SCOPES),
            }
        )
        return f"{cls._AUTH_URL}?{query}"

    def getToken(self, code):
        params = {
            "code": code,
            "client_id": self.clientId,
            "client_secret": self.clientSecret,
            "redirect_uri": self.redirectUri,
            "grant_type": "authorization_code",
        }
        resp = self._getJson(
            method="POST",
            url=self._TOKEN_URL,
            data=params,
            headers={"Accept": "application/json"},
        )
        if "error" in resp:
            raise RestException(
                f'Got an error exchanging token from provider: "{resp}".', code=502
            )
        return resp

    def revokeToken(self, token):
        params = {
            "token": token["refresh_token"],
            "client_id": self.clientId,
            "client_secret": self.clientSecret,
        }

        resp = requests.request(
            method="POST",
            url=self._REVOKE_URL,
            data=params,
            headers={"Accept": "application/json"},
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            raise RestException(
                'Got {} code from provider, response="{}".'.format(resp.status_code, resp.content.decode("utf8")),
                code=502,
            )

    def refreshToken(self, token):
        params = {
            "refresh_token": token["refresh_token"],
            "client_id": self.clientId,
            "client_secret": self.clientSecret,
            "grant_type": "refresh_token",
        }
        resp = self._getJson(
            method="POST",
            url=self._TOKEN_URL,
            data=params,
            headers={"Accept": "application/json"},
        )
        if "error" in resp:
            raise RestException(
                f'Got an error refreshing token from provider: "{resp}".', code=502
            )
        return resp

    def getUser(self, token):
        providerName = self.getProviderName()
        externalName = self.getProviderName(external=True)
        headers = {
            "Authorization": "Bearer {}".format(token["access_token"]),
            "Accept": "application/vnd.orcid+json",
        }
        # Get user's email address
        resp = self._getJson(
            method="GET", url=self._API_USER_URL.format(path="/person", **token), headers=headers
        )

        try:
            email = resp["emails"]["email"][0]["email"]
        except (KeyError, TypeError, IndexError):
            email = "{}@{}".format(token["orcid"], self._ID_DOMAIN)

        oauthId = token["orcid"]
        if not oauthId:
            raise RestException(f"{externalName} did not return a user ID.", code=502)
        try:
            lastName = resp["name"]["family-name"]["value"]
        except (KeyError, TypeError):
            lastName = "N/A"
        try:
            firstName = resp["name"]["given-names"]["value"]
        except (KeyError, TypeError):
            firstName = "N/A"

        if lastName == "" and firstName == "":
            raise RestException(f"{externalName} did not return a user name.", code=502)

        userName = firstName.replace(" ", "") + "-" + lastName.replace(" ", "")
        user = User().findOne({"oauth.provider": providerName, "oauth.id": oauthId})
        setId = not user
        if not user:
            user = User().findOne({"email": email})

        dirty = False
        if not user:
            if Setting().get(
                SettingKey.REGISTRATION_POLICY
            ) == "closed" and not Setting().get(
                PluginSettings.IGNORE_REGISTRATION_POLICY
            ):
                raise RestException(
                    "Registration is closed. Contact an administrator to create an account "
                    "for you."
                )
            login = self._deriveLogin(email, firstName, lastName, userName)
            user = User().createUser(
                login=login,
                password=None,
                firstName=firstName,
                lastName=lastName,
                email=email,
            )
        else:
            if firstName != user["firstName"] and firstName:
                user["firstName"] = firstName
                dirty = True
            if lastName != user["lastName"] and lastName:
                user["lastName"] = lastName
                dirty = True
            if email != user["email"] and email != f"{oauthId}@{self._ID_DOMAIN}":
                user["email"] = email
                dirty = True
        if setId:
            user.setdefault("oauth", []).append(
                {"provider": providerName, "id": oauthId}
            )
            dirty = True
        if dirty:
            user = User().save(user)

        return user


class SandboxORCID(ORCID):
    _PROVIDER_ID = "orcid_sandbox"
    _EXTERNAL_NAME = "ORCID Sandbox"
    _CLIENT_ID_SETTING = "oauth.orcid_sandbox_client_id"
    _CLIENT_SECRET_SETTING = "oauth.orcid_sandbox_client_secret"
    _ID_DOMAIN = "sandbox.orcid.org"
    _AUTH_URL = "https://sandbox.orcid.org/oauth/authorize"
    _AUTH_SCOPES: ClassVar[list[str]] = [
        "/authenticate",
        "/activities/update",
        "/read-limited",
    ]
    _TOKEN_URL = "https://sandbox.orcid.org/oauth/token"
    _REVOKE_URL = "https://sandbox.orcid.org/oauth/revoke"
    _API_USER_URL = "https://api.sandbox.orcid.org/v3.0/{orcid}{path}"

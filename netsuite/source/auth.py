from dlt.sources.helpers.rest_client.auth import AuthConfigBase, OAuthJWTAuth, pendulum
from dlt.common.configuration import configspec
from dlt.common.typing import TSecretStrValue

from requests_oauthlib import OAuth1


@configspec
class NetsuiteOAuth1(AuthConfigBase):
    """OAuth 1.0 (HMAC-SHA256) authenticator."""

    consumer_key: TSecretStrValue = None
    consumer_secret: TSecretStrValue = None
    token_key: TSecretStrValue = None
    token_secret: TSecretStrValue = None
    realm: str = None


    def __call__(self, request):
        self.oauth = OAuth1(
            client_key=self.consumer_key,
            client_secret=self.consumer_secret,
            resource_owner_key=self.token_key,
            resource_owner_secret=self.token_secret,
            signature_method="HMAC-SHA256",
            realm=self.realm.upper(),
        )
        return self.oauth(request)


@configspec
class NetsuiteOAuth2(OAuthJWTAuth):
    """NetSuite OAuth 2.0 client credentials grant via signed JWT client assertion (RFC 7523 §2.2)."""

    kid: str = None

    def obtain_token(self) -> None:
        import jwt

        payload = self.create_jwt_payload()
        assertion = jwt.encode(
            payload,
            self.load_private_key(),
            algorithm="PS256",
            headers={"kid": self.kid},
        )
        data = {
            "grant_type": "client_credentials",
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": assertion,
        }

        response = self.session.post(
            self.auth_endpoint,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=data,
        )
        response.raise_for_status()

        token_response = response.json()
        self.token = token_response["access_token"]
        self.token_expiry = pendulum.now().add(
            seconds=token_response.get("expires_in", self.default_token_expiration)
        )

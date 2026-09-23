from dlt.sources.helpers.rest_client.auth import AuthConfigBase
from dlt.common.configuration import configspec
from dlt.common.typing import TSecretStrValue

from requests_oauthlib import OAuth1


@configspec
class OAuth1Auth(AuthConfigBase):
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

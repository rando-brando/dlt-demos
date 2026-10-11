from dlt.common.configuration import configspec
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.typing import TSecretStrValue


@configspec
class SalesforceCredentials(CredentialsConfiguration):
    """Salesforce connected-app client credentials"""

    consumer_key: TSecretStrValue = None
    consumer_secret: TSecretStrValue = None
    domain: str = None

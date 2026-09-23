import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator

from source.auth import OAuth1Auth


def suiteql_query(
    client: RESTClient,
    resource: str,
    cursor: str = None,
    last_value: str = None,
    suiteql: bool = False
):
    """Page through resource via NetSuite's SuiteQL endpoint."""
    if suiteql:
        with open(f"suiteql/{resource}.sql", "r", encoding="utf-8") as file:
            suiteql = file.read()
        print(f"INFO: Suiteql file loaded for resource {resource}.", end="\n\n")
    else:
        suiteql = f"SELECT * FROM {resource}"
        print(f"INFO: No Suiteql file provided for {resource}. Defaulting to SELECT *.", end="\n\n")
    
    if cursor and last_value:
        if cursor == 'lastmodfieddate':
            last_value = f"TO_TIMESTAMP('{last_value}', 'YYYY-MM-DD HH24:MI:SS')" # convert to compatible timestamp
        suiteql += f" WHERE {cursor} > {last_value}"
    
    yield from client.paginate(
        "query/v1/suiteql",
        method="POST",
        headers={"Prefer": "transient", "Content-Type": "application/json"},
        json={"q": suiteql},
        data_selector="items",
        paginator=OffsetPaginator(limit=1000, total_path=None, has_more_path="hasMore"),
    )


@dlt.source(name="netsuite")
def netsuite_source(credentials: dict = dlt.secrets.value, account_id: str = dlt.secrets.value):
    """Netsuite source and resources"""
    base_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest"
    client = RESTClient(base_url=base_url, auth=OAuth1Auth(**credentials))

    # ─────────────────────────────────────────────
    # FULL LOAD (replace)
    # ─────────────────────────────────────────────
    @dlt.resource(
        name="account",
        write_disposition="replace",
        primary_key="id",
        columns={"id": {"data_type": "bigint", "unique": True}}
    )
    def account():
        yield from suiteql_query(client, "account")


    @dlt.resource(
        name="department",
        write_disposition="replace",
        primary_key="id",
        columns={"id": {"data_type": "bigint", "unique": True}}
    )
    def department():
        yield from suiteql_query(client, "department")


    @dlt.resource(
        name="subsidiary",
        write_disposition="replace",
        primary_key="id",
        columns={"id": {"data_type": "bigint", "unique": True}}
    )
    def subsidiary():
        yield from suiteql_query(client, "subsidiary")


    @dlt.resource(
        name="currency",
        write_disposition="replace",
        primary_key="id",
        columns={"id": {"data_type": "bigint", "unique": True}}
    )
    def currency():
        yield from suiteql_query(client, "currency")

    # ─────────────────────────────────────────────
    # INCREMENTAL (merge)
    # ─────────────────────────────────────────────
    @dlt.resource(
        name="customer",
        write_disposition="merge",
        primary_key="id",
        columns={
            "id": {"data_type": "bigint", "unique": True},
            "lastmodifieddate": {"data_type": "timestamp", "precision": 7, "timezone": False}
        }
    )
    def customer(incremental=dlt.sources.incremental("lastmodifieddate", initial_value=None)):
        yield from suiteql_query(client, "customer", "lastmodifieddate", incremental.last_value, True)


    @dlt.resource(
        name="job",
        write_disposition="merge",
        primary_key="id",
        columns={
            "id": {"data_type": "bigint", "unique": True},
            "lastmodifieddate": {"data_type": "timestamp", "precision": 7, "timezone": False}
        }
    )
    def job(incremental=dlt.sources.incremental("lastmodifieddate", initial_value=None)):
        yield from suiteql_query(client, "job", "lastmodifieddate", incremental.last_value, True)


    return (
        account,
        department,
        subsidiary,
        customer,
        currency,
        job,
    )

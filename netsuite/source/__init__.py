import dlt
from dlt.sources.helpers.rest_client import RESTClient

from source.helpers import file_hints, metadata_hints, suiteql_query
from source.auth import OAuth1Auth


@dlt.source(name="netsuite")
def netsuite_source(credentials: dict = dlt.secrets.value, account_id: str = dlt.secrets.value):
    """Netsuite source and resources"""
    base_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest"
    client = RESTClient(base_url=base_url, auth=OAuth1Auth(**credentials))

    # ─────────────────────────────────────────────
    # FULL LOAD (replace)
    # ─────────────────────────────────────────────
    @dlt.resource(
        name="Account",
        write_disposition="replace",
        primary_key="id"
    )
    def account():
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="Currency",
        write_disposition="replace",
        primary_key="id"
    )
    def currency():
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="Department",
        write_disposition="replace",
        primary_key="id"
    )
    def department():
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="Subsidiary",
        write_disposition="replace",
        primary_key="id"
    )
    def subsidiary():
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    # ─────────────────────────────────────────────
    # INCREMENTAL (merge)
    # ─────────────────────────────────────────────
    @dlt.resource(
        name="Customer",
        write_disposition="merge",
        primary_key="id"
    )
    def customer(incremental=dlt.sources.incremental("lastmodifieddate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "lastmodifieddate", incremental.last_value):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="CustomerSubsidiaryRelationship",
        write_disposition="merge",
        primary_key="id"
    )
    def customer_subsidiary_relationship(incremental=dlt.sources.incremental("lastmodifieddate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "lastmodifieddate", incremental.last_value):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="DeletedRecord",
        write_disposition="merge",
        primary_key={"recordTypeId", "recordId"}
    )
    def deleted_record(incremental=dlt.sources.incremental("deleteddate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = file_hints("hints/DeletedRecord.json")
        for items in suiteql_query(client, resource, columns, "deleteddate", incremental.last_value):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="Job",
        write_disposition="merge",
        primary_key="id"
    )
    def job(incremental=dlt.sources.incremental("lastmodifieddate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "lastmodifieddate", incremental.last_value):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="TransactionLineLink",
        write_disposition="merge",
        primary_key={"nextDoc", "nextLine", "previousDoc", "previousLine"}
    )
    def transaction_line_link(incremental=dlt.sources.incremental("lastmodifieddate", initial_value=None)):
        #resource = dlt.current.resource_name()
        columns = file_hints("hints/TransactionLineLink.json")
        for items in suiteql_query(client, "NextTransactionLineLink", columns, "lastmodifieddate", incremental.last_value):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))


    return (
        account,
        currency,
        customer,
        customer_subsidiary_relationship,
        deleted_record,
        department,
        job,
        subsidiary,
        transaction_line_link,
    )

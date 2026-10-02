import dlt
from dlt.sources.helpers.rest_client import RESTClient

from source.helpers import file_hints, metadata_hints, suiteql_query
from source.auth import NetsuiteOAuth2, NetsuiteOAuth1


@dlt.source(name="netsuite")
def netsuite_source(credentials: dict = dlt.secrets.value, account_id: str = dlt.secrets.value):
    """Netsuite source and resources"""
    base_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest"
    auth_url = f"{base_url}/auth/oauth2/v1/token"

    auth = NetsuiteOAuth2(auth_endpoint=auth_url, scopes="rest_webservices", **credentials) # OAuth 2.0 option
    #auth=NetsuiteOAuth1(**credentials) # OAuth 1.0 option (deprecated in 2027)

    client = RESTClient(base_url=base_url, auth=auth)

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
        for items in suiteql_query(client, resource, columns, "id"):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="AccountingPeriod",
        write_disposition="replace",
        primary_key="id"
    )
    def accounting_period():
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id"):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="Currency",
        write_disposition="replace",
        primary_key="id"
    )
    def currency():
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id"):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="Department",
        write_disposition="replace",
        primary_key="id"
    )
    def department():
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id"):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="Subsidiary",
        write_disposition="replace",
        primary_key="id"
    )
    def subsidiary():
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id"):
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
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="CustomerSubsidiaryRelationship",
        write_disposition="merge",
        primary_key="id"
    )
    def customer_subsidiary_relationship(incremental=dlt.sources.incremental("lastmodifieddate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="DeletedRecord",
        write_disposition="merge",
        primary_key=("recordtypeid", "recordid")
    )
    def deleted_record(incremental=dlt.sources.incremental("deleteddate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = file_hints("hints/DeletedRecord.json")
        for items in suiteql_query(client, resource, columns, "deleteddate, recordid", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="Employee",
        write_disposition="merge",
        primary_key=("id")
    )
    def employee(incremental=dlt.sources.incremental("lastmodifieddate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="Entity",
        write_disposition="merge",
        primary_key="id"
    )
    def entity(incremental=dlt.sources.incremental("lastmodifieddate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = file_hints("hints/Entity.json")
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="Job",
        write_disposition="merge",
        primary_key="id"
    )
    def job(incremental=dlt.sources.incremental("lastmodifieddate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="TimeBill",
        write_disposition="merge",
        primary_key="id"
    )
    def time_bill(incremental=dlt.sources.incremental("lastmodifieddate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="Transaction",
        write_disposition="merge",
        primary_key="id"
    )
    def transaction(incremental=dlt.sources.incremental("lastmodifieddate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = file_hints("hints/Transaction.json")
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="TransactionAccountingLine",
        write_disposition="merge",
        primary_key=("transaction", "transactionline", "accountingbook")
    )
    def transaction_accounting_line(incremental=dlt.sources.incremental("lastmodifieddate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = file_hints("hints/TransactionAccountingLine.json")
        for items in suiteql_query(client, resource, columns, "transaction, transactionline", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="TransactionLine",
        write_disposition="merge",
        primary_key="uniquekey"
    )
    def transaction_line(incremental=dlt.sources.incremental("linelastmodifieddate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = file_hints("hints/TransactionLine.json")
        for items in suiteql_query(client, resource, columns, "transaction, id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))
        # on delta loads also run a cross filter on transaction for modified rows
        if incremental.start_value and not incremental.end_value:
            cross_filter = {"ftable": "transaction", "fkey": "transaction", "fcursor": "lastmodifieddate"}
            for items in suiteql_query(client, resource, columns, "transaction, id", incremental, cross_filter):
                yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="TransactionLineLink",
        write_disposition="merge",
        primary_key=("nextdoc", "nextline", "previousdoc", "previousline")
    )
    def transaction_line_link(incremental=dlt.sources.incremental("lastmodifieddate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = file_hints("hints/TransactionLineLink.json")
        for items in suiteql_query(client, f"Next{resource}", columns, "previousdoc, nextdoc, previousline, nextline", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))


    return (
        account,
        accounting_period,
        currency,
        customer,
        customer_subsidiary_relationship,
        deleted_record,
        department,
        employee,
        entity,
        job,
        subsidiary,
        time_bill,
        transaction,
        transaction_accounting_line,
        transaction_line,
        transaction_line_link,
    )

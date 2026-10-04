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
        name="account",
        write_disposition="replace",
        primary_key="id"
    )
    def account():
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource); columns.pop("custrecord_level_1_approver", None)
        for items in suiteql_query(client, resource, columns, "id"):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="accounting_period",
        write_disposition="replace",
        primary_key="id"
    )
    def accounting_period():
        resource = "accountingperiod"
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id"):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="currency",
        write_disposition="replace",
        primary_key="id"
    )
    def currency():
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id"):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="department",
        write_disposition="replace",
        primary_key="id"
    )
    def department():
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id"):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="subsidiary",
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
        name="customer",
        write_disposition="merge",
        primary_key="id"
    )
    def customer(incremental=dlt.sources.incremental("last_modified_date", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="customer_subsidiary_relationship",
        write_disposition="merge",
        primary_key="id"
    )
    def customer_subsidiary_relationship(incremental=dlt.sources.incremental("last_modified_date", initial_value=None)):
        resource = "customersubsidiaryrelationship"
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="deleted_record",
        write_disposition="merge",
        primary_key=("recordtypeid", "recordid")
    )
    def deleted_record(incremental=dlt.sources.incremental("deleted_date", initial_value="2026-09-20: 00:00:00")):
        resource = "deletedrecord"
        columns = file_hints("hints/deleted_record.json")
        for items in suiteql_query(client, resource, columns, "deleted_date, record_id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="employee",
        write_disposition="merge",
        primary_key=("id")
    )
    def employee(incremental=dlt.sources.incremental("last_modified_date", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="entity",
        write_disposition="merge",
        primary_key="id"
    )
    def entity(incremental=dlt.sources.incremental("last_modified_date", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = file_hints("hints/entity.json")
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="job",
        write_disposition="merge",
        primary_key="id"
    )
    def job(incremental=dlt.sources.incremental("last_modified_date", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="time_bill",
        write_disposition="merge",
        primary_key="id"
    )
    def time_bill(incremental=dlt.sources.incremental("last_modified_date", initial_value=None)):
        resource = "timebill"
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="transaction",
        write_disposition="merge",
        primary_key="id"
    )
    def transaction(incremental=dlt.sources.incremental("last_modified_date", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = file_hints("hints/Transaction.json")
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="transaction_accounting_line",
        write_disposition="merge",
        primary_key=("transaction", "transaction_line", "accounting_book")
    )
    def transaction_accounting_line(incremental=dlt.sources.incremental("last_modified_date", initial_value=None)):
        resource = "transactionaccountingline"
        columns = file_hints("hints/transaction_accounting_line.json")
        for items in suiteql_query(client, resource, columns, "transaction, transaction_line", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="transaction_line",
        write_disposition="merge",
        primary_key="unique_key"
    )
    def transaction_line(incremental=dlt.sources.incremental("line_last_modified_date", initial_value=None)):
        resource = "transactionline"
        columns = file_hints("hints/transaction_line.json")
        for items in suiteql_query(client, resource, columns, "transaction, id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))
        # on delta loads also run a cross filter on transaction for modified rows
        if incremental.start_value and not incremental.end_value:
            cross_filter = {"ftable": "transaction", "fkey": "transaction", "fcursor": "last_modified_date"}
            for items in suiteql_query(client, resource, columns, "transaction, id", incremental, cross_filter):
                yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="transaction_line_link",
        write_disposition="merge",
        primary_key=("next_doc", "next_line", "previous_doc", "previous_line", "link_type")
    )
    def transaction_line_link(incremental=dlt.sources.incremental("last_modified_date", initial_value=None)):
        resource = "transactionlinelink"
        columns = file_hints("hints/transaction_line_link.json")
        sort = "previous_doc, next_doc, previous_line, next_line, link_type"
        for items in suiteql_query(client, f"Next{resource}", columns, sort, incremental):
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

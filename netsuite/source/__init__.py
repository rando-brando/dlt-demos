import dlt
from dlt.sources.helpers.rest_client import RESTClient

from source.helpers import metadata_hints, schema_hints, suiteql_query
from source.auth import NetsuiteOAuth2, NetsuiteOAuth1


@dlt.source(name="netsuite")
def netsuite_source(auth: NetsuiteOAuth2 = dlt.secrets.value):
    """Netsuite source and resources"""
    client = RESTClient(base_url=auth.base_url, auth=auth)

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
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id"):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="accounting_period",
        table_name="accountingPeriod",
        write_disposition="replace",
        primary_key="id"
    )
    def accounting_period():
        resource = dlt.current.resource().table_name
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
        name="consolidated_exchange_rate",
        table_name="consolidatedExchangeRate",
        write_disposition="merge",
        primary_key="id"
    )
    def consolidated_exchange_rate():
        resource = dlt.current.resource().table_name
        columns = metadata_hints(client, resource)
        filter=None
        if not dlt.current.pipeline().first_run:
            filter = (
                "EXISTS ("
                "\n\tSELECT 1"
                "\n\tFROM AccountingPeriod ap"
                "\n\tWHERE tbl.postingperiod = ap.id"
                "\n\t  AND ap.closedondate IS NULL"
                "\n)"
            ) # fetch and reload open posting periods
        for items in suiteql_query(client, resource, columns, "id", None, filter):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="customer",
        write_disposition="merge",
        primary_key="id"
    )
    def customer(incremental=dlt.sources.incremental("lastModifiedDate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        [columns.pop(c) for c in ("alcoholRecipientType", "negativeNumberFormat", "numberFormat", "shippingCarrier", "thirdPartyCarrier")]
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="customer_subsidiary_relationship",
        table_name="customerSubsidiaryRelationship",
        write_disposition="merge",
        primary_key="id"
    )
    def customer_subsidiary_relationship(incremental=dlt.sources.incremental("lastModifiedDate", initial_value=None)):
        resource = dlt.current.resource().table_name
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="currency_rate",
        table_name="currencyRate",
        write_disposition="merge",
        primary_key="id"
    )
    def currency_rate(incremental=dlt.sources.incremental("lastModifiedDate", initial_value=None)):
        resource = dlt.current.resource().table_name
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="deleted_record",
        table_name="deletedRecord",
        write_disposition="merge",
        primary_key=("recordTypeId", "recordId")
    )
    def deleted_record(incremental=dlt.sources.incremental("deletedDate", initial_value="2026-09-20: 00:00:00")):
        resource = dlt.current.resource().table_name
        columns = schema_hints("schemas/deleted_record.yaml")
        for items in suiteql_query(client, resource, columns, "deleteddate, recordid", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="employee",
        write_disposition="merge",
        primary_key="id"
    )
    def employee(incremental=dlt.sources.incremental("lastModifiedDate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource); columns.pop("gender")
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="entity",
        write_disposition="merge",
        primary_key="id"
    )
    def entity(incremental=dlt.sources.incremental("lastModifiedDate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = schema_hints("schemas/entity.yaml")
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="job",
        write_disposition="merge",
        primary_key="id"
    )
    def job(incremental=dlt.sources.incremental("lastModifiedDate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = metadata_hints(client, resource)
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="time_bill",
        table_name="timeBill",
        write_disposition="merge",
        primary_key="id"
    )
    def time_bill(incremental=dlt.sources.incremental("lastModifiedDate", initial_value=None)):
        resource = dlt.current.resource().table_name
        columns = metadata_hints(client, resource) | schema_hints("schemas/time_bill.yaml")
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="transaction",
        write_disposition="merge",
        primary_key="id"
    )
    def transaction(incremental=dlt.sources.incremental("lastModifiedDate", initial_value=None)):
        resource = dlt.current.resource_name()
        columns = schema_hints("schemas/transaction.yaml")
        for items in suiteql_query(client, resource, columns, "id", incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="transaction_accounting_line",
        table_name="transactionAccountingLine",
        write_disposition="merge",
        merge_key="transactionId",
        primary_key=("transactionId", "transactionLineId", "accountingBookId")
    )
    def transaction_accounting_line(incremental=dlt.sources.incremental("lastModifiedDate", initial_value=None)):
        resource = dlt.current.resource().table_name
        columns = schema_hints("schemas/transaction_accounting_line.yaml")
        filter=None
        if incremental.start_value and not incremental.end_value:
            filter = (
                "transaction IN ("
                "\n\tSELECT transaction"
                f"\n\tFROM TransactionLine"
                f"\n\tWHERE linelastmodifieddate > TO_TIMESTAMP('{incremental.last_value}', 'YYYY-MM-DD HH24:MI:SS')"
                "\n)"
            ) # fetch and reload all lines associated with a transaction if any have changed to capture deletes
        for items in suiteql_query(client, resource, columns, "transaction, transactionline", not filter and incremental, filter):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="transaction_line",
        table_name="transactionLine",
        write_disposition="merge",
        merge_key="transactionId",
        primary_key=("transactionId", "id")
    )
    def transaction_line(incremental=dlt.sources.incremental("lineLastModifiedDate", initial_value=None)):
        resource = dlt.current.resource().table_name
        columns = schema_hints("schemas/transaction_line.yaml")
        filter=None
        if incremental.start_value and not incremental.end_value:
            filter = (
                "transaction IN ("
                "\n\tSELECT transaction"
                f"\n\tFROM {resource}"
                f"\n\tWHERE linelastmodifieddate > TO_TIMESTAMP('{incremental.last_value}', 'YYYY-MM-DD HH24:MI:SS')"
                "\n)"
            ) # fetch and reload all lines associated with a transaction if any have changed to capture deletes
        for items in suiteql_query(client, resource, columns, "transaction, id", not filter and incremental, filter):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="transaction_line_link",
        table_name="transactionLineLink",
        write_disposition="merge",
        primary_key=("nextDoc", "nextLine", "previousDoc", "previousLine", "linkType")
    )
    def transaction_line_link(incremental=dlt.sources.incremental("lastModifiedDate", initial_value=None)):
        resource = dlt.current.resource().table_name
        columns = schema_hints("schemas/transaction_line_link.yaml")
        sort = "previousdoc, nextdoc, previousline, nextline, linktype"
        for items in suiteql_query(client, f"Next{resource}", columns, sort, incremental):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))


    return (
        account,
        accounting_period,
        consolidated_exchange_rate,
        currency,
        currency_rate,
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

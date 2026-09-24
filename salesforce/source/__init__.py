import dlt
from simple_salesforce import Salesforce

from source.helpers import metadata_hints, soql_query


@dlt.source(name="salesforce")
def salesforce_source(credentials: dict = dlt.secrets.value):
    """Salesforce source and resources"""
    sf = Salesforce(**credentials) # initialize simple salesforce client

    # ─────────────────────────────────────────────
    # FULL LOAD (replace)
    # ─────────────────────────────────────────────
    @dlt.resource(
        name="RecordType",
        write_disposition="replace",
        primary_key="Id",
        columns={"Id": {"data_type": "text", "precision": 18, "unique": True}}
    )
    def record_type():
        sobject = dlt.current.resource_name()
        columns = metadata_hints(sf, sobject)
        for items in soql_query(sf, sobject, columns):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    # ─────────────────────────────────────────────
    # INCREMENTAL (merge)
    # ─────────────────────────────────────────────
    @dlt.resource(
        name="Account",
        write_disposition="merge",
        primary_key="Id",
        columns={"Id": {"data_type": "text", "precision": 18, "unique": True}}
    )
    def account(incremental=dlt.sources.incremental("SystemModstamp",initial_value=None)):
        sobject = dlt.current.resource_name()
        columns = metadata_hints(sf, sobject)
        for items in soql_query(sf, sobject, columns, "SystemModstamp", incremental.last_value):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="Contact",
        write_disposition={"disposition": "merge", "strategy": "insert-only"},
        primary_key="Id",
        columns={"Id": {"data_type": "text", "precision": 18, "unique": True}}
    )
    def contact(incremental=dlt.sources.incremental("CreatedDate", initial_value=None)):
        sobject = dlt.current.resource_name()
        columns = metadata_hints(sf, sobject)
        for items in soql_query(sf, sobject, columns, "CreatedDate", incremental.last_value):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="Lead",
        write_disposition="merge",
        primary_key="Id",
        columns={"Id": {"data_type": "text", "precision": 18, "unique": True}}
    )
    def lead(incremental=dlt.sources.incremental("SystemModstamp", initial_value=None)):
        sobject = dlt.current.resource_name()
        columns = metadata_hints(sf, sobject)
        for items in soql_query(sf, sobject, columns, "SystemModstamp", incremental.last_value):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))

    @dlt.resource(
        name="Opportunity",
        write_disposition="merge",
        primary_key="Id",
        columns={"Id": {"data_type": "text", "precision": 18, "unique": True}}
    )
    def opportunity(incremental=dlt.sources.incremental("SystemModstamp",initial_value=None)):
        sobject = dlt.current.resource_name()
        columns = metadata_hints(sf, sobject)
        for items in soql_query(sf, sobject, columns, "SystemModstamp", incremental.last_value):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))


    return (
        account,
        contact,
        lead,
        opportunity,
        record_type,
    )
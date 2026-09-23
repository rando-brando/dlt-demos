import dlt
import duckdb
import io
from simple_salesforce import Salesforce


def column_hints(sf: Salesforce, sobject: str):
    """Return dlt column hints from /describe endpoint"""
    meta = getattr(sf, sobject).describe()

    columns = {}
    for f in meta["fields"]:
        if f["type"] in ("currency", "double", "percent"):
            columns[f["name"]] = {"data_type": "decimal", "precision": 18, "scale": 2}
        elif f["type"] == "datetime":
            columns[f["name"]] = {"data_type": "timestamp", "precision": 7, "timezone": False}
        elif 0 < f["length"] <= 4000: # mssql max text size
            columns[f["name"]] = {"data_type": "text", "precision": f["length"]}
        elif f["type"] not in ("address", "location"):
            columns[f["name"]] = {}

    return columns


def soql_query(sf: Salesforce, sobject: str, columns: dict, cursor: str = None, last_value: str = None):
        """Bulk-query an SObject, using query_all to include deleted rows."""

        soql = "SELECT {fields} FROM {sobject}".format(fields=", ".join(columns.keys()), sobject=sobject)
        if cursor and last_value:
            soql += f" WHERE {cursor} > {last_value}"

        con = duckdb.connect(":memory:")
        for csv_chunk in getattr(sf.bulk2, sobject).query_all(soql):
            page = con.read_csv(io.StringIO(csv_chunk), all_varchar=True)
            yield page.arrow()


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
        columns = column_hints(sf, sobject)
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
        columns = column_hints(sf, sobject)
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
        columns = column_hints(sf, sobject)
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
        columns = column_hints(sf, sobject)
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
        columns = column_hints(sf, sobject)
        for items in soql_query(sf, sobject, columns, "SystemModstamp", incremental.last_value):
            yield dlt.mark.with_hints(items, dlt.mark.make_hints(columns=columns))


    return (
        account,
        contact,
        lead,
        opportunity,
        record_type,
    )

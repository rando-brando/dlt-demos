from simple_salesforce import Salesforce
import duckdb
import io


def metadata_hints(sf: Salesforce, sobject: str):
    """Return dlt column hints from /describe endpoint"""
    meta = getattr(sf, sobject).describe()

    hints = {}
    for fields in meta["fields"]:
        if fields["name"] == "Id":
            hints["Id"] == {"data_type": "text", "precision": 18, "unique": True}
        elif fields["type"] in ("currency", "double", "percent"):
            hints[fields["name"]] = {"data_type": "decimal", "precision": fields["precision"], "scale": fields["scale"]}
        elif fields["type"] == "datetime":
            hints[fields["name"]] = {"data_type": "timestamp", "precision": 7, "timezone": False}
        elif 0 < fields["length"] <= 4000: # mssql max text size
            hints[fields["name"]] = {"data_type": "text", "precision": fields["length"]}
        elif fields["type"] not in ("address", "location"):
            hints[fields["name"]] = {}

    return hints


def soql_query(
        sf: Salesforce,
        sobject: str,
        columns: dict,
        cursor: str = None,
        last_value: str = None
    ):
    """Bulk-query an SObject, using query_all to include deleted rows."""

    soql = "SELECT {fields} FROM {sobject}".format(fields=", ".join(columns.keys()), sobject=sobject)
    if cursor and last_value:
        soql += f" WHERE {cursor} > {last_value}"

    con = duckdb.connect(":memory:")
    for csv_chunk in getattr(sf.bulk2, sobject).query_all(soql):
        page = con.read_csv(io.StringIO(csv_chunk), all_varchar=True)
        yield page.arrow()
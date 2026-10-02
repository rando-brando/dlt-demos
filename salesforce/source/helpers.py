import io

import duckdb
from dlt.common import logger
from simple_salesforce import Salesforce


def metadata_hints(sf: Salesforce, sobject: str):
    """Return dlt column hints from /describe endpoint"""
    meta = getattr(sf, sobject).describe()

    hints = {}
    for fields in meta["fields"]:
        if fields["name"] == "Id":
            hints["Id"] = {"data_type": "text", "precision": 18, "unique": True}
        elif fields["type"] in ("currency", "double", "percent"):
            hints[fields["name"]] = {"data_type": "decimal", "precision": fields["precision"], "scale": fields["scale"]}
        elif fields["type"] == "datetime":
            hints[fields["name"]] = {"data_type": "timestamp", "precision": 7, "timezone": False}
        elif fields["type"] == "date":
            hints[fields["name"]] = {"data_type": "date"}
        elif fields["type"] == "boolean":
            hints[fields["name"]] = {"data_type": "bool"}
        elif 0 < fields["length"] <= 4000: # mssql max text size
            hints[fields["name"]] = {"data_type": "text", "precision": fields["length"]}
        elif fields["type"] not in ("address", "location"):
            hints[fields["name"]] = {}

    return hints


def _format_value(data_type: str, value):
    """Format an incremental cursor value for inline use in a SOQL WHERE clause."""
    # SOQL date and dateTime literals must be bare (unquoted)
    if data_type == "timestamp":
        return value.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return f"'{value}'"


def soql_query(
        sf: Salesforce,
        sobject: str,
        columns: dict,
        incremental=None
    ):
    """Bulk-query an SObject, using query_all to include deleted rows."""

    soql = "SELECT\n\t{fields}\nFROM {sobject}".format(fields=",\n\t".join(columns.keys()), sobject=sobject)

    if incremental:
        cursor = incremental.cursor_path
        data_type = columns[cursor]["data_type"]

        conditions = []
        if incremental.last_value:
            conditions.append(f"{cursor} > {_format_value(data_type, incremental.last_value)}")
        if incremental.end_value:
            conditions.append(f"{cursor} <= {_format_value(data_type, incremental.end_value)}")
        if conditions:
            soql += "\nWHERE " + "\nAND ".join(conditions)

    logger.info(f"Generated SOQL query string:\n{soql}\n")

    dtypes_map = {"decimal": "DOUBLE", "bool": "BOOLEAN", "timestamp": "TIMESTAMP", "date": "DATE"}
    dtypes = {c: dtypes_map.get(h.get("data_type"), "VARCHAR") for c, h in columns.items()}

    con = duckdb.connect(":memory:")
    for csv_chunk in getattr(sf.bulk2, sobject).query_all(soql):
        page = con.read_csv(io.StringIO(csv_chunk), dtype=dtypes)
        yield page.arrow()
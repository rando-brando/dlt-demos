import io

import pyarrow as pa
import pyarrow.csv as csv
from dlt.common import logger
from simple_salesforce import Salesforce


def metadata_hints(client: Salesforce, sobject: str):
    """Return dlt column hints from /describe endpoint"""
    meta = getattr(client, sobject).describe()

    hints = {}
    for fields in meta["fields"]:
        if fields["name"] == "Id":
            hints["Id"] = {"data_type": "text", "precision": 18, "unique": True}
        elif fields["type"] in ("currency", "double", "percent") and fields["scale"] == 0:
            hints[fields["name"]] = {"data_type": "bigint"}
        elif fields["type"] in ("currency", "double", "percent"):
            hints[fields["name"]] = {"data_type": "double"}
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


def _arrow_type(hint: dict) -> pa.DataType:
    """Map a dlt column hint to the pyarrow type used to parse that SOQL CSV column."""
    data_type = hint.get("data_type")
    if data_type == "decimal":
        return pa.decimal128(hint["precision"], hint["scale"])
    if data_type == "double":
        return pa.float64()
    if data_type == "bigint":
        return pa.int64()
    if data_type == "bool":
        return pa.bool_()
    if data_type == "timestamp":
        return pa.timestamp("us", tz="UTC")  # Bulk API emits "Z"; parsed tz-aware, stripped to naive below
    if data_type == "date":
        return pa.date32()
    return pa.string()


def _format_value(data_type: str, value):
    """Format an incremental cursor value for inline use in a SOQL WHERE clause."""
    # SOQL date and datetime literals must be bare (unquoted)
    if data_type == "timestamp":
        return value.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return f"'{value}'"


def soql_query(
        client: Salesforce,
        sobject: str,
        columns: dict,
        incremental = None,
        filter: str = None
    ):
    """Bulk-query an SObject, using query_all to include deleted rows."""

    soql = "SELECT\n\t{fields}\nFROM {sobject}".format(fields=",\n\t".join(columns.keys()), sobject=sobject)

    conditions = [filter] if filter else [] # use customer filter if provided

    if incremental:
        cursor = incremental.cursor_path
        data_type = columns[cursor]["data_type"]

        if incremental.last_value:
            conditions.append(f"{cursor} > {_format_value(data_type, incremental.last_value)}")
        if incremental.end_value:
            conditions.append(f"{cursor} <= {_format_value(data_type, incremental.end_value)}")

    if conditions:
        soql += "\nWHERE " + "\nAND ".join(conditions)

    logger.info(f"Generated SOQL query string:\n{soql}\n")

    column_types = {c: pa.float64() if h.get("data_type") == "bigint" else _arrow_type(h) for c, h in columns.items()}
    convert_options = csv.ConvertOptions(column_types=column_types, strings_can_be_null=True, null_values=[""])
    parse_options = csv.ParseOptions(newlines_in_values=True)

    for csv_chunk in getattr(client.bulk2, sobject).query_all(soql):
        table = csv.read_csv(io.BytesIO(csv_chunk.encode("utf-8")), convert_options=convert_options, parse_options=parse_options)
        schema = pa.schema({c: _arrow_type(h) for c, h in columns.items()})
        yield table.cast(schema, safe=False)

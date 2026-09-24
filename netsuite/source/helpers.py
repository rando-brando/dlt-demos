from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
import json


def file_hints(path: str):
    """Return dlt column hints from file"""
    return json.load(open(path))


def metadata_hints(client: RESTClient, resource: str):
    """Return dlt column hints from /metadata-catalog endpoint"""
    meta = client.get(
        f"record/v1/metadata-catalog/{resource}",
        headers={"Accept": "application/schema+json"}
    ).json()

    DATA_TYPE_HINTS = {
        "boolean": {"data_type": "bool"},
        "double": {"data_type": "double"},
        "float": {"data_type": "double"},
        "int64": {"data_type": "bigint"},
        "date": {"data_type": "date"},
        "date-time": {"data_type": "timestamp", "precision": 7, "timezone": False}
    }

    fields = [
        {
            "name": name, # column name
            "lname": name.lower(),# lowercase column name
            "type": field.get("type"), # field type
            "format": field.get("format") # data type
        }
        for name, field in meta.get("properties").items() # fields meta
        if name in meta.get("x-ns-filterable") # selectable fields list
    ]

    hints = {}
    for field in fields:
        if field["name"] == "id":
            hints[field["lname"]] = {"name": field["name"], "data_type": "bigint", "unique": True}
        elif field["format"]:
            hints[field["lname"]] = {"name": field["name"]} | DATA_TYPE_HINTS.get(field["format"])
            # suiteql transformations
            if field["format"] == "date":
                hints[field["lname"]]["x-annotation-xform"] = f"TO_CHAR({field["name"]}, 'YYYY-MM-DD')"
            if field["format"] == "date-time":
                hints[field["lname"]]["x-annotation-xform"] = f"TO_CHAR({field["name"]}, 'YYYY-MM-DD HH24:MI:SS')"
        elif field["type"] == "object":
            # the object's internal id
            hints[field["lname"] + "id"] = {
                "name": field["name"] + "Id",
                "data_type": "bigint",
                "x-annotation-xform": f"{field["name"]}" # suiteql transformation
            }
            # the object's text value
            hints[field["lname"]] = {
                "name": field["name"],
                "data_type": "text",
                "x-annotation-xform": f"BUILTIN.DF({field["name"]})" # suiteql transformation
            }
        elif field["type"] == "string":
            hints[field["lname"]] = {"name": field["name"], "data_type": "text", "precision": 4000} # mssql max text size
        else:
            hints[field["lname"]] = {"name": field["name"]} | DATA_TYPE_HINTS.get(field["type"])

    return hints


def suiteql_query(
        client: RESTClient,
        resource: str,
        columns: dict,
        sort: str = None,
        incremental=None
    ):
    """Page through resource via NetSuite's SuiteQL endpoint."""

    fields = [
        f'{columns[f]["x-annotation-xform"]} AS {f}' # xform AS alias
        if "x-annotation-xform" in columns[f] else f
        for f in columns
    ]
    suiteql = "SELECT {fields} FROM {resource}".format(fields=", ".join(fields), resource=resource)

    if incremental and incremental.last_value:
        cursor, last_value = incremental.cursor_path, incremental.last_value
        data_type = columns[cursor]["data_type"]
        if data_type == "date":
            last_value = f"TO_DATE('{last_value}', 'YYYY-MM-DD')" # convert to compatible date
        if data_type == "timestamp":
            last_value = f"TO_TIMESTAMP('{last_value}', 'YYYY-MM-DD HH24:MI:SS')" # convert to compatible timestamp
        suiteql += f" WHERE {cursor} > {last_value}"

    if sort:
        suiteql += f" ORDER BY {sort}"

    yield from client.paginate(
        "query/v1/suiteql",
        method="POST",
        headers={"Prefer": "transient", "Content-Type": "application/json"},
        json={"q": suiteql},
        data_selector="items",
        paginator=OffsetPaginator(limit=1000, total_path=None, has_more_path="hasMore"),
    )
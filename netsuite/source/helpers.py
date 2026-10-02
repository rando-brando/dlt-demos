import json

from dlt.common import logger
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator


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
            "type": field.get("type"), # field type
            "format": field.get("format"), # data type
            "properties": field.get("properties") # object properties
        }
        for name, field in meta.get("properties").items() # fields meta
        if name in meta.get("x-ns-filterable") # selectable fields list
    ]
    fields.sort(key=lambda f: f["name"].lower() != "id") # id first

    hints = {}
    for field in fields:
        name = field["name"].lower()
        if name == "id":
            hints[name] = {"data_type": "bigint", "unique": True}
        elif field["format"]:
            hints[name] = {} | DATA_TYPE_HINTS.get(field["format"])
            # suiteql transformations
            if field["format"] == "date":
                hints[name]["x-annotation-xform"] = f"TO_CHAR({name}, 'YYYY-MM-DD')"
            if field["format"] == "date-time":
                hints[name]["x-annotation-xform"] = f"TO_CHAR({name}, 'YYYY-MM-DD HH24:MI:SS')"
        elif field["type"] == "object":
            # objects with "links" require BUILTIN.DF to access thier text value
            if field["properties"].get("links") and name[-3:] != "_id":
                # the object's internal id
                hints[name + "_id"] = {
                    "data_type": "text",
                    "x-annotation-xform": name # suiteql transformation
                }
                # the object's text value
                hints[name] = {
                    "data_type": "text",
                    "x-annotation-xform": f"BUILTIN.DF({name})" # suiteql transformation
                }
            else:
                hints[name] = {"data_type": "text"}
        elif field["type"] == "string":
            hints[name] = {"data_type": "text"}
        else:
            hints[name] = {} | DATA_TYPE_HINTS.get(field["type"], {})

    return hints


def _format_value(data_type: str, value):
    """Format an incremental cursor value for inline use in a SuiteQL WHERE clause."""
    if data_type == "date":
        return f"TO_DATE('{value}', 'YYYY-MM-DD')" # convert to compatible date
    if data_type == "timestamp":
        return f"TO_TIMESTAMP('{value}', 'YYYY-MM-DD HH24:MI:SS')" # convert to compatible timestamp
    return value


def suiteql_query(
        client: RESTClient,
        resource: str,
        columns: dict,
        sort: str = None,
        incremental = None,
        xfilter: dict = None # cross resource filter
    ):
    """Page through resource via NetSuite's SuiteQL endpoint."""

    fields = [
        f'\t{columns[f]["x-annotation-xform"]} AS {f}' # xform AS alias
        if "x-annotation-xform" in columns[f] else f'\t{f}'
        for f in columns
    ]
    suiteql = "SELECT\n{fields}\nFROM {resource}".format(fields=",\n".join(fields), resource=resource)

    if incremental:
        cursor = incremental.cursor_path
        data_type = columns[cursor]["data_type"]

        if xfilter:
            suiteql += f"""\nWHERE {xfilter["fkey"]} IN (
                        SELECT id
                        FROM {xfilter["ftable"]}
                        WHERE {xfilter["fcursor"]} > {_format_value(data_type, incremental.last_value)}
                    )"""
        else:
            conditions = []
            if incremental.last_value:
                conditions.append(f"{cursor} > {_format_value(data_type, incremental.last_value)}")
            if incremental.end_value:
                conditions.append(f"{cursor} <= {_format_value(data_type, incremental.end_value)}")
            if conditions:
                suiteql += "\nWHERE " + "\nAND ".join(conditions)

    if sort:
        suiteql += f"\nORDER BY {sort}"

    logger.info(f"Generated SuiteQL query string:\n{suiteql}\n")

    yield from client.paginate(
        "query/v1/suiteql",
        method="POST",
        headers={"Prefer": "transient", "Content-Type": "application/json"},
        json={"q": suiteql},
        data_selector="items",
        paginator=OffsetPaginator(limit=1000, total_path=None, has_more_path="hasMore"),
    )


def numeric_max(values):
    """max() that compares numeric-looking values as numbers (NetSuite returns ids as strings)."""
    def key(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return v
    return max(values, key=key)
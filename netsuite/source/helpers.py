import yaml

from dlt.common import logger
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator


def schema_hints(path: str):
    """Return dlt column hints for a table from a dlt schema yaml file.

    x-transformation defaults to the lowercased column name; set it in the yaml only when it differs."""
    with open(path) as f:
        schema = yaml.safe_load(f)
    return {
        name: {"x-transformation": name.lower(), **hints}
        for name, hints in schema["columns"].items()
        if not name.startswith("_dlt_")
    }


def metadata_hints(client: RESTClient, resource: str):
    """Return dlt column hints from /metadata-catalog endpoint"""
    meta = client.get(
        f"record/v1/metadata-catalog/{resource}",
        headers={"Accept": "application/schema+json"}
    ).json()

    DATA_TYPE_HINTS = {
        "boolean": {"data_type": "bool"},
        "date": {"data_type": "date"},
        "date-time": {"data_type": "timestamp", "precision": 7, "timezone": False},
        "double": {"data_type": "double"},
        "float": {"data_type": "double"},
        "int64": {"data_type": "bigint"},
        "object": {"data_type": "text", "precision": 900},
        "string": {"data_type": "text", "precision": 4000}
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
    fields.sort(key=lambda f: f["name"] != "id") # id first

    hints = {}
    for field in fields:
        name = raw = field["name"] # camelCase name from the metadata catalog
        if name == "id":
            hints[name] = {"data_type": "bigint", "unique": True, "x-transformation": raw}
        elif field["format"]:
            hints[name] = DATA_TYPE_HINTS.get(field["format"], {}) | {"x-transformation": raw}
            # suiteql transformations
            if field["format"] == "date":
                hints[name]["x-transformation"] = f"TO_CHAR({raw}, 'YYYY-MM-DD')"
            if field["format"] == "date-time":
                hints[name]["x-transformation"] = f"TO_CHAR({raw}, 'YYYY-MM-DD HH24:MI:SS')"
        # objects require BUILTIN.DF to access their text value
        elif field["type"] == "object" and not name.endswith(("Id", "_id")):
            """ Object fields return an id value whose title value can be accessed using Netsuite's BUILTIN.DF
            The id value is generally an integer with some exceptions:
            1. ids of type enum are categorical text (most common)
            2. fields of type dropdown are categorical text (rare)
            3. array objects are comma separated ids (occasional)
            """
            is_enum = field.get("properties", {}).get("id", {}).get("enum") # enum ids
            is_dropdown = name.startswith("dropdown")
            has_items = field.get("properties", {}).get("items") # array of ids
            
            hint = "object" if is_enum or has_items or is_dropdown else "int64"
            # the object's internal id
            hints[name + ("_id" if "_" in name else "Id")] = DATA_TYPE_HINTS.get(hint, {}) | {"x-transformation": raw}
            # the object's text value
            hints[name] = DATA_TYPE_HINTS.get("string", {}) | {"x-transformation": f"BUILTIN.DF({raw})"}
        else:
            hints[name] = DATA_TYPE_HINTS.get(field["type"], {}) | {"x-transformation": raw}

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
        filter: str = None
    ):
    """Page through resource via NetSuite's SuiteQL endpoint."""

    fields = [f'\t{columns[f]["x-transformation"]} AS {f}' for f in columns] # x-transformation AS alias
    suiteql = "SELECT\n{fields}\nFROM {resource} tbl".format(fields=",\n".join(fields), resource=resource)

    conditions = [filter] if filter else [] # use customer filter if provided

    if incremental:
        cursor = incremental.cursor_path
        data_type = columns[cursor]["data_type"]

        if incremental.last_value:
            conditions.append(f"{cursor} > {_format_value(data_type, incremental.last_value)}")
        if incremental.end_value:
            conditions.append(f"{cursor} <= {_format_value(data_type, incremental.end_value)}")

    if conditions:
        suiteql += "\nWHERE " + "\nAND ".join(conditions)

    if sort:
        suiteql += f"\nORDER BY {sort}"

    logger.info(f"Generated SuiteQL query string:\n{suiteql}\n")

    # SuiteQL returns all keys lowercased; restore the camelCase column names
    names = {c.lower(): c for c in columns}

    for items in client.paginate(
        "query/v1/suiteql",
        method="POST",
        headers={"Prefer": "transient", "Content-Type": "application/json"},
        json={"q": suiteql},
        data_selector="items",
        paginator=OffsetPaginator(limit=1000, total_path=None, has_more_path="hasMore"),
    ):
        yield [{names.get(k, k): v for k, v in item.items()} for item in items]


def numeric_max(values):
    """max() that compares numeric-looking values as numbers (NetSuite returns ids as strings)."""
    def key(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return v
    return max(values, key=key)
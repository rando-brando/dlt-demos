import argparse
import dlt
from dlt.common import logger

from source import netsuite_source
from source.helpers import numeric_max


def load_resource(resource_name: str, field: str = None, start=None, end=None) -> None:
    """Load a single named resource from Netsuite"""

    pipeline = dlt.pipeline(
        pipeline_name=f"Netsuite_{resource_name}",
        destination="duckdb",
        dataset_name="nts",
        progress="log"
    )

    source = netsuite_source()
    if resource_name not in source.resources:
        logger.error(f"Resource '{resource_name}' not implemented.")
    else:
        resource = source.resources[resource_name]
        if field:
            resource = resource(incremental=dlt.sources.incremental(field, initial_value=start, end_value=end, range_end="closed", last_value_func=numeric_max))
        logger.info(f"Running load for resource: {resource_name}")
        load_info = pipeline.run(resource, loader_file_format="parquet")
        print(load_info)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("resource_name")
    parser.add_argument("--field", help="Cursor field to bind a custom incremental to, e.g. id")
    parser.add_argument("--start", help="initial_value for --field")
    parser.add_argument("--end", help="end_value for --field")
    args = parser.parse_args()

    load_resource(args.resource_name, args.field, args.start, args.end)

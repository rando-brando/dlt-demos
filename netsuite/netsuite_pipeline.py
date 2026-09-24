import dlt
import sys

from source import netsuite_source


def load_resource(resource_name: str) -> None:
    """Load a single named resource from Netsuite"""

    pipeline = dlt.pipeline(
        pipeline_name=f"Netsuite_{resource_name}",
        destination=dlt.destinations.duckdb("netsuite.duckdb"),
        dataset_name="raw_nts",
        progress="log"
    )

    source = netsuite_source()
    if resource_name not in source.resources:
        print(f"[[ERROR]]: Resource '{resource_name}' not implemented.")
    else:
        print(f"[[INFO]]: Running load for resource: {resource_name}", end="\n\n")
        resource = source.with_resources(resource_name)
        load_info = pipeline.run(resource, loader_file_format="parquet")
        print(load_info)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        load_resource(sys.argv[1])
    else:
        print("[[ERROR]]: Missing required argument for resource_name: python [pipeline_file] [resource_name].")

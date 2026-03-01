"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


# Default ISBNs for the Books API (batch lookup by bibkeys)
DEFAULT_BIBKEYS = (
    "ISBN:0451526538,ISBN:9780140328721,ISBN:0385472579,ISBN:9780980200447,"
    "ISBN:0201558025,ISBN:0140328726,ISBN:0547928227,ISBN:0439023521"
)


@dlt.source
def open_library_rest_api_source(bibkeys: str = DEFAULT_BIBKEYS):
    """Define dlt resources from Open Library REST API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org/",
            # Open Library Books API is public; no authentication required
        },
        "resource_defaults": {
            "primary_key": "key",
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "api/books",
                    "method": "GET",
                    "params": {
                        "bibkeys": bibkeys,
                        "format": "json",
                        "jscmd": "data",
                    },
                    "data_selector": "$.*",
                    "paginator": {
                        "type": "single_page",
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201

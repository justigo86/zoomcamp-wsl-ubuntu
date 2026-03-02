"""NYC Yellow Taxi REST API pipeline using dlt."""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

@dlt.resource(name="nyc_yellow_taxi_trips", write_disposition="replace")
def taxi_pipeline(): # Changed name as requested
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net"
    )
    
    # We explicitly tell it NOT to expect a total count in the response
    paginator = PageNumberPaginator(base_page=1, page_param="page", total_path=None)

    for page in client.paginate(
        "data_engineering_zoomcamp_api",
        paginator=paginator
    ):
        # If the API returns an empty list [], we stop the generator
        if not page:
            break
        yield page

pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline())
    print(load_info)  # noqa: T201

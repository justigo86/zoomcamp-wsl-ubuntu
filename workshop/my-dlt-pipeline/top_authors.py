import marimo

__generated_with = "0.20.2"
app = marimo.App()


@app.cell
def __():
    import marimo as mo

    mo.md(
        """
        # Top 10 Authors by Book Count

        This notebook uses **dlt** dataset access with **Ibis** to query the Open Library pipeline
        and visualize the top 10 authors by number of books.

        **Prerequisites:** `pip install marimo "ibis-framework[duckdb]" altair`
        Run from the pipeline directory: `marimo edit top_authors.py`
        """
    )
    return (mo,)


@app.cell
def _():
    import dlt

    pipeline = dlt.pipeline(
        pipeline_name="open_library_pipeline",
        destination="duckdb",
    )
    dataset = pipeline.dataset()
    dataset_name = pipeline.dataset_name
    ibis_conn = dataset.ibis()
    return dataset_name, ibis_conn


@app.cell
def _(dataset_name, ibis_conn):
    authors_table = ibis_conn.table("books__authors", database=dataset_name)
    agg = authors_table.group_by(authors_table.name).aggregate(
        book_count=authors_table.name.count()
    )
    top_authors_expr = agg.order_by(agg.book_count.desc()).limit(10)
    return (top_authors_expr,)


@app.cell
def _(mo, top_authors_expr):
    top_authors_df = top_authors_expr.execute()
    mo.output.replace(mo.ui.table(top_authors_df))
    return (top_authors_df,)


@app.cell
def _(top_authors_df):
    import altair as alt

    chart = (
        alt.Chart(top_authors_df)
        .mark_bar()
        .encode(
            x=alt.X("book_count:Q", title="Number of books"),
            y=alt.Y("name:N", title="Author", sort="-x"),
            tooltip=["name", "book_count"],
        )
        .properties(
            title="Top 10 authors by book count",
            width=500,
            height=320,
        )
    )
    chart
    return


if __name__ == "__main__":
    app.run()

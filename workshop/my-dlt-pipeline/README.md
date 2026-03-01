## The Open Library REST API source is implemented and running successfully.
## Summary
File: workshop/my-dlt-pipeline/open_library_pipeline.py
Configuration:
Base URL: https://openlibrary.org/
Endpoint: api/books (as in open_library-docs.yaml)
Auth: None (public API)
Params: bibkeys, format=json, jscmd=data
Data selector: $.* (extracts book objects from the root)
Paginator: single_page (no pagination)
Primary key: key (e.g. /books/OL1017798M)
Write disposition: replace


## Additional prompts:
**"What tables were created in the pipeline?"**
**Main table**
books – one row per book (title, key, url, number_of_pages, publish_date, etc.)
**Child tables (normalized from nested JSON)**
booksauthors
booksclassifications_dewey_decimal_class
booksclassifications_lc_classifications
booksebooks
booksexcerpts
booksidentifiers_amazon
booksidentifiers_goodreads
booksidentifiers_google
booksidentifiers_isbn_10
booksidentifiers_isbn_13
booksidentifiers_lccn
booksidentifiers_librarything
booksidentifiers_oclc
booksidentifiers_openlibrary
booksidentifiers_project_gutenberg
booksidentifiers_wikidata
bookslinks
bookspublish_places
bookspublishers
bookssubject_people
bookssubject_places
bookssubject_times
bookssubjects
bookstable_of_contents

**"Show me the schema for the books table."**
Column	Data type
_dlt_id	text
_dlt_load_id	text
by_statement	text
cover__large	text
cover__medium	text
cover__small	text
key	text
notes	text
number_of_pages	bigint
pagination	text
publish_date	text
subtitle	text
title	text
url	text
weight	text

**"How many rows were loaded?"**
Main table
books: 7 rows (one per book from the default bibkeys).
All tables
Total rows loaded: 480 across the 26 tables (books + normalized child tables).
Breakdown: 7 in books, 9 in books__authors, 316 in books__subjects, and the rest in the other identifier, classification, and link tables.


## Bonus prompt
**"Create a marimo notebook that visualizes the top 10 authors by book count. Use ibis for data access. Reference: https://dlthub.com/docs/general-usage/dataset-access/marimo"**
Summary of what’s in place:
File: workshop/my-dlt-pipeline/top_authors.py
Notebook contents:
Markdown – Short intro and prerequisites (marimo, ibis-framework[duckdb], altair), plus how to run it.
Imports – marimo and dlt.
Pipeline & Ibis – Build the dlt pipeline, get the dataset, and create the Ibis connection with dataset.ibis() as in the dlt marimo docs.
Ibis query – Read books__authors, group by name, count rows as book count, sort by that count descending, take top 10.
Table – Execute the Ibis expression and show the result with mo.ui.table().
Chart – Altair bar chart of top 10 authors by book count.
**Updates needed**
app = mo.App() – Create one app at the top.
@app.cell – Each logical “cell” is a function decorated with @app.cell.
Cell inputs/outputs – Function parameters are the variables a cell uses; the return value is what it defines for other cells.
Markdown – Replaced the markdown # %% block with mo.md(...) in the first cell.
Display – Table and chart are shown via mo.output.replace(mo.ui.table(...)) and by putting chart as the last expression in its cell.
**Updates needed after initial marimo run command**
Imports marimo as mo
Returns (mo,) so other cells can use it

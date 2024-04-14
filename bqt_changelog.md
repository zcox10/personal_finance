# 1
## 1.1.1
* Silence google.Job query progress bar output when PrintTools is muted
## 1.1.0
* Add optional `job_id_prefix` arg to query methods. Value is prepended to the generated BQ job ID
* Set `job_id_prefix` for queries run when using Workbench (`WORKBENCH_PROFILE_PARENT` env var is present)
## 1.0.5
* Consider `bqt.verbose` when printing out information about latest partition
## 1.0.4
* fix flaky test
* include `tqdm` dependency
* include `python-Levenshtein` dependency
## 1.0.3
* Add `return_generator` functionality to `fast_query`
## 1.0.2
* Add `result_format` parameter to `query` to allow download directly to [Polars](https://www.pola.rs/) DataFrames.  Pandas is still default if unspecified.
## 1.0.1
* Uses bq native `to_dataframe` result extraction for faster downloads via `query`.  Adds visual progress bar.
## 1.0.0
* Update the module name to `spotify-bqt` to resolve conflicts with an external library
# 0
## 0.7.4
* Add lsp_dry_run flag to bqt.reader.query_async
## 0.7.3
* use old-style typing to support python <3.9 
## 0.7.2
* add back read_return query
## 0.7.1
* loosen requirement on jinja2 package
## 0.7.0
* Support jinja templated queries
## 0.6.1
* Updates example for bqt, and removes query_async default job-id message output
## 0.6.0
* Added bqt magic extension with query support and data preview
## 0.5.4
* Added ability to pass a custom scope to the BigQuery client, allowing querying of tables with data on Google Drive/Sheets
## 0.5.3
* Show job id in initial "Job started..." logging output.
## 0.5.2
* Added new parameter `verbose_cache` to turn off printed statements of cache age for `bqt.query()`, `bqt.query_async`, `bqt.fast_query`
## 0.5.1
* Bug fix: operation (e.g. querying) fails if there's a bad job in the queue. Fix is to catch the exception and remove that job
## 0.5
* Fixes bug in pickling/unpickling
## 0.4.8
* Experimental fast_query() support of nested and repeated results.
## 0.4.7
* Fixes bug with `bqt.change_location()`
## 0.4.6
* Adds location as an option to `BqClient` and surfaces it in the `BqT` class
## 0.4.5
* Make some delay and retries in fast_query when extracted data from BQ is not available in GCS
## 0.4.4
* Suppress 404 NotFound when deleting blob in fast_query & fast_load
## 0.4.3
* Fixed time range bug with ranges outside of days
## 0.4.2
* Pinning packages to avoid dependency issues
## 0.4.1
* Add method to update view
## 0.4
* Added fix to order of parameter replacement to allow for `{table}{LATEST}` to work.
## 0.3.25
* fixing wheel based pip installs
## 0.3.24
* testing new deploy with Tingle and PythonLibrary template
## 0.3.23
* bumps `google-cloud-bigquery` to latest to fix some dependency issues
## 0.3.22
* Fixes `auto_annotate=True` bug with `update_table_metadata()` and alter_table() copy bug
## 0.3.21
* passing args of `fast_query` down to `pandas.read_csv()` to deal with the likes of NaNs
## 0.3.20
* Fixes issue where nothing was using cache after generators were introduced
## 0.3.19
* removed python-levenshtein because it was causing all linux builds to fail
## 0.3.18
* Add `bqt.bq_insight` to get an insight about BigQuery tables on a project
## 0.3.17
* Fix dependency hell courtesy of our friends at Google
## 0.3.16
* add option to return large dataframe as a generator of smaller dataframes in `query`
## 0.3.15
* Fixes bug where `cache=None` causes `fast_query()` to fail
## 0.3.14
* Fixes bug in sampler happening when a single table is used for sampling
## 0.3.13
* Fix bug so table description is only added after table is created in `create_table()`
## 0.3.12
* add parameter insertion support for table name in `fast_load()`
## 0.3.11
* Can get `schema` and `description` table metadata from a BqSingleTable object
* Can `create_table()` with explictly defined table schema
* Update/add table description and field descriptions with `update_table_metadata()`
* Update a single field's description with `update_field_description()`
* Automatically add semantic type field descriptions to commonly named fields by passing `auto_annotate=True` to `update_table_metadata()`
## 0.3.10
* Fixes bug in `bqt.delete_all_partitions` and any other method using `BqTable` that acted on all partitions. more info: https://ghe.spotify.net/science-box/bqt/issues/119
## 0.3.9
* add `write_disposition` to `fast_load`
## 0.3.8
* adds support for reading queries from files
## 0.3.7
* updated arrow to use `.shift()` instead of `.replace()` because the latter has deprecated plural replacements
## 0.3.6
* Change millnames 'B' -> 'G' in human_number (useful for size)  
## 0.3.5
* Handling exception on caching big results for fast_query and enforce google-cloud-core version
## 0.3.4
* Added handy function `bqt.parse_string` to make bqt parameter management useful on its own
## 0.3.3
* Add bqt.writer.fast_load to atomic load a pandas DataFrame to BigQuery at least 5X faster than Google BigQuery API and pandas gbq
## 0.3.2
* Return the query in the `dry_run` mode
* Fixed tests in `reader_test` and `writer_test`
## 0.3.1
* making `psutil` an optional functionality that only works if the package is installed since it was causing issues in non SB environments
## 0.3
* added `set_params()` to set multiple parameters at the same time
* throws exception if trying to set a parameter to None as the behavior is undefined
* adds checks on table size and aborts if results won't fit into memory
## 0.2.25
* Change fast_query's wait_for_job handling to BqJobManager
## 0.2.24
* Bump googleapis-common-protos due to old deps on sb
## 0.2.23
* Added printing of the query in the `dry_run` mode
## 0.2.22
* Bump google api-core, cloud-bigquery & cloud-storage due to old deps on sb
## 0.2.21
* Add bqt.reader.fast_query to read big tables at least 5X faster than bqt.reader.query
## 0.2.20
* removed job state function from the job manager because they weren't useful and were causing issues
## 0.2.19
* adding two more date formatting options to parameter manager ('YYMMDD' and 'MMDD')
## 0.2.18
* Fix bug where `{date[1D]}` failed because it didn't have a sign
## 0.2.17
* Fixes Python3 bug with regex
## 0.2.16
* Fixes bug with `sample_query_stratified` calling the wrong method
## 0.2.15
* Fixed bug in 0.2.14 where class method name was outdated
## 0.2.14
* Added offset option to `{LATEST}`, i.e. `table_name_{LATEST[-3D]}`
## 0.2.13
* Fixed bug where date offsets on Python3 caused an encoding error
## 0.2.12
* added `skip_formatting` to calls that did SQL formatting because sometime that fails when the SQL contains UDFs
## 0.2.11
* Bug fix where `{LATEST}` would return the oldest table
## 0.2.10
* adds support for inserting list of values in queries directly.
## 0.2.9
* Adds `get_all_tables()` to the query analyzer
## 0.2.8
* Fixed bug in `bqt.change_project()` that was introduced in 0.2.7
## 0.2.7
* Moved the loading up of the gcloud lib from import to first use so it doesn't break on import if creds are missing
## 0.2.6
* Fixes issue with `{LATEST}` picking up tables that are not partitions
* Fixes bug with pickle version mismatch when bqt is used with two versions of python
## 0.2.5
* Fixes bug in parameter manager where all parameters had to be replace or an Exception happened
## 0.2.4
* fixes bug with parameter manager returning inconsistent values
* Adds hour to partition detection
## 0.2.3
* Adds date offset support to parameter manager so now you can do `date[-10D+2M]_YYYYMMDD`
## 0.2.2
* Small bug fixes
## 0.2.1
* Adds dry run to `query()` and `query_async()`
* Adds the query parser and analyzer in early alpha mode
## 0.2
* Adds Sampler which supports sampling tables and queries
* Failed queries because of sytax error now show the position of the error
* verbose switch was moved to PrintTools to make it global
* Added message for when all jobs are finished so the user can know
* Interrupting execution through `KeyboardInterrupt` now prompts to cancel the job on BigQuery if one is running and we're waiting on it
* Fixed issue where the query has a `#StandardSQL` annotation
## 0.1.12
* bug fix version, bqt didn't load because of dynamic `__version__` attribute
## 0.1.11
* Adds table and column analyzer (for now basic functionality)
* Adds a message when creating a table is done
* Adds `renew_cache` function to run the same query but also update the cache
## 0.1.10
* Get latest partitition function now returns only exact matches for table prefix
## 0.1.9
* Fixes bugs on object deletion in instance manager
* Query: Imports IPython dependencies only if we are inside notebook
## 0.1.8
* Trying to make `BqT` compatible, package-wise, with other SB packages, part 1 of ...
## 0.1.7
* Fixes python3 bug with using `basestring`
## 0.1.6
* Changes the parameter range functionality to use generators and for loops
## 0.1.5
* Adds `undelete_table` and `undelete_table_async` to [duh!] undelete tables
* Fixes bug where `long` is used but breaks in Python 3
* Adds better printing for fetching results
## 0.1.4
* Fixes bug when using Python3 and table iterator
## 0.1.3
* Fixes bug when setting write_disposition to any value but `WRITE_EMPTY` in bqt.create_table function
## 0.1.2
* Fixes bug when data types differ between reading from BQ and reading from cached results locally
* Adds CLI tool `bqt` that can delete and alter tables
* Fixes minor bugs with deleting tables
* Adds `ShortCache` class to cache small optimizations in memory, currently used to store latest partitions for an hour when `{LATEST}` is used.
* Adds a `project` parameter to create table API to control which project the table is in
* fixes a bug where a parameter can't be set as an Arrow date object directly
## 0.1.1
* Fixes error with `BqJobManager.wait_for_job()` when the job failed
## 0.1
A lot of changes:
1. better and more consistent API
2. Many small and big bug fixes
3. Majority of the library now has tests
4. Job History and state persistence that will let you retrieve jobs from BigQuery directly and retrieve running jobs when the kernel fails
5. Parameter management to keep track of analysis parameters
6. Copy table support
7. More messages and info across the board, plus query formatter!
8. Better error handling and visibility into BigQuery specific errors
9. Better table partition support
10. More example notebooks
## 0.0
### 0.0.5
Adds requirements.txt file to MANIFEST.in for proper installation
### 0.0.4
fixes python package by adding MANIFEST.in
### 0.0.3
Minor fixes and job management tool, basic functionality working. first PyPi release!
### 0.0.2
Added basic package structure and basic read/create/delete/alter functionality
### 0.0.1
Initial repository structure, no actual code

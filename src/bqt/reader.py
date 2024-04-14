import hashlib
import json
import os
import re
import time

import arrow
import fastavro
import pandas
import polars as pl
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from bqt.lib.connection import BqConnection
from bqt.lib.decorators import expose_as_api
from bqt.lib.job import BqJobManager, BqJobResult, BqJobDoesntExist
from bqt.lib.print_tools import PrintTools
from bqt.lib.query import Query
from bqt.lib.table import BqSingleTable
from bqt.lib.utils import resolve_query_to_string
from bqt.workbench import get_workbench_job_id_prefix, is_workbench


class Reader(object):
    """Class to do read operations on BQ data"""

    def __init__(self, bqt_obj):
        self.bqt_obj = bqt_obj

        # setup cache, NullCache is used to handle /dev/null type caching
        self.caches = {None: NullCache, "local": LocalCache, "bq": BqCache}

        # keep track of fast_query's ability to support advanced_dtype_handling
        self.snappy_installed = False

        self.result_format = None

    @expose_as_api
    def query_async(
        self,
        query,
        dialect="standard",
        cache="local",
        renew_cache=False,
        dry_run=False,
        skip_formatting=False,
        verbose_cache=True,
        jinja=True,
        lsp_dry_run=False,
        result_format="pandas",
        job_id_prefix=None,
    ):
        """Run a query and return the results (async)

        Note: This method will run a query and return immediately, i.e. it
            doesn't wait for the query to finish.

        Args:
            query (string): query to use
            dialect (string): one of 'standard' or 'legacy'
            cache (string): one of:
                'local': stores results as CSV locally
                'bq': stores results as tables in BQ
                None: use BigQuery's native caching
            renew_cache (bool): If True, will ignore an existing cache to renew
                the content
            dry_run (bool): If True, will only calculate the cost of the query
            verbose_cache (bool): If False, will not print out information on cache age
            jinja (bool): If False, opt-out of jinja rendering step
            lsp_dry_run (bool): If True, same as dry_run but also preserves the original query formatting
                and prevents writing to stdout
            job_id_prefix (Optional[str]):
                The prefix to use for a randomly generated job ID
        Returns
            BqJobResult object that has more information about the job
        """
        query_str = resolve_query_to_string(query)
        cache_obj, query_job = self._get_cache_and_job(
            query_str,
            dialect,
            cache,
            renew_cache,
            dry_run,
            skip_formatting,
            verbose_cache,
            jinja,
            lsp_dry_run,
            result_format=result_format,
            job_id_prefix=job_id_prefix,
        )
        if not renew_cache and cache_obj.hit():
            return cache_obj.fetch()

        if not dry_run and not lsp_dry_run:
            query_job.register_on_results_callback(cache_obj.store_results)
        elif self.bqt_obj.verbose:
            bytes_processed = query_job.job.total_bytes_processed or 0

            # if running from LSP, do not print output
            if not lsp_dry_run:
                PrintTools(
                    "DRY RUN: Would Process: {} Would Cost: ${:,.2f}".format(
                        PrintTools.human_number(bytes_processed), self.bqt_obj.get_config("bq_price") * bytes_processed
                    ),
                    _type=PrintTools.WARNING,
                )

        return BqJobManager.queue_job(query_job)

    def _get_cache_and_job(
        self,
        query,
        dialect="standard",
        cache="local",
        renew_cache=False,
        dry_run=False,
        skip_formatting=False,
        verbose_cache=True,
        jinja=True,
        lsp_dry_run=False,
        result_format="pandas",
        job_id_prefix=None,
    ):
        if not skip_formatting:
            query = self.bqt_obj.parameter_manager.insert_params(query, jinja)
        query, standard = Query.remove_and_report_standard(query)
        if standard:
            dialect = "standard"

        # Use the formatted query as the cache key
        if not skip_formatting:
            formatted_query = Query(query).format
        else:
            formatted_query = query
        cache_obj = self.caches[cache](self.bqt_obj, formatted_query, result_format)

        if not renew_cache and cache_obj.hit():
            # if running from LSP, do not print output
            if verbose_cache and not lsp_dry_run:
                self.bqt_obj.print_msg(
                    "Found cached data that is {age} old, using it.".format(
                        age=PrintTools.human_duration(cache_obj.age)
                    )
                )
            return cache_obj, None

        job_config = bigquery.QueryJobConfig()
        job_config.use_legacy_sql = dialect == "legacy"
        job_config.dry_run = dry_run or lsp_dry_run

        if not dry_run and not lsp_dry_run and cache_obj.cache_requires_job_config:
            cache_obj.config_job(job_config)
        elif dry_run and not lsp_dry_run:
            PrintTools(Query(formatted_query).colorize())

        # if we are running in Workbench and job_id_prefix is not set, use context "python" as default
        if not job_id_prefix and is_workbench():
            job_id_prefix = get_workbench_job_id_prefix("python")

        # If we are dry running from a LSP, execute the raw un-formatted query so line/char numbers
        # match what the caller sees.
        # *we still want to format the query above so we can take advantage of cached results
        query_job = BqJobResult(
            self.bqt_obj.bq_client.query(
                query if lsp_dry_run else formatted_query, job_config=job_config, job_id_prefix=job_id_prefix
            ),
            self.bqt_obj,
            result_format=result_format,
        )
        return cache_obj, query_job

    @expose_as_api
    def query(
        self,
        query,
        dialect="standard",
        cache="local",
        renew_cache=False,
        dry_run=False,
        skip_formatting=False,
        return_generator=False,
        verbose_cache=True,
        jinja=True,
        result_format="pandas",
        job_id_prefix=None,
    ):
        """Run a query and return the results

        Note: This method will run a query and wait (block) for the results

        Args:
            query (string or filepath): query to use
            dialect (string): one of 'standard' or 'legacy'
            cache (string): one of:
                'local': stores results as CSV locally
                'bq': stores results as tables in BQ
                None: don't use cache
            renew_cache (bool): If True, will ignore an existing cache to renew
                the content
            dry_run (bool): If True, will only calculate the cost of the query
                and return the query
            return_generator (bool): If True, will return a list of dfs as a
                generator
            verbose_cache (bool): If False, will not print out information on cache age
            jinja (bool): If False, opt-out of jinja rendering step
            result_format (str): Format for the returned result. Options are:
                'pandas': pandas dataframe
                'polars': polars dataframe
            job_id_prefix (Optional[str]):
                The prefix to use for a randomly generated job ID
        Returns
            if dry_run=True, return the query
            if return_generator=True, return a generator of pandas.DataFrame
            otherwise returns pandas.DataFrame
        """
        # return generator does not support cache
        if return_generator:
            cache = None
        # generator does not currently support polars
        if return_generator and result_format == "polars":
            raise NotImplementedError("Generator currently supports only pandas.")
        job: BqJobResult = self.query_async(
            query,
            dialect=dialect,
            cache=cache,
            renew_cache=renew_cache,
            skip_formatting=skip_formatting,
            dry_run=dry_run,
            verbose_cache=verbose_cache,
            jinja=jinja,
            result_format=result_format,
            job_id_prefix=job_id_prefix,
        )
        if dry_run:
            to_return = resolve_query_to_string(query)
        elif return_generator:
            to_return = job.results_generator(generator_row_size=self.bqt_obj.get_config("generator_row_size"))
        else:
            to_return = job.results
        if job.has_drive_error():
            if (
                not self.bqt_obj.custom_scope
                or "https://www.googleapis.com/auth/drive" not in self.bqt_obj.custom_scope
            ):
                PrintTools(
                    "Unable to query, found table(s) with data stored in Google Drive. "
                    "Added experimental Google Drive permissions and trying again...",
                    _type=PrintTools.WARNING,
                )
                if not self.bqt_obj.custom_scope:
                    self.bqt_obj.change_custom_scope(["https://www.googleapis.com/auth/drive"])
                else:
                    self.bqt_obj.change_custom_scope(
                        self.bqt_obj.custom_scope + ["https://www.googleapis.com/auth/drive"]
                    )
                return self.query(
                    query, dialect, cache, renew_cache, dry_run, skip_formatting, return_generator, verbose_cache, jinja
                )
            else:
                PrintTools(
                    "Unable to query data stored in Google Drive despite expanded credentials scope. "
                    "Make sure the account you are querying with has access to the underlying file or Sheet.",
                    _type=PrintTools.WARNING,
                )
        return to_return

    @expose_as_api
    def clear_cache(self, older_than=0):
        """Clear all cached BigQuery results

        Args:
            older_than (int): files older than this many seconds are removed
        """
        lc = LocalCache(self.bqt_obj, "")
        lc.MAX_CACHE_TTL = older_than
        lc.cleanup_old_caches_cause_gdpr()

    @expose_as_api
    def fast_query(
        self,
        query,
        staging_bucket=None,
        dialect="standard",
        cache="local",
        renew_cache=False,
        dry_run=False,
        skip_formatting=False,
        advanced_dtype_handling=False,
        verbose_cache=True,
        jinja=True,
        result_format="pandas",
        return_generator=False,
        job_id_prefix=None,
        **csv_args,
    ):
        """Run a query and return the results much faster (on big results) with
            the help of GCS and compression

        Note: This method will run a query and wait (block) for the results

        Args:
            query (string): query to use
            staging_bucket (string, optional):
                name of bucket to store the intermediate data
                if not set a default bucket will be created
            dialect (string): one of 'standard' or 'legacy'
            cache (string): one of:
                'local': stores results as CSV locally
                'bq': stores results as tables in BQ
                None: don't use cache
            renew_cache (bool): If True, will ignore an existing cache to renew
                the content
            dry_run (bool): If True, will only calculate the cost of the query
            skip_formatting (bool): If False, will format the query string
            advanced_dtype_handling (bool): If True, will preserve datetime
                dtypes and enable nested or repeated results. Experimental flag,
                requires python-snappy (included in in science-box>=1.1)
            verbose_cache (bool): If False, will not print out information on cache age
            jinja (bool): If False, opt-out of jinja rendering step
            return_generator (bool): If True, will yield chunks as individual dataframes
            **csv_args (mixed): any extra keyword arguments are directly
                passed into pandas.read_csv()
            job_id_prefix (Optional[str]):
                The prefix to use for a randomly generated job ID

        Returns:
            pandas.DataFrame if dry_run=False otherwise nothing is returned

        Example:
            >>> from bqt import bqt
            >>> df = bqt.fast_query("SELECT * FROM my_dataset.my_table")
        """
        start = arrow.now()
        self.result_format = result_format
        query = resolve_query_to_string(query)
        cache_obj, query_job = self._get_cache_and_job(
            query,
            dialect,
            cache,
            renew_cache,
            dry_run,
            skip_formatting,
            verbose_cache,
            jinja,
            result_format=self.result_format,
            job_id_prefix=job_id_prefix,
        )

        results = None
        if not renew_cache and cache_obj.hit():
            results = cache_obj.fetch().results

        elif not dry_run:
            if not advanced_dtype_handling:
                PrintTools("Attempting dry-run before executing full query...", _type=PrintTools.WARNING)
                preview_job = self.query_async(query, dry_run=True)
                schema = preview_job.job._properties["statistics"]["query"].get("schema")
                if schema:
                    for schema_field in schema.get("fields"):
                        if schema_field.get("type") in ("STRUCT", "RECORD") or schema_field.get("mode") == "REPEATED":
                            PrintTools(
                                "Nested or repeated results, enabling experimental advanced_dtype_handling flag.",
                                _type=PrintTools.WARNING,
                            )
                            advanced_dtype_handling = True
                            break
            if advanced_dtype_handling and not self.snappy_installed:
                try:
                    import snappy

                    self.snappy_installed = True
                except ImportError:
                    raise SnappyMissing("Install python-snappy or use Science Box 1.1+ to use advanced_dtype_handling.")
            try:
                bucket, blob_name = self._bq_to_gcs(
                    query_job, cache_obj.cache_key, staging_bucket, advanced_dtype_handling=advanced_dtype_handling
                )
                if return_generator:
                    results = self._gcs_to_df_generator(
                        bucket,
                        blob_name,
                        self.bqt_obj.get_config("fastbqt.temp_dir"),
                        csv_args,
                        advanced_dtype_handling,
                    )
                else:
                    results = self._gcs_to_results(
                        bucket,
                        blob_name,
                        self.bqt_obj.get_config("fastbqt.temp_dir"),
                        csv_args,
                        advanced_dtype_handling,
                    )
            except RuntimeError as e:
                if query_job.has_drive_error():
                    if (
                        not self.bqt_obj.custom_scope
                        or "https://www.googleapis.com/auth/drive" not in self.bqt_obj.custom_scope
                    ):
                        PrintTools(
                            "Unable to query, found table(s) with data stored in Google Drive. "
                            "Added experimental Google Drive permissions and trying again...",
                            _type=PrintTools.WARNING,
                        )
                        if not self.bqt_obj.custom_scope:
                            self.bqt_obj.change_custom_scope(["https://www.googleapis.com/auth/drive"])
                        else:
                            self.bqt_obj.change_custom_scope(
                                self.bqt_obj.custom_scope + ["https://www.googleapis.com/auth/drive"]
                            )
                        return self.fast_query(
                            query,
                            staging_bucket,
                            dialect,
                            cache,
                            renew_cache,
                            dry_run,
                            skip_formatting,
                            advanced_dtype_handling,
                            verbose_cache,
                            **csv_args,
                        )
                    else:
                        PrintTools(
                            "Unable to query data stored in Google Drive despite expanded credentials scope. "
                            "Make sure the account you are querying with has access to the underlying file or Sheet.",
                            _type=PrintTools.WARNING,
                        )
                raise e
            try:
                cache_obj.store_results(results, query_job)
            except Exception as e:
                PrintTools(
                    "Results can't be cached on this machine due to " f"the following error \n{type(e).__name__}: {e}",
                    _type=PrintTools.WARNING,
                )
        elif self.bqt_obj.verbose:
            bytes_processed = query_job.job.total_bytes_processed or 0
            PrintTools(
                "DRY RUN: Would Process: {} Would Cost: ${:,.2f}".format(
                    PrintTools.human_number(bytes_processed), self.bqt_obj.get_config("bq_price") * bytes_processed
                ),
                _type=PrintTools.WARNING,
            )
        end = arrow.now()
        PrintTools("Time elapsed: {}".format(end - start), _type=PrintTools.SUCCESS)
        return results

    def _bq_to_gcs(self, query_job, cache_key, staging_bucket=None, advanced_dtype_handling=False):
        bucket = self.bqt_obj.create_staging_bucket(staging_bucket, query_job.job.location)
        table_ref = query_job.job.destination
        blob_name = self.bqt_obj.get_config("fastbqt.fastquery_blob_name").format(
            cache_key, arrow.now().format("YYYYMMDDHHmmss")
        )
        job_config = bigquery.ExtractJobConfig()
        if not advanced_dtype_handling:
            destination_uri = "gs://{}/{}/*.csv.gzip".format(bucket.name, blob_name)
            job_config.compression = "GZIP"
            job_config.destination_format = "CSV"
        else:
            destination_uri = "gs://{}/{}/*.avro".format(bucket.name, blob_name)
            job_config.compression = "SNAPPY"
            job_config.destination_format = "AVRO"
        PrintTools("Job %s:%s submitted ..." % (query_job.job.project, query_job.job.job_id), _type=PrintTools.INFO)
        BqJobManager.wait_for_job(query_job)

        table = self.bqt_obj.bq_client.get_table(table_ref)
        PrintTools(
            "Got {:,d} rows ({})".format(table.num_rows, PrintTools.human_number(table.num_bytes)),
            _type=PrintTools.INFO,
        )

        PrintTools("Compressing and preparing files ...", _type=PrintTools.INFO)
        extract_job = self.bqt_obj.bq_client.extract_table(table_ref, destination_uri, job_config=job_config)
        extract_job.result()
        return bucket, blob_name

    def _gcs_to_results(self, bucket, blob_name, tmp_dir, csv_args, advanced_dtype_handling):
        retries = self.bqt_obj.get_config("fastbqt.fastquery_blob_retries")
        list_blobs_started = arrow.now()
        for i in range(retries):
            blobs = [blob for blob in bucket.list_blobs() if blob.name.startswith("{}/".format(blob_name))]
            if len(blobs) > 0:
                break

            time.sleep(i * 5)
            if i == retries - 1:
                elapsed = (arrow.now() - list_blobs_started).total_seconds()
                raise NotFound(
                    f"After {retries} retries and {elapsed} seconds BQ extract_table hasn't loaded the data to GCS"
                )
        size = sum(blob.size for blob in blobs)
        PrintTools(
            "{} chunks ({}) will be downloaded!".format(len(blobs), PrintTools.human_number(size)),
            _type=PrintTools.INFO,
        )
        cache_dir = "{}{}{}".format(tmp_dir, os.sep, blob_name)
        os.mkdir(cache_dir)
        list_df = []
        for i, blob in enumerate(blobs):
            PrintTools(
                "Downloading chunk {}/{} ({}) ...".format(i + 1, len(blobs), PrintTools.human_number(blob.size)),
                _type=PrintTools.INFO,
            )
            filename = "{}{}{}".format(tmp_dir, os.sep, blob.name)
            blob.download_to_filename(filename)
            df = self._load_results_from_file(filename, advanced_dtype_handling, csv_args)
            list_df.append(df)
            os.remove(filename)
            try:
                blob.delete()
            except NotFound:
                pass
        os.rmdir(cache_dir)

        if self.result_format == "polars":
            return pl.concat(list_df, how="vertical")
        return pandas.concat(list_df)

    def _load_results_from_file(self, filename, advanced_dtype_handling, csv_args):
        if advanced_dtype_handling:
            with open(filename, "rb") as fp:
                records = [r for r in fastavro.reader(fp)]
            if self.result_format == "polars":
                return pl.DataFrame(records)
            else:
                return pandas.DataFrame.from_records(records)
        else:
            if self.result_format == "polars":
                return pl.read_csv(filename, **csv_args)
            else:
                return pandas.read_csv(filename, compression="gzip", **csv_args)

    def _gcs_to_df_generator(self, bucket, blob_name, tmp_dir, csv_args, advanced_dtype_handling):
        retries = self.bqt_obj.get_config("fastbqt.fastquery_blob_retries")
        list_blobs_started = arrow.now()
        for i in range(retries):
            blobs = [blob for blob in bucket.list_blobs() if blob.name.startswith("{}/".format(blob_name))]
            if len(blobs) > 0:
                break

            time.sleep(i * 5)
            if i == retries - 1:
                elapsed = (arrow.now() - list_blobs_started).total_seconds()
                raise NotFound(
                    f"After {retries} retries and {elapsed} seconds BQ extract_table hasn't loaded the data to GCS"
                )
        size = sum(blob.size for blob in blobs)
        PrintTools(
            "{} chunks ({}) will be downloaded!".format(len(blobs), PrintTools.human_number(size)),
            _type=PrintTools.INFO,
        )
        cache_dir = "{}{}{}".format(tmp_dir, os.sep, blob_name)
        os.mkdir(cache_dir)
        for i, blob in enumerate(blobs):
            PrintTools(
                "Downloading chunk {}/{} ({}) ...".format(i + 1, len(blobs), PrintTools.human_number(blob.size)),
                _type=PrintTools.INFO,
            )
            filename = "{}{}{}".format(tmp_dir, os.sep, blob.name)
            blob.download_to_filename(filename)
            if advanced_dtype_handling:
                with open(filename, "rb") as fp:
                    records = [r for r in fastavro.reader(fp)]
                df = pandas.DataFrame.from_records(records)
            else:
                df = pandas.read_csv(filename, compression="gzip", **csv_args)
            yield df
            os.remove(filename)
            try:
                blob.delete()
            except NotFound:
                pass


class SnappyMissing(RuntimeError):
    pass


class CacheNotSetup(RuntimeError):
    pass


class NullCache(object):
    """/dev/null type of cache, doesn't do anything"""

    def __init__(self, bqt_obj, query, result_format=None):
        self.cache_key = self._cache_key(query)
        self.age = 0
        self.bqt_obj = bqt_obj
        self.result_format = result_format

    def hit(self):
        return False

    def fetch(self):
        return None

    def store_results(self, results, bq_job_results):
        return True

    @property
    def cache_requires_job_config(self):
        return False

    def config_job(self, job_config):
        raise NotImplementedError

    def _cache_key(self, query):
        return hashlib.md5(query.strip().encode("utf-8")).hexdigest()


class LocalCache(NullCache):
    # Max time in seconds a cache can exist, mainly because of GDPR
    MAX_CACHE_TTL = 86400 * 30

    cache_file_regex = re.compile(r"bqt_[a-z0-9]{32}\.(pkl|parquet)")
    cache_info_file_regex = re.compile(r"bqt_[a-z0-9]{32}\.dat")

    """Helper class to handle local file caching of results"""

    def __init__(self, bqt_obj, query, result_format=None):
        super(LocalCache, self).__init__(bqt_obj, query, result_format=result_format)
        self.ttl = bqt_obj.get_config("cache.ttl")
        self.tmp_dir = bqt_obj.get_config("cache.local.temp_dir")
        self.cache_file_ = "%s/bqt_%s" % (self.tmp_dir, self.cache_key)
        self.cache_file = self.cache_file_ + ".parquet" if self.result_format == "polars" else self.cache_file_ + ".pkl"
        self.cache_info_file = "%s/bqt_%s.dat" % (self.tmp_dir, self.cache_key)

    def hit(self):
        if os.path.exists(self.cache_file) and os.path.exists(self.cache_info_file):
            self.age = self._file_age(self.cache_file)
            if self.ttl == -1 or self.age < self.ttl:
                return True
        return False

    def fetch(self):
        try:
            if self.result_format == "polars":
                data = pl.read_parquet(self.cache_file)
            else:
                data = pandas.read_pickle(self.cache_file)
            with open(self.cache_info_file) as f:
                info = json.load(f)
            return BqJobResult.from_storage(info, data, self.bqt_obj, result_format=self.result_format)
        except (BqJobDoesntExist, ValueError):
            # ValueError happens when pickle versions is from newer python
            # not much we can do here, clear stale local cache and re-raise
            os.remove(self.cache_file)
            os.remove(self.cache_info_file)
            raise

    def store_results(self, results, bq_job_results):
        if self.result_format == "polars":
            results.write_parquet(self.cache_file)
        else:
            results.to_pickle(self.cache_file)
        with open(self.cache_info_file, "w") as f:
            json.dump(bq_job_results.serialize(), f)

    def cleanup_old_caches_cause_gdpr(self):
        """Clean up old cache files from the OS"""
        for file in os.listdir(self.tmp_dir):
            if self.cache_file_regex.match(file) or self.cache_info_file_regex.match(file):
                full_path = os.path.join(self.tmp_dir, file)
                if self._file_age(full_path) > self.MAX_CACHE_TTL:
                    os.remove(full_path)

    def _file_age(self, file):
        """Return age of file in seconds"""
        return time.time() - os.path.getmtime(file)


class BqCache(NullCache):
    """Helper class to handle caching results as tables in BQ"""

    def __init__(self, bqt_obj, query, result_format=None):
        super(BqCache, self).__init__(bqt_obj, query, result_format=result_format)

        self.ttl = bqt_obj.get_config("cache.ttl")
        self.project = bqt_obj.get_config("cache.bq.project") or bqt_obj.project
        self.dataset = bqt_obj.get_config("cache.bq.dataset")
        self.table_prefix = bqt_obj.get_config("cache.bq.table_prefix")
        self.cache_table = self.table_prefix + self._cache_key(query)

    def check_config(self):
        if self.dataset is None:
            raise CacheNotSetup("BQ cache is not setup properly")

    def hit(self):
        self.check_config()

        table = BqSingleTable(self.project, self.dataset, self.cache_table)
        if table.exists:
            self.age = table.age
            if self.ttl == -1 or table.age < self.ttl:
                return True
        return False

    def fetch(self):
        self.check_config()

        query = "SELECT * FROM `%s.%s.%s`" % (self.project, self.dataset, self.cache_table)
        return self.bqt_obj.query_async(query, cache=None, result_format=self.result_format)

    @property
    def cache_requires_job_config(self):
        return True

    def config_job(self, job_config):
        self.check_config()

        job_config.write_disposition = "WRITE_TRUNCATE"
        job_config.destination = BqConnection.get_dataset(self.dataset, project=self.project).table(self.cache_table)
        return job_config

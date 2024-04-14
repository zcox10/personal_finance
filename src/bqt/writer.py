import arrow
import os
import uuid
import math

from google.cloud import bigquery
from google.cloud.exceptions import Conflict, BadRequest, NotFound
from threading import Thread
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import json

from bqt.lib.decorators import expose_as_api
from bqt.lib.job import BqJobManager, BqJobResult, BqJobWithoutResults
from bqt.lib.table import BqSingleTable, BqTable
from bqt.lib.print_tools import PrintTools
from bqt.lib.query import Query
from bqt.lib.utils import resolve_query_to_string, FIELD_DESCRIPTION_MAP


class Writer(object):
    """Class to create data in bigquery"""

    def __init__(self, bqt_obj):
        self.bqt_obj = bqt_obj

    @expose_as_api
    def create_table_async(self, query, dataset, table, project=None,
                           dialect='standard',
                           write_disposition='WRITE_EMPTY',
                           ignore_exists=False, skip_formatting=False,
                           schema=None, confirm=True, jinja=True):
        """Creates a table from a query as cache

        Note: this is an async call and will return immediately, you have
              to wait for the job to finish using the job object returned

        Args:
            query (string): query to run
            dataset (string): dataset of destination table
            table (string): destination table name
            dialect (string): 'legacy' or 'standard', use standard ;)
            write_disposition (string): one of the supported BigQuery
                write disposition types
            ignore_exists (bool): ignore the table if it exists
            schema (list of dictionaries): in format from BqSingleTable().schema
                Example:
                [{'description': u'Genre GID in hexadecimal string',
                  'mode': u'REQUIRED',
                  'name': u'genre_gid',
                  'type': u'STRING'},
                 {'description': u'Date of entity creation',
                  'mode': u'REQUIRED',
                  'name': u'date',
                  'type': u'STRING'}]
            confirm (bool): if True, the call will explicitly confirm before
                deleting anything; only applies when passing in a desired schema
            jinja (bool): If False, opt-out of jinja rendering step

        Returns:
            The query job that creates the table
        """
        query = resolve_query_to_string(query)
        if not skip_formatting:
            query = self.bqt_obj.parameter_manager.insert_params(query, jinja)
        query, standard = Query.remove_and_report_standard(query)
        if standard:
            dialect = 'standard'
        table = self.bqt_obj.parameter_manager.insert_params(table, jinja)

        table = BqSingleTable(
            project or self.bqt_obj.bq_client.project, dataset, table
        )
        if table.exists and ignore_exists:
            self.bqt_obj.print_msg(
                "Skipping, `%s` already exists" % repr(table),
                _type="warn"
            )
            return

        job_config = bigquery.QueryJobConfig()
        job_config.destination = table.table_ref
        job_config.use_legacy_sql = dialect == 'legacy'

        # By default, a QueryJobConfig will write all fields except for RECORDs
        # as NULLABLE. If we want to explictly override this and make
        # certain fields REQUIRED, we need to create an empty table
        # then perform a QueryJobConfig with WRITE_APPEND to this table with the
        # schema explicty defined with REQUIRED fields
        if schema:
            if table.exists:
                if confirm and not PrintTools.confirm_action(
                    "Table already exists; it must be deleted first before "
                        "explicitly creating it with a desired schema. Ok? "):
                    return False
                self.bqt_obj.bq_client.delete_table(table.table_ref)
            target_table = bigquery.Table(table.table_ref,
                                          schema=self._schema_from_dict(schema))
            self.bqt_obj.bq_client.create_table(target_table)
            job_config.write_disposition = 'WRITE_APPEND'
        else:
            job_config.write_disposition = write_disposition

        self.bqt_obj.print_msg("Creating table `%s` ..." % repr(table))
        # we wrap everything in a BqJobResult class or some subclass depending
        # on what's needed
        query_job = BqJobResult(
            self.bqt_obj.bq_client.query(query, job_config=job_config),
            self.bqt_obj
        )

        # it's important to keep track of all running jobs
        # it will block if the we've reached max concurrency
        # it also creates a BqJobResult object that has nice methods :D
        return BqJobManager.queue_job(query_job)

    @expose_as_api
    def create_table(self, query, dataset, table, project=None,
                     dialect='standard',
                     write_disposition='WRITE_EMPTY',
                     ignore_exists=False, skip_formatting=False,
                     schema=None, description=None, confirm=True, jinja=True):
        """Create a table and blocks until it's finished

        Args:
            query (string): query to run
            dataset (string): dataset of destimatopm table
            table (string): destination table name
            dialect (string): 'legacy' or 'standard', use standard ;)
            write_disposition (string): one of the supported BigQuery
                write disposition types
            ignore_exists (bool): ignore the table if it exists
            schema (list of dictionaries): in format from BqSingleTable().schema
                Example:
                [{'description': u'Genre GID in hexadecimal string',
                  'mode': u'REQUIRED',
                  'name': u'genre_gid',
                  'type': u'STRING'},
                 {'description': u'Date of entity creation',
                  'mode': u'REQUIRED',
                  'name': u'date',
                  'type': u'STRING'}]
            description (string): description for table metadata
            confirm (bool): if True, the call will explicitly confirm before
                deleting anything; only applies when passing in a desired schema
            jinja (bool): If False, opt-out of jinja rendering step

        Returns:
            The query job that creates the table
        """
        job = self.create_table_async(
            query, dataset, table, project=project,
            dialect=dialect, write_disposition=write_disposition,
            ignore_exists=ignore_exists, skip_formatting=skip_formatting,
            schema=schema, confirm=confirm, jinja=jinja
        )
        if job:
            BqJobManager.wait_for_job(job)
            if description:
                self.update_table_metadata(dataset,
                                           table,
                                           project,
                                           description=description)
        return job

    @expose_as_api
    def update_table_metadata(self, dataset, table, project=None, schema=None,
                              description=None, description_access_tier=None,
                              auto_annotate=False, exact_match=False):
        """Update metadata for the table

        Args:
            dataset (string): dataset of destimation table
            table (string): destination table name
            project (string): project of the table
            schema (list of dictionaries): in format from BqSingleTable().schema
                Will update field descriptions; to change datatypes or mode,
                you will need to create a new table and pass in the desired
                mode and datatypes as a schema to create_table()
                Example:
                [{'description': u'Genre GID in hexadecimal string',
                  'mode': u'REQUIRED',
                  'name': u'genre_gid',
                  'type': u'STRING'},
                 {'description': u'Date of entity creation',
                  'mode': u'REQUIRED',
                  'name': u'date',
                  'type': u'STRING'}]
            description (string): description for table metadata
            description_access_tier (string): use in place of description if
                one only needs to describe access level of table in description
                e.g., "BROAD"/"NARROW"/"STRICT"
            auto_annotate: automatically adds semantic types to commonly named
                fields; if auto_annotate, you do not need to pass in a schema
            exact_match: for auto_annoation, defaults to ignoring case and
                underscores when matching field names
        """
        table = BqSingleTable(
            project or self.bqt_obj.bq_client.project, dataset, table
        )

        changes = []

        if auto_annotate:
            if schema:
                self.bqt_obj.print_msg(
                    "With auto_annotate, there's no need to pass in a schema "
                    "and it is actually ignored.",
                    _type="warn"
                )
            schema = table.schema
            schema = self.add_annotation_to_schema(schema,
                                                   FIELD_DESCRIPTION_MAP,
                                                   exact_match)

        if schema:
            table.table_obj.schema = self._schema_from_dict(schema)
            changes.append("schema")
        if description_access_tier and description:
            description = "{ policy: { accessTier: %s }, description: '%s'}" % (
                           description_access_tier, json.dumps(description))
        elif description_access_tier:
            description = "{ policy: { accessTier: %s } }" % (
                description_access_tier)
        if description:
            table.table_obj.description = description
            changes.append("description")
        self.bqt_obj.bq_client.update_table(table.table_obj, changes)

    def _find_field(self, schema, name):
        if not len(schema):
            return None
        for f in schema:
            if f["name"] == name:
                return f
        return None

    def _find_path(self, schema, path):
        tokens = path.split(".")
        current = schema
        f = None
        for t in tokens:
            f = self._find_field(current, t)
            if not f:
                break
            try:
                current = f["fields"]
            except KeyError:
                break
        return f

    @expose_as_api
    def update_field_description(self, dataset, table, project=None,
                                 field=None, description=None):
        """Update description for a single field

        Args:
            dataset (string): dataset of destimation table
            table (string): destination table name
            project (string): project of the table
            field (string): name of the field to be updated, use dot notation
                to specify fields within structs, e.g.: "artist.gid"
            description (string): description for the field
        """
        table = BqSingleTable(
            project or self.bqt_obj.bq_client.project, dataset, table
        )

        schema_dict = table.schema
        found_field = self._find_path(schema_dict, field)
        if found_field:
            found_field["description"] = description
        else:
            self.bqt_obj.print_msg(
                "Field not found",
                _type="warn"
            )
            return

        self.update_table_metadata(table=table,
                                   dataset=dataset,
                                   project=project,
                                   schema=schema_dict)

    @expose_as_api
    def add_annotation_to_schema(self, schema,
                                 field_description_map=FIELD_DESCRIPTION_MAP,
                                 exact_match=True):
        """Update description for a single field

        Args:
            schema (list of dictionaries): in format from BqSingleTable().schema
            field_description_map (dictionary): name of field and description
                Example:
                    {"user_id":"{ policy: { semanticType: userId } }",
                    "reg_country":"{ policy: { semanticType: country } }",
                    "reportingCountry":"{ policy: { semanticType: country } }",
                    "streams":"streams from past day"}
            exact_match (bool): if False, it will ignore case and remove all
                underscores in key and field when matching
        """
        def _iterate_through_keys_and_update(d, exact_match):
            # if contain 'fields' as value, dive in and keep iterating
            if "fields" in d:
                for field in d['fields']:
                    _iterate_through_keys_and_update(field, exact_match)
            else:
                try:
                    # take field name and try looking it up in the field map
                    if exact_match:
                        d['description'] = field_description_map[d['name']]
                    else:
                        d['description'] = (field_description_map[d['name']
                                            .lower().replace("_", "")])
                except KeyError:
                    pass

        if not exact_match and field_description_map != FIELD_DESCRIPTION_MAP:
            field_description_map = {
                k.lower(): v for k, v in field_description_map.items()
            }

        for d in schema:
            _iterate_through_keys_and_update(d, exact_match)
        return schema

    @expose_as_api
    def delete_table(self, tables, dataset=None, project=None, confirm=True):
        """Delete a BqTable object

        Args:
            tables (mixed): table or tables to delete, this can either be
                a single table name (string) or a BqTable object
            dataset (string): required if tables is a single table name,
                the dataset the table belongs to
            project (string): project of the table, only used when `tables`
                is a string and will default to the BqT project
            confirm (bool): if True, the call will explicitly confirm before
                deleting anything
        """
        tables = BqTable(
            tables, dataset=dataset, project=project or self.bqt_obj.project
        )
        if confirm and not PrintTools.confirm_action(
                "Are you sure you want to delete %s tables?" % len(tables)):
            return False
        for table in tables:
            if table.exists:
                self.bqt_obj.print_msg("Deleting %s ..." % str(table))
                self.bqt_obj.bq_client.delete_table(table.table_ref)
            else:
                self.bqt_obj.print_msg(
                    "Table `%s` doesn't exist" % repr(table)
                )

    @expose_as_api
    def delete_all_partitions(self, table, dataset, project=None,
                              confirm=True):
        """Delete all partitions of a table

        Args:
            table (string): table name WITH partition format,
                e.g. my_nice_table_YYYYMMDD
            dataset (string): dataset the table belongs to
            project (string): project of the table, only used when `tables`
                is a string and will default to the BqT project
            confirm (bool): if True, the call will explicitly confirm before
                deleting anything
        """
        tables = BqTable(
            name=table, dataset=dataset,
            project=project or self.bqt_obj.project
        )
        return self.delete_table(tables, confirm=confirm)

    @expose_as_api
    def delete_partition_range(self, table, dataset, start, end, project=None,
                               confirm=True):
        """delete a specific range of tables

        Args:
            table (string): table name WITH partition format,
                e.g. my_nice_table_YYYYMMDD
            dataset (string): dataset the table belongs to
            start (date like): start date, inclusive
            end (date like): end date, inclusive
            project (string): project of the table, only used when `tables`
                is a string and will default to the BqT project
            confirm (bool): if True, the call will explicitly confirm before
                deleting anything
        """
        tables = BqTable(
            name=table, dataset=dataset,
            project=project or self.bqt_obj.project,
            start=start, end=end
        )
        return self.delete_table(tables, confirm=confirm)

    @expose_as_api
    def copy_table(self, src_tables, dst_tables,
                   src_dataset=None, dst_dataset=None,
                   src_project=None, dst_project=None):
        """Copy a BqTable object

        Args:
            src_tables (mixed): src tables to copy
            dst_tables (mixed): dst_tables to copy to
            src_dataset (string): source table's dataset, only used if
                `src_tables` is a string for table name
            dst_dataset (string): destination table's dataset, only used if
                `dst_tables` is a string for table name
            src_project (string): source table's project, only used if
                `src_tables` is a string for table name
            dst_project (string): destination table's project, only used if
                `dst_tables` is a string for table name
        Returns:
            list(BqJobWithoutResults): list of jobs that were queued
        """
        src_tables = list(BqTable(
            src_tables, dataset=src_dataset,
            project=src_project or self.bqt_obj.project
        ))
        dst_tables = list(BqTable(
            dst_tables, dataset=dst_dataset,
            project=dst_project or self.bqt_obj.project
        ))
        if len(src_tables) != len(dst_tables):
            raise ValueError(
                "Number of tables in source and destination are different: "
                "%s vs. %s" % (len(src_tables), len(dst_tables))

            )
        jobs = []
        for i in range(len(src_tables)):
            src = src_tables[i]
            dst = dst_tables[i]
            if dst.exists:
                self.bqt_obj.print_msg(
                    "Skipping `%s`, destination already exists" % str(src),
                    _type="warn"
                )
            else:
                jobs.append(BqJobManager.queue_job(
                    BqJobWithoutResults(
                        self.bqt_obj.bq_client.copy_table(
                            src.table_ref, dst.table_ref), self.bqt_obj
                    )
                ))
                self.bqt_obj.print_msg(
                    "Copying `%s` to `%s`" % (str(src), str(dst))
                )
        return jobs

    @expose_as_api
    def copy_all_partitions(self, src_table, src_dataset, src_project,
                            dst_table, dst_dataset, dst_project):
        """Copy all partitions of a table from `src_*` to `dst_*`

        Args:
            src_table (string): table name WITH partition format,
                e.g. my_nice_table_YYYYMMDD
            src_dataset (string): source dataset
            src_project (string): source project
            dst_table (string): table name WITH partition format,
                e.g. my_nice_table_YYYYMMDD
            dst_dataset (string): source dataset
            dst_project (string): source project
        Returns:
            list(BqJobWithoutResults): list of jobs that were queued
        """
        src_tables = BqTable(
            name=src_table, dataset=src_dataset,
            project=src_project or self.bqt_obj.project
        )
        dst_tables = BqTable.clone_with_new_name(
            src_tables, dst_table, dst_dataset,
            dst_project or self.bqt_obj.project,
        )
        return self.copy_table(src_tables, dst_tables)

    @expose_as_api
    def copy_partition_range(self, src_table, src_dataset, src_project,
                             dst_table, dst_dataset, dst_project, start, end):
        """Copy a range of partitions of a table from `src_*` to `dst_*`

        Args:
            src_table (string): table name WITH partition format,
                e.g. my_nice_table_YYYYMMDD
            src_dataset (string): source dataset
            src_project (string): source project
            dst_table (string): table name WITH partition format,
                e.g. my_nice_table_YYYYMMDD
            dst_dataset (string): source dataset
            dst_project (string): source project
            start (date line): start partition, inclusive
            end (date line): end partition, inclusive
        Returns:
            list(BqJobWithoutResults): list of jobs that were queued
        """
        src_tables = BqTable(
            name=src_table, dataset=src_dataset,
            project=src_project or self.bqt_obj.project,
            start=start, end=end
        )
        dst_tables = BqTable.create_new_range(
            dst_table, dst_dataset, dst_project or self.bqt_obj.project,
            start, end
        )
        return self.copy_table(src_tables, dst_tables)

    @expose_as_api
    def undelete_table_async(self, deleted_table, new_table, dataset, snapshot,
                             project=None):
        """Bring back a table using an older snapshot of a data,


        Note: undeleting is only possible for a period of 7 days after the
            table is deleted
        Note: You cannot reference a deleted table if a table bearing the same
              ID in the dataset was created after the deletion time
        Note: You cannot reference a deleted table if the encapsulating
              dataset was also deleted/recreated since the table deletion event
        Note: this is an async call and will return immediately, you have
              to wait for the job to finish using the job object returned

        Args:
            deleted_table (string): deleted table that needs resurrection
            new_table (string): table name to resurrect `delete_table` as
            dataset (string): dataset name
            snapshot (date like): UTC time to use to pull data snapshot from
                MUST BE WITHIN THE LAST 7 DAYS
            project (string): project name, defaults to BqT project
        """
        query = """
        SELECT * FROM `%s.%s.%s` FOR SYSTEM TIME AS OF '%s'
        """
        query %= (
            project or self.bqt_obj.project,
            dataset,
            deleted_table,
            arrow.get(snapshot).format('YYYY-MM-DD HH:mm:ss-00:00')
        )
        return self.create_table_async(
            query, dataset, new_table, project=project
        )

    @expose_as_api
    def undelete_table(self, deleted_table, new_table, dataset, snapshot,
                       project=None):
        """Bring back a table using an older snapshot of a data

        Note: undeleting is only possible for a period of 7 days after the
            table is deleted
        Note: You cannot reference a deleted table if a table bearing the same
              ID in the dataset was created after the deletion time
        Note: You cannot reference a deleted table if the encapsulating
              dataset was also deleted/recreated since the table deletion event

        Args:
            deleted_table (string): deleted table that needs resurrection
            new_table (string): table name to resurrect `delete_table` as
            dataset (string): dataset name
            snapshot (date like): UTC time to use to pull data snapshot from
                MUST BE WITHIN THE LAST 7 DAYS
            project (string): project name, defaults to BqT project
        """
        job = self.undelete_table_async(
            deleted_table, new_table, dataset, snapshot, project=project
        )
        if job:
            BqJobManager.wait_for_job(job)
        return job

    @expose_as_api
    def update_view_query(self, dataset, table, new_query,
                          project=None, confirm=True):
        """Update the query for a view

        Args:
            dataset (string): dataset of the view
            table (string): table name of the view
            project (string): project of the view
            new_query (string): new query for the view
            confirm (bool): confirm before replacing query in the view

        Example:
        >>> bqt.update_view_query(dataset='my_dataset', table='my_view',
                                  new_query='SELECT 1 as one')
        """
        project = project or self.bqt_obj.bq_client.project
        view = BqSingleTable(project, dataset, table).table_obj
        if not view.table_type == 'VIEW':
            raise RuntimeError("`%s` is not a view" % view.full_table_id)
        if confirm and not PrintTools.confirm_action(
            f"Current view will be replaced with \n {new_query} \n"
            "OK?"
        ):
            self.bqt_obj.print_msg(
                "Action cancelled, "
                "did not replace the view.", _type="warn")
            return
        view.view_query = new_query
        self.bqt_obj.bq_client.update_table(view, ["view_query"])

    @expose_as_api
    def fast_load(self, dataframe, dataset, table, project=None,
                  staging_bucket=None, write_disposition='WRITE_EMPTY'):
        """Atomic load of a pandas DataFrame to BigQuery using parallel uploads,
           compressed parquet files and GCS
           This loading is much faster (on big DataFrames) compare to
           Google BigQuery API and pandas gbq!

        Note: The user or running service account
            must have storage.buckets access

        Args:
            dataframe (pandas.DataFrame):
                DataFrame to be written to a BigQuery table
            dataset (string):
                name of dataset to be written
            table (string):
                name of table to be written
            project (string, optional):
                name of project to be written
                if not set default project from bigquery client will be used
            staging_bucket (string, optional):
                name of bucket to store the intermediate data
                if not set a default bucket will be created
            write_disposition: Action that occurs if the destination table
                already exists. If `BIGQUERY_OVERWRITE` env variable is "true",
                automatically set it to 'WRITE_TRUNCATE'

        Example:
            >>> import pandas as pd
            >>> import numpy as np
            >>> from bqt import bqt
            >>> df = pd.DataFrame([[1, 'v1', 1.2], [2, np.NaN, 2.2]],
            >>>                   columns=['c1', 'c2', 'c3'])
            >>> bqt.fast_load(df, 'my_dataset', 'my_table')
        """
        start = arrow.now()
        project = project or self.bqt_obj.bq_client.project
        table = self.bqt_obj.parameter_manager.insert_params(table)
        table = BqSingleTable(project, dataset, table)
        # backstage overwrite mode
        if os.environ.get('BIGQUERY_OVERWRITE', '').lower() == "true":
            write_disposition = 'WRITE_TRUNCATE'
        if table.exists and write_disposition == 'WRITE_EMPTY':
            raise Conflict("Already Exists: {}".format(repr(table)))
        location = self.bqt_obj.bq_client \
            .get_dataset(table.dataset_obj).location

        # create gcs staging bucket
        bucket = self.bqt_obj.create_staging_bucket(staging_bucket, location)
        if bucket.location != location:
            raise BadRequest("Cannot read and write in different locations: "
                             "staging_bucket: {}, destination dataset: {}"
                             .format(bucket.location, location))

        # create local temp directory
        tmp_dir = self.bqt_obj.get_config("cache.local.temp_dir")
        blob_name = self.bqt_obj.get_config("fastbqt.fastload_blob_name") \
            .format(uuid.uuid4().hex, arrow.now().format("YYYYMMDDHHmmss"))
        tmp_dir = os.path.join(tmp_dir, blob_name)
        os.mkdir(tmp_dir)

        # get bq temp table reference under user's temp dataset
        tmp_dataset_id = "_fastload_{}{}".format(
            location.lower(),
            self.bqt_obj.bq_client.query("SELECT 1").result()._table.dataset_id)
        tmp_dataset = bigquery.Dataset("{}.{}".format(project, tmp_dataset_id))
        tmp_dataset.location = location
        tmp_dataset.default_table_expiration_ms = 6 * 60 * 60 * 1000  # 6 hours
        self.bqt_obj.bq_client.create_dataset(tmp_dataset, exists_ok=True)
        tmp_table_ref = tmp_dataset.table(blob_name)

        # split the dataframe into chunks
        chunk_size = self.bqt_obj.get_config("fastbqt.fastload_chunk_size")
        n_chunks = min(math.ceil(len(dataframe.index) / chunk_size), 10)
        PrintTools(
            "Splitting the dataframe into {} chunks ...".format(n_chunks),
            _type=PrintTools.INFO)
        sub_dfs = np.array_split(dataframe, n_chunks)

        # write sub dataframes in parallel into temp bq table
        threads = []
        pyarrowschema = pa.Schema.from_pandas(dataframe, preserve_index=False)
        for ix, df in enumerate(sub_dfs):
            file_name = "{{}}{sep}{ix:012d}.parquet".format(sep=os.sep, ix=ix)
            blob = bucket.blob(file_name.format(blob_name))
            file_dir = file_name.format(tmp_dir)
            # we need to force the main schema because this chunk may have all
            # nans on any column that will be detected as integer by parquet!
            pyarrowtable = pa.Table.from_pandas(df, schema=pyarrowschema,
                                                preserve_index=False)
            pq.write_table(pyarrowtable, file_dir)
            t = ExcThread(target=self._file_to_bq,
                          args=(file_dir, blob, tmp_table_ref,
                                location, ix, n_chunks))
            threads.append((t, file_dir))
            t.start()
        for t, file_dir in threads:
            t.join()
            os.remove(file_dir)
        os.rmdir(tmp_dir)

        # copy temp bq table into destination table
        # to make the whole load process atomic

        config = bigquery.job.CopyJobConfig()
        config.write_disposition = write_disposition

        self.bqt_obj.bq_client.copy_table(tmp_table_ref, table.table_ref,
                                          location=location,
                                          job_config=config)

        end = arrow.now()
        PrintTools(
            "Time elapsed: {}".format(end - start), _type=PrintTools.SUCCESS)

    def _file_to_bq(self, file_dir, blob, table_ref, location, ix, n_chunks):
        PrintTools(
            "Compressed chunk {}/{} ({}) is being uploaded ...".format(
                ix + 1, n_chunks,
                PrintTools.human_number(os.path.getsize(file_dir))),
            _type=PrintTools.INFO)

        blob.upload_from_filename(file_dir)
        gcs_uri = "gs://{}/{}".format(blob.bucket.name, blob.name)
        load_conf = bigquery.LoadJobConfig()
        load_conf.create_disposition = \
            bigquery.CreateDisposition.CREATE_IF_NEEDED
        load_conf.source_format = bigquery.SourceFormat.PARQUET
        load_conf.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        load_job = self.bqt_obj.bq_client.load_table_from_uri(
            gcs_uri, table_ref, location=location, job_config=load_conf,
            job_id_prefix='gcs_to_bq_loader')
        load_job.result()
        try:
            blob.delete()
        except NotFound:
            pass

        PrintTools(
            "Chunk {}/{} has been inserted to bq".format(ix + 1, n_chunks),
            _type=PrintTools.INFO)

    def _schema_from_dict(self, schema_d):
        """
            Take as input a mapped dictionary for a table format, e.g.,
            BqSingleTable.schema, and convert it into BQ schemafield format
            which can be passed to create_table() method or
            update_table_metadata() method
        """
        return [bigquery.SchemaField.from_api_repr(field) for field in schema_d]


class ExcThread(Thread):
    def run(self):
        self.exc = None
        try:
            if hasattr(self, '_Thread__target'):
                self.ret = self._Thread__target(*self._Thread__args,
                                                **self._Thread__kwargs)
            else:
                self.ret = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self.exc = e

    def join(self):
        super(ExcThread, self).join()
        if self.exc:
            raise self.exc
        return self.ret

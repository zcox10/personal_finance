import multiprocessing.dummy as thread
import pandas as pd
import arrow
import yaml
from fuzzywuzzy import fuzz
from functools import reduce
from bqt.lib.decorators import expose_as_api
from itertools import product
from bqt.lib.bq_audit_queries import audit_modifier_query, audit_access_query
from bqt.lib.table import BqTable


class ProjectAnalyzer:
    def __init__(self, bqt_obj):
        self.bqt_obj = bqt_obj

    def get_tables_by_project(self, project):
        datasets = [(ds.dataset_id, project) for ds in
                    self.bqt_obj.bq_client.list_datasets(project)]
        parallelism = self.bqt_obj.get_config("bq_insight.parallelism")
        with thread.Pool(parallelism) as pool:
            tables = pool.starmap(self.get_tables_by_dataset, datasets)
        tables = reduce(lambda x, y: x + y, tables)
        return tables

    def get_tables_by_dataset(self, dataset, project=None):
        ds = self.bqt_obj.bq_client.dataset(dataset, project)
        tables = list(self.bqt_obj.bq_client.list_tables(ds))
        table_ids = {}
        for tbl in tables:
            partition_type, name = BqTable.find_partition_type(tbl.table_id)
            if partition_type == 'YYYYMMDD':
                partition = tbl.table_id[-len(partition_type):]
                if name in table_ids:
                    table_ids[name].append(partition)
                else:
                    table_ids[name] = [partition]
            else:
                table_ids[tbl.table_id] = None

        tables = []
        for table_prefix, partition in table_ids.items():
            max_partition = max(partition) if partition is not None else ''
            table_ref = ds.table(table_prefix + max_partition)
            table = self.bqt_obj.bq_client.get_table(table_ref)
            tables.append((table_prefix, table))
        return tables

    def _is_annotated(self, description, is_table):
        if description is None:
            return False
        if is_table:
            keywords = self.bqt_obj.get_config(
                'bq_insight.table_annotation_keywords')
        else:
            keywords = self.bqt_obj.get_config(
                'bq_insight.field_annotation_keywords')
        kw = keywords.split('.')
        try:
            yaml.safe_load(description)[kw[0]][kw[1]]
            return True
        except Exception:
            return False

    def _is_sensitive(self, field_name):
        field_name = field_name.lower()
        for semantic_type in self.bqt_obj.get_config(
                'bq_insight.semantic_types'):
            if semantic_type in field_name:
                return True
            r = fuzz.ratio(field_name, semantic_type)
            if r > self.bqt_obj.get_config(
                    'bq_insight.fuzzy_matching_threshold'):
                return True
        return False

    @staticmethod
    def _get_last_modifier(insight, modifier):
        if (insight['dataset'] == modifier['dataset'] and
                modifier['table'].startswith(insight['table_prefix'])):
            return (insight['dataset'],
                    insight['table_prefix'],
                    modifier['last_modifier'],
                    modifier['max_timestamp'])
        return None

    def _get_last_modifiers(self, project, df_insight):
        df_modifier = self.bqt_obj.query(
            audit_modifier_query.format(project=project),
            renew_cache=True)

        parallelism = self.bqt_obj.get_config("bq_insight.parallelism")
        with thread.Pool(parallelism) as pool:
            cartesian_input = product(df_insight.to_records(),
                                      df_modifier.to_records())
            rows = pool.starmap(self._get_last_modifier, cartesian_input)
            rows = list(filter(lambda a: a is not None, rows))

        df_modifier_prefix = pd.DataFrame(rows, columns=['dataset',
                                                         'table_prefix',
                                                         'last_modifier',
                                                         'max_timestamp'])
        df_modifier_prefix_max = (
            df_modifier_prefix.groupby(['dataset', 'table_prefix'])[
                'max_timestamp'].max().reset_index())
        df_modifier_prefix = pd.merge(df_modifier_prefix,
                                      df_modifier_prefix_max,
                                      on=['dataset', 'table_prefix',
                                          'max_timestamp'])
        df_last_modifier = df_modifier_prefix[
            ['dataset', 'table_prefix', 'last_modifier']]
        return df_last_modifier

    @staticmethod
    def _get_last_access_range(insight, rng):
        if (insight['dataset'] == rng['dataset'] and
                insight['table'].startswith(rng['table'][:-1])):
            return (insight['dataset'],
                    insight['table_prefix'],
                    rng['last_accessed'])
        return None

    @staticmethod
    def _get_last_access_full_name(insight, full):
        if (insight['dataset'] == full['dataset'] and
                full['table'].startswith(insight['table_prefix'])):
            return (insight['dataset'],
                    insight['table_prefix'],
                    full['last_accessed'])
        return None

    def _get_last_accesses(self, project, df_insight):
        df_access = self.bqt_obj.query(
            audit_access_query.format(project=project),
            renew_cache=True)
        df_full_name_tables = df_access[~df_access['table'].str.endswith('*')]
        df_range_tables = df_access[(df_access['table'].str.endswith('*')) &
                                    (df_access['table'] != '*')]

        parallelism = self.bqt_obj.get_config("bq_insight.parallelism")
        with thread.Pool(parallelism) as pool:
            cartesian_full = product(df_insight.to_records(),
                                     df_full_name_tables.to_records())
            rows_full = pool.starmap(self._get_last_access_full_name,
                                     cartesian_full)
            rows_full = list(filter(lambda a: a is not None, rows_full))

            cartesian_range = product(df_insight.to_records(),
                                      df_range_tables.to_records())
            rows_range = pool.starmap(self._get_last_access_range,
                                      cartesian_range)
            rows_range = list(filter(lambda a: a is not None, rows_range))

        df_access_prefix = pd.DataFrame(rows_full + rows_range,
                                        columns=['dataset',
                                                 'table_prefix',
                                                 'last_accessed'])
        df_access_prefix_max = (
            df_access_prefix.groupby(['dataset', 'table_prefix'])[
                'last_accessed'].max().reset_index())
        df_access_prefix_max['last_accessed'] = (
            df_access_prefix_max['last_accessed'].dt.date)
        return df_access_prefix_max

    @expose_as_api
    def bq_insight(self, project, by_retention_importance=True):
        """Scan a BigQuery project to extract a full insight with the following
            fields:
            dataset, table, table_type, annotated, retained_infinite,
            days_to_expire, created, expires, last_accessed, last_modifier,
            location, sensitive_fields

        Note: last_accessed and last_modifier are fetched from
            bigquery_audit_logs which by default have 90 days history.
            sensitive_fields is a fuzzy matching list of unannotated fields that
            could potentially be sensitive.

        Args:
            project (string):
                name of project to be scanned
            by_retention_importance (bool):
                sort the output by the tables that will expire the first.
                filter views and bigquery_audit_logs from the output.
        Returns:
            pandas.DataFrame

        Example:
            >>> from bqt import bqt
            >>> df = bqt.bq_insight('my_project')
        """
        all_tables = self.get_tables_by_project(project)
        all_rows = []
        for table_prefix, table in all_tables:
            annotated = self._is_annotated(table.description, True)
            sensitive_fields = None
            for field in table.schema:
                field_annotated = self._is_annotated(field.description, False)
                if not annotated:
                    annotated = field_annotated

                if not field_annotated and self._is_sensitive(field.name):
                    if sensitive_fields:
                        sensitive_fields += '|{}'.format(field.name)
                    else:
                        sensitive_fields = field.name
            expires = table.expires.date() if table.expires else None
            days_to_expire = (
                (expires - arrow.now().date()).days if expires else None)
            retained_infinite = annotated and expires is None
            row = (table.dataset_id,
                   table_prefix,
                   table.table_id,
                   table.table_type,
                   annotated,
                   retained_infinite,
                   days_to_expire,
                   table.created.date(),
                   expires,
                   table.location,
                   sensitive_fields)
            all_rows.append(row)

        df_insight = pd.DataFrame(all_rows,
                                  columns=['dataset', 'table_prefix', 'table',
                                           'table_type', 'annotated',
                                           'retained_infinite',
                                           'days_to_expire', 'created',
                                           'expires', 'location',
                                           'sensitive_fields'])
        df_last_modifier = self._get_last_modifiers(project, df_insight)
        df_last_access = self._get_last_accesses(project, df_insight)

        df_insight = pd.merge(df_insight, df_last_modifier,
                              how='left', on=['dataset', 'table_prefix'])
        df_insight = pd.merge(df_insight, df_last_access,
                              how='left', on=['dataset', 'table_prefix'])
        df_insight = df_insight[['dataset', 'table', 'table_type',
                                 'annotated', 'retained_infinite',
                                 'days_to_expire', 'created', 'expires',
                                 'last_accessed', 'last_modifier', 'location',
                                 'sensitive_fields']]
        if by_retention_importance:
            df_insight = df_insight[
                (df_insight['table_type'] != 'VIEW') &
                (df_insight['dataset'] != 'bigquery_audit_logs')]
            df_insight.sort_values(
                by=['days_to_expire', 'created', 'annotated'],
                inplace=True)
        return df_insight

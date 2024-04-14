import arrow
import re

try:
    import pandas
except ImportError:
    pandas = None


class TableAnalyzer(object):
    """Class for analyzing tables, duh

    This class aims at providing general statistics not specific to a type of
    table
    """

    # converts BQ partition format to PARSE_DATE arg
    partition_format_parsers = {
        'YYYYMMDD': '%Y%m%d',
        'YYYYMM': '%Y%m',
        'YYYYMMDDHH': '%Y%m%d%H',
        'YYYYMMDDTHH': '%Y%m%dT%H'
    }

    def __init__(self, table, bqt_obj):
        self.table = table
        self.bqt_obj = bqt_obj
        self.analysis_data = {}

    def get_missing_partitions(self):
        """Get a dataframe of missing partitions

        Returns:
            pandas.DataFrame
        """
        missing_partitions = self.partition_info[
            self.partition_info['diff'] > 1
        ]
        missing = []
        for index, row in missing_partitions.iterrows():
            arrow_format = self.table.partition_formats[
                self.table.partition_format
            ]
            for i in range(int(row['diff']) - 1):
                missing.append(
                    arrow.get(row['start_partition']).shift(
                        **{arrow_format: i + 1}
                    ).datetime
                )
        return pandas.DataFrame(missing, columns=['Missing Partition'])

    @property
    def has_missing_partitions(self):
        return len(self.get_missing_partitions()) > 0

    @property
    def has_consistent_schema(self):
        return len(self.schema_history) == 0

    @property
    def schema_history(self):
        if pandas is None:
            raise RuntimeError("Pandas is required by `schema_history()`")
        return pandas.DataFrame(self._get_schemas_changes())

    @property
    def latest_schema(self):
        last_partition = list(self.table)[-1]
        return last_partition.table_obj.schema

    @property
    def partition_info(self):
        if 'partition_info' not in self.analysis_data:
            self._get_partition_info()
        return self.analysis_data['partition_info']

    def get_column_statistics(self):
        if 'column_statistics' not in self.analysis_data:
            self._get_column_statistics(full=False)
        return self.analysis_data['column_statistics']

    def get_column_statistics_for_all_partitions(self):
        if 'column_statistics_full' not in self.analysis_data:
            self._get_column_statistics(full=True)
        return self.analysis_data['column_statistics_full']

    def _get_partition_info(self):
        """Get information about the table partitions from BQ

        Returns:
            None, will set `self.analysis_data['partition_info']` instead
        """
        partition_format = self.table.partition_format
        if partition_format is None:
            query = """
            SELECT table_id, TIMESTAMP_MILLIS(creation_time) AS creation_time,
                   row_count, size_bytes,
                   NULL AS start_partition, NULL AS next_partition,
                   NULL AS diff
            FROM `{project}.{dataset}.__TABLES__`
            WHERE table_id = '{table}'
            """.format(
                project=self.table.project,
                dataset=self.table.dataset,
                table=self.table.name
            )
        elif partition_format in ('YYYYMMDDHH', 'YYYYMMDDTHH'):
            query = """
            WITH table_info AS (
              SELECT PARSE_DATETIME(
                        '{format}', SUBSTR(table_id, -{partition_len})) p,
                     table_id, creation_time, row_count, size_bytes
              FROM `{project}.{dataset}.__TABLES__`
              WHERE SUBSTR(table_id, 0, LENGTH(table_id) - {partition_len})
                        = '{table_prefix}'
            )
            SELECT table_id, TIMESTAMP_MILLIS(creation_time) AS creation_time,
                   row_count, size_bytes,
                   p AS start_partition,
                   LAG(p) OVER (ORDER BY p DESC) AS next_partition,
                   DATETIME_DIFF(
                    LAG(p) OVER (ORDER BY p DESC), p, HOUR) AS diff
            FROM table_info
            """.format(
                format=self.partition_format_parsers[partition_format],
                project=self.table.project,
                dataset=self.table.dataset,
                partition_len=len(partition_format),
                table_prefix=self.table.name_without_partition
            )
        else:
            query = """
            WITH table_info AS (
              SELECT PARSE_DATE(
                    '{format}', SUBSTR(table_id, -{partition_len})) p,
                     table_id, creation_time, row_count, size_bytes
              FROM `{project}.{dataset}.__TABLES__`
              WHERE SUBSTR(table_id, 0, LENGTH(table_id) - {partition_len})
                        = '{table_prefix}'
            )
            SELECT table_id, TIMESTAMP_MILLIS(creation_time) AS creation_time,
                   row_count, size_bytes,
                   p AS start_partition,
                   LAG(p) OVER (ORDER BY p DESC) AS next_partition,
                   DATE_DIFF(LAG(p) OVER (ORDER BY p DESC), p, DAY) AS diff
            FROM table_info
            """.format(
                format=self.partition_format_parsers[partition_format],
                project=self.table.project,
                dataset=self.table.dataset,
                partition_len=len(partition_format),
                table_prefix=self.table.name_without_partition
            )

        results = self.bqt_obj.query(query)
        # cause BQ is stupid and returns column names that correspond to
        # functions as all upper case (row_count in this case)
        results.columns = [x.lower() for x in results.columns]
        self.analysis_data['partition_info'] = results

    def get_column_stats(self, columns, measures, only_latest=True):
        """Get columns statistics based on `measures`

        Args:
            columns (list(string)): list of columns to describe
            measures (list(string)): list of metrics to use for the description
            only_latest (bool): whether to query all partitions or just the
                latest. if True, start_partition and end_partition are ignored
        Returns:
            DataFrame with statistics based on `columns` and `measures`
        """
        measure_names = zip(measures, [
            self._create_column_name(m) for m in measures
        ])
        rank_measure_names = zip(measures, [
            self._create_column_name(m) for m in measures
        ])
        order_by_str = ', '.join(['%s DESC' % m for m, n in measure_names])
        measure_str = ', '.join(['%s AS %s' % (m, n) for m, n in rank_measure_names])

        partition_format = self.table.partition_format

        if partition_format is None or only_latest:
            query = """
            SELECT NULL part, {columns}, {measures}, 1 AS rank
            FROM `{project}.{dataset}.{table}`
            GROUP BY {group_by}
            ORDER BY {order_by}
            LIMIT 5000 -- just a safeguard incase cardinality is really high
            """.format(
                project=self.table.project,
                dataset=self.table.dataset,
                table=(
                    str(list(self.table)[-1])
                    if only_latest else self.table.name
                ),
                columns=', '.join(columns),
                measures=measure_str,
                order_by=order_by_str,
                group_by=', '.join(map(str, range(1, len(columns) + 2)))
            )
        elif partition_format in ('YYYYMMDDHH', 'YYYYMMDDTHH'):
            query = """
            WITH data AS (
                SELECT PARSE_DATETIME('{format}', _TABLE_SUFFIX) part,
                       {columns}, {measures},
                       RANK() OVER (PARTITION BY {columns} ORDER BY {order_by})
                        AS rank
                FROM `{project}.{dataset}.{table_prefix}*`
                {prefix_condition}
                GROUP BY {group_by}
            )
            SELECT * FROM data
            WHERE rank < 5000 -- safeguard incase cardinality is really high
            """.format(
                format=self.partition_format_parsers[partition_format],
                project=self.table.project,
                dataset=self.table.dataset,
                table_prefix=self.table.name_without_partition,
                columns=', '.join(columns),
                measures=measure_str,
                order_by=order_by_str,
                group_by=', '.join(map(str, range(1, len(columns) + 2))),
                prefix_condition=(
                    "WHERE BETWEEN '%s' AND '%s'" % (
                        self.table.start.format(partition_format),
                        self.table.end.format(partition_format),
                    ) if self.table.start else ''
                )
            )
        else:
            query = """
            WITH data AS (
                SELECT PARSE_DATE('{format}', _TABLE_SUFFIX) part,
                       {columns}, {measures},
                       RANK() OVER (PARTITION BY {columns} ORDER BY {order_by})
                        AS rank
                FROM `{project}.{dataset}.{table_prefix}*`
                {prefix_condition}
                GROUP BY {group_by}
            )
            SELECT * FROM data
            WHERE rank < 5000 -- safeguard incase cardinality is really high
            """.format(
                format=self.partition_format_parsers[partition_format],
                project=self.table.project,
                dataset=self.table.dataset,
                table_prefix=self.table.name_without_partition,
                columns=', '.join(columns),
                measures=measure_str,
                order_by=order_by_str,
                group_by=', '.join(map(str, range(1, len(columns) + 2))),
                prefix_condition=(
                    "WHERE BETWEEN '%s' AND '%s'" % (
                        self.table.start.format(partition_format),
                        self.table.end.format(partition_format),
                    ) if self.table.start else ''
                )
            )
        res = self.bqt_obj.query(query)
        del res['rank']
        return res.sort_values([n for m, n in measure_names], ascending=False)

    def _create_column_name(self, name):
        """Turn a SQL expression into a valid identifier

        Args:
            name (string): SQL expression
        Returns:
            string, valid SQL identifier
        """
        reps = {
            '_': ['(', ')', '_', '-', ' '],
            'all': ['*']
        }
        for sub in reps:
            for search in reps[sub]:
                name = name.replace(search, sub)
        name = re.sub(r'[^a-zA-Z0-9_]', r'', name)
        return '_'.join(
            [x for x in name.split('_') if x != '']
        ).lower().strip('_')

    def _get_schemas_changes(self):
        """Get a list of schema changes

        Schema changes are changes between two consecutive partitions

        Returns:
            list(dict(field, change_type, from, to, from_table, to_table))
        """
        last_schema = None
        schema_changes = []
        for table in self.table:
            if last_schema is None:
                last_schema = table.table_obj.schema
                continue
            changes = self._get_schema_diff(
                last_schema, table.table_obj.schema
            )
            if changes:
                for change in changes:
                    change['from_table'] = str(last_schema)
                    change['to_table'] = str(table.table_obj.schema)
                schema_changes += changes
            last_schema = table.table_obj.schema
        return schema_changes

    def _get_schema_diff(self, old, new):
        """Get the differences between schemas

        Args:
            old (list(SchemaField)): old schema
            new (list(SchemaField)): new schema
        Returns:
            changes between the two schemas as:
                list(dict(field, change_type, from, to, from_table, to_table))
        """
        changes = []
        old_by_name = {f.name: f for f in old}
        new_by_name = {f.name: f for f in new}
        for name in old_by_name:
            if name not in new_by_name:
                changes.append({
                    'field': name,
                    'change_type': 'field_removed',
                    'from': name,
                    'to': None
                })
                continue
            old_f = old_by_name[name]
            new_f = new_by_name[name]
            if old_f.field_type != new_f.field_type:
                changes.append({
                    'field': name,
                    'change_type': 'type_change',
                    'from': old_f.field_type,
                    'to': new_f.field_type
                })
            if old_f.mode != new_f.mode:
                changes.append({
                    'field': name,
                    'change_type': 'mode_change',
                    'from': old_f.mode,
                    'to': new_f.mode
                })
        for name in set(new_by_name.keys()) - set(old_by_name.keys()):
            changes.append({
                'field': name,
                'change_type': 'field_added',
                'from': None,
                'to': name
            })
        return changes

    def _get_column_statistics(self, full=False):
        """Get statistics for columns in the table

        Args:
            full (bool): whether to look back at historical partitions or not
        Returns:
            None, sets:
                * self.analysis_data['column_statistics']
                * self.analysis_data['column_statistics_full']
            depending on the value of `full`
        """
        # TODO:
        #  1. add support for nested columns
        if full:
            schema_changes = self.schema_history
            inconsistent_cols = (
                [x['field'] for x in schema_changes] if full else []
            )

        if full:
            base_query = """
            SELECT _TABLE_SUFFIX AS part, {stats}
            FROM `{project}.{dataset}.{table_prefix}*`
            """
        else:
            last_partition = list(self.table)[-1]
            base_query = """
            SELECT {stats}
            FROM `{project}.{dataset}.%s`
            """ % str(last_partition)

        if full and self.table.start:
            base_query += """
            WHERE _TABLE_SUFFIX BETWEEN '{start}' AND '{end}'
            """.format(
                start=self.table.start.format(self.table.partition_format),
                end=self.table.end.format(self.table.partition_format)
            )
        if full:
            base_query += "GROUP BY part"

        stats = ['COUNT(*) AS row_count']
        for field in self.latest_schema:
            c = field.name
            if full and c in inconsistent_cols:
                continue
            stats.append('COUNTIF({c} IS NULL) AS {c}_nulls'.format(c=c))
            if field.mode == 'REPEATED':
                stats.append('AVG(ARRAY_LENGTH({c})) AS {c}_avglength'.format(
                    c=c))
            elif field.field_type == 'STRING':
                stats.append(
                    'APPROX_COUNT_DISTINCT({c}) AS {c}_cardinality'.format(c=c)
                )
            elif field.field_type in ('INTEGER', 'FLOAT', 'NUMERIC'):
                stats.append('AVG({c}) AS {c}_avg'.format(c=c))
                stats.append(
                    'APPROX_QUANTILES({c}, 4) AS {c}_quantiles'.format(c=c)
                )
        data = self.bqt_obj.query(
            base_query.format(
                stats=', '.join(stats),
                project=self.table.project,
                dataset=self.table.dataset,
                table_prefix=self.table.name_without_partition,
            )
        )
        # cause BQ is stupid and returns column names that correspond to
        # functions as all upper case (row_count in this case)
        data.columns = [x.lower() for x in data.columns]

        self.analysis_data[
            'column_statistics%s' % ('_full' if full else '')
        ] = data

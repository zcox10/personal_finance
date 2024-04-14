import arrow
from collections import OrderedDict

from bqt.lib.decorators import expose_as_api
from bqt.lib.table import BqTable
from bqt.lib.table_analyzer import TableAnalyzer

try:
    import pandas
    from IPython.display import Markdown, display
except ImportError:
    pandas = None
    Markdown = str  # fallback to string
    plt = None

    # fallback to print
    def display(s):
        print(s)


class Stats(object):
    """Class to do read operations on BQ data"""

    def __init__(self, bqt_obj):
        self.bqt_obj = bqt_obj

    @expose_as_api
    def describe_table(self, table, dataset, project=None,
                       start_partition=None, end_partition=None):
        """Describe a table

        Prints and returns various statistics about a table

        Note: there are a few ways to call this function:
            1. table='some_table'
                -> will print statistics for a single table
            2. table='some_table_YYYYMMDD'
                -> will also look into historical partition information
            3. table='some_table_YYYYMMDD' AND provide start/end_partition
                -> similar to above but limited to the specified range

        Args:
            table (string): name of the table
            dataset (string): dataset table belongs to
            project (string): project table belongs to, if not provided
                defaults to BqT project
            start_partition (date like): when to start querying stats from
            end_partition (date like): when to end querying stats
        Returns:
            list(DataFrame) where each dataframe corresponds to one of the
                statistics as they are displayed
        """
        if pandas is None:
            raise RuntimeError(
                "describe_table requires pandas to be installed"
            )

        table = BqTable(
            table, start_partition, end_partition, dataset,
            project or self.bqt_obj.project
        )
        summary = []
        count = len(table)
        ta = TableAnalyzer(table, self.bqt_obj)
        summary.append("## Describing `%s.%s.%s` (%s partitions)" % (
            table.project, table.dataset, table.name, count
        ))
        if count > 0:
            summary.append("### Partition History")
            summary.append("**10 Latest Partitions:**")
            summary.append(ta.partition_info[
                ['table_id', 'creation_time', 'row_count', 'size_bytes']
            ].sort_values(
                'table_id', ascending=False).set_index('table_id').head(10)
            )
            # SLA calculations
            summary.append("** SLA statistics:**")
            summary.append(pandas.DataFrame((
                ta.partition_info.creation_time
                - ta.partition_info.start_partition.apply(
                    lambda x: arrow.get(x).datetime
                )
            ).agg(['min', 'mean', 'max']), columns=['SLA']))

            if ta.has_missing_partitions:
                summary.append("")
                summary.append("**It is missing some partitions:**")
                summary.append(ta.get_missing_partitions())

            schema_history = ta.schema_history
            if len(schema_history) > 0:
                summary.append("")
                summary.append("**It has incosistent schema over time:**")
                summary.append(schema_history)

        summary.append("")
        summary.append("### Column Statistics")
        col_stats = ta.get_column_statistics().iloc[0]
        latest_schema = ta.latest_schema
        stats_df = []
        for field in latest_schema:
            c = field.name
            stats = OrderedDict([
                ('Column', c),
                ('Type', field.field_type),
                ('Null Perc.', (
                    col_stats['%s_nulls' % c] / col_stats['row_count'])),
                ('Avg. Length (Arrays)', None),
                ('Cardinality (Strings)', None),
                ('Avg. (Numerics)', None),
                ('Min (Numerics)', None),
                ('Max (Numerics)', None),
                ('1st Quantile', None),
                ('2nd Quantile', None),
                ('3rd Quantile', None),
            ])

            if '%s_avglength' % c in col_stats:
                stats['Avg. Length (Arrays)'] = col_stats['%s_avglength' % c]
            if '%s_cardinality' % c in col_stats:
                stats['Cardinality (Strings)'] = (
                    col_stats['%s_cardinality' % c])
            if '%s_avg' % c in col_stats:
                stats['Avg. (Numerics)'] = col_stats['%s_avg' % c]
            if '%s_quantiles' % c in col_stats:
                stats['Min (Numerics)'] = col_stats['%s_quantiles' % c][0]
                stats['Max (Numerics)'] = col_stats['%s_quantiles' % c][4]
                stats['1st Quantile'] = col_stats['%s_quantiles' % c][1]
                stats['2nd Quantile'] = col_stats['%s_quantiles' % c][2]
                stats['3rd Quantile'] = col_stats['%s_quantiles' % c][3]
            stats_df.append(stats)
        summary.append(pandas.DataFrame(stats_df).set_index('Column'))

        dfs = []
        for s in summary:
            if isinstance(s, str):
                display(Markdown(s))
            else:
                dfs.append(s)
                display(s)

        return dfs

    @expose_as_api
    def describe_columns(self, table, dataset, project=None,
                         columns=None, measures='COUNT(*)',
                         from_latest_partition=True,
                         start_partition=None, end_partition=None,
                         print_stats=True):
        """Describe one or more columns based on one or more measures.

        This is useful for when you want to get a sense of values in a tables,
        i.e. what are possible combinations, and what's the distribution.

        Args:
            table (string): name of the table
            dataset (string): dataset table belongs to
            project (string): project table belongs to, if not provided
                defaults to BqT project
            columns (string or list(string)): list of (or single) columns
                to describe
            measures (string or list(string)): list of (or single) metrics
                to use for the description
            from_latest_partition (bool): whether to query all partitions
                or just the latest. if True, start_partition and end_partition
                are ignored
            start_partition (date like): when to start querying stats from
            end_partition (date like): when to end querying stats
            print_stats (bool): if True will also print statistics
        Returns:
            DataFrame with statistics based on `columns` and `measures`
        """
        if not columns or not measures:
            raise RuntimeError("both `columns` and `measures` are required.")

        if isinstance(columns, str):
            columns = [columns]
        if isinstance(measures, str):
            measures = [measures]

        table = BqTable(
            table, start_partition, end_partition, dataset,
            project or self.bqt_obj.project
        )
        ta = TableAnalyzer(table, self.bqt_obj)
        res = ta.get_column_stats(columns, measures)
        if print_stats:
            stats = res.groupby(columns).mean()
            display(stats.sort_values(
                stats.columns[0], ascending=False).reset_index())
        return res

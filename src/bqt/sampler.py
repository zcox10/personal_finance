from bqt.lib.decorators import expose_as_api
from bqt.lib.table import BqTable


class Sampler(object):
    """Class to provide a set of sampling tools
    """

    def __init__(self, bqt_obj):
        self.bqt_obj = bqt_obj

    @expose_as_api
    def sample_table_random_async(self, table, dataset, project=None,
                                  partition_start=None, partition_end=None,
                                  ratio=None, num_rows=None,
                                  ignore_exists=True,
                                  dest_table=None, dest_dataset=None,
                                  dest_project=None):
        """Basic random sample from a table [asynchronous]

        Note: only one of ratio and num_rows must be provided
        Note: if `dest_table` is not provided, the result will be stored in a
            table called `{table}_sample`

        Args:
            table (string): table name, if sampling across partitions is
                required the name mus have the partition format, e.g:
                    my_table_YYYYMMDD
            dataset (string): source dataset
            project (string): source project
            partition_start (date like): start of partition (inclusive) to
                sample from, only needed for partitioned tables
            partition_end (date like): end of partition (inclusive) to
                sample from, only needed for partitioned tables
            ratio (float): number in (0, 1) the ratio of sampled data to
                original
            num_rows (int): number of rows needed in final sample
            ignore_exists (bool): whether to skip existing samples or not
                if False the table will be recreated.
            dest_table (string): destination table for the sample. if `table`
                is partitioned, this name must also include the same partition
                format
            dest_dataset (string): destination dataset
            dest_project (string): destination project
        """
        if not ratio and not num_rows:
            raise RuntimeError("One of ratio or size must be provided")
        elif ratio and num_rows:
            raise RuntimeError("Only one of ratio and size must be provided")

        project = project or self.bqt_obj.project
        dest_dataset = dest_dataset or dataset
        dest_project = dest_project or project
        tables = BqTable(
            table, start=partition_start, end=partition_end,
            dataset=dataset, project=project
        )
        jobs = []
        for table in tables:
            sample_table_name = self._get_new_table_name(
                tables, table, dest_table
            )

            if num_rows:
                ratio = num_rows / float(table.num_rows)
            query = """
            SELECT * FROM `{table}` WHERE RAND() <= {ratio}
            """.format(table=repr(table), ratio=ratio)

            jobs.append(self.bqt_obj.create_table_async(
                query, dest_dataset, sample_table_name, project=dest_project,
                write_disposition='WRITE_TRUNCATE', ignore_exists=ignore_exists
            ))

        return jobs

    def _get_new_table_name(self, tables, table, dest_table):
        if tables.partition_format:
            if dest_table:
                sample_table_name = dest_table[:-len(tables.partition_format)]
                sample_table_name += str(table)[-len(tables.partition_format):]
            else:
                sample_table_name = '{table}sample_{partition}'.format(
                    table=tables.name_without_partition,
                    partition=str(table)[-len(tables.partition_format):]
                )
        else:
            sample_table_name = dest_table or (str(table) + '_sample')

        return sample_table_name

    @expose_as_api
    def sample_table_random(self, table, dataset, project=None,
                            partition_start=None, partition_end=None,
                            ratio=None, num_rows=None,
                            ignore_exists=True, dest_table=None,
                            dest_dataset=None, dest_project=None):
        """Basic random sample from a table

        Note: only one of ratio and num_rows must be provided
        Note: if `dest_table` is not provided, the result will be stored in a
            table called `{table}_sample`

        Args:
            table (string): table name, if sampling across partitions is
                required the name mus have the partition format, e.g:
                    my_table_YYYYMMDD
            dataset (string): source dataset
            project (string): source project
            partition_start (date like): start of partition (inclusive) to
                sample from, only needed for partitioned tables
            partition_end (date like): end of partition (inclusive) to
                sample from, only needed for partitioned tables
            ratio (float): number in (0, 1) the ratio of sampled data to
                original
            num_rows (int): number of rows needed in final sample
            ignore_exists (bool): whether to skip existing samples or not
                if False the table will be recreated.
            dest_table (string): destination table for the sample. if `table`
                is partitioned, this name must also include the same partition
                format
            dest_dataset (string): destination dataset
            dest_project (string): destination project
        """
        self.sample_table_random_async(
            table, dataset, project=project,
            partition_start=partition_start, partition_end=partition_end,
            ratio=ratio, num_rows=num_rows, ignore_exists=ignore_exists,
            dest_table=dest_table, dest_dataset=dest_dataset,
            dest_project=dest_project
        )
        self.bqt_obj.wait_for_all_jobs()

    @expose_as_api
    def sample_query_random(self, query, ratio=None, num_rows=None,
                            ignore_exists=True, dest_table=None,
                            dest_dataset=None, dest_project=None):
        """Basic random sample from a query

        Note: only one of ratio and num_rows must be provided

        Args:
            query (string): query the results of which is sampled
            ratio (float): number in (0, 1) the ratio of sampled data to
                original
            num_rows (int): number of rows needed in final sample
            ignore_exists (bool): whether to skip existing samples or not
                if False the table will be recreated.
            dest_table (string): destination table for the sample. if `table`
                is partitioned, this name must also include the same partition
                format
            dest_dataset (string): destination dataset
            dest_project (string): destination project
        """
        job = self.bqt_obj.query_async(query)
        self.bqt_obj.wait_for_job(job)
        ref = job.job.destination
        self.sample_table_random(
            ref.table_id, ref.dataset_id, project=ref.project,
            ratio=ratio, num_rows=num_rows, ignore_exists=ignore_exists,
            dest_table=dest_table, dest_dataset=dest_dataset,
            dest_project=dest_project
        )

    @expose_as_api
    def sample_table_stable_random_async(
            self, table, dataset, project=None,
            partition_start=None, partition_end=None,
            ratio=None, num_rows=None, ignore_exists=True,
            dest_table=None, dest_dataset=None, dest_project=None,
            columns=[]):
        """Stable random sampling from a table [asynchronous]

        Stable here means that a set identified by `columns` will either always
        be get included or excluded. For example if `columns='user_id'` and a
        row with user_id=1234 is selected in the sample, all other rows for
        that user will also be included.

        Note: only one of ratio and num_rows must be provided
        Note: if `dest_table` is not provided, the result will be stored in a
            table called `{table}_sample`

        Args:
            table (string): table name, if sampling across partitions is
                required the name mus have the partition format, e.g:
                    my_table_YYYYMMDD
            dataset (string): source dataset
            project (string): source project
            partition_start (date like): start of partition (inclusive) to
                sample from, only needed for partitioned tables
            partition_end (date like): end of partition (inclusive) to
                sample from, only needed for partitioned tables
            ratio (float): number in (0, 1) the ratio of sampled data to
                original
            num_rows (int): number of rows needed in final sample
            ignore_exists (bool): whether to skip existing samples or not
                if False the table will be recreated.
            dest_table (string): destination table for the sample. if `table`
                is partitioned, this name must also include the same partition
                format
            dest_dataset (string): destination dataset
            dest_project (string): destination project
            columns (string or list): one or more columns that should be stable
                over all rows and partitions
        """
        if not ratio and not num_rows:
            raise RuntimeError("One of ratio or size must be provided")
        elif ratio and num_rows:
            raise RuntimeError("Only one of ratio and size must be provided")
        if not columns:
            raise RuntimeError("You must provide `columns` to sample based on")
        if isinstance(columns, (str, u"".__class__)):
            columns = [columns]

        _hash = (
            'CONCAT(' +
            ','.join(['CAST(%s AS STRING)' % c for c in columns]) +
            ')'
        )

        project = project or self.bqt_obj.project
        dest_dataset = dest_dataset or dataset
        dest_project = dest_project or project
        tables = BqTable(
            table, start=partition_start, end=partition_end,
            dataset=dataset, project=project
        )
        jobs = []
        for table in tables:
            sample_table_name = self._get_new_table_name(
                tables, table, dest_table
            )

            if num_rows:
                ratio = num_rows / float(table.num_rows)

            query = """
            SELECT * FROM `{table}`
            WHERE MOD(ABS(FARM_FINGERPRINT({hash})), 1000000) <= {permil}
            """.format(table=repr(table), permil=ratio * 1000000, hash=_hash)
            print(query)
            jobs.append(self.bqt_obj.create_table_async(
                query, dest_dataset, sample_table_name, project=dest_project,
                write_disposition='WRITE_TRUNCATE', ignore_exists=ignore_exists
            ))

        return jobs

    @expose_as_api
    def sample_table_stable_random(
            self, table, dataset, project=None,
            partition_start=None, partition_end=None,
            ratio=None, num_rows=None, ignore_exists=True,
            dest_table=None, dest_dataset=None, dest_project=None,
            columns=[]):
        """Stable random sampling from a table

        Stable here means that a set identified by `columns` will either always
        be get included or excluded. For example if `columns='user_id'` and a
        row with user_id=1234 is selected in the sample, all other rows for
        that user will also be included.

        Note: only one of ratio and num_rows must be provided
        Note: if `dest_table` is not provided, the result will be stored in a
            table called `{table}_sample`

        Args:
            table (string): table name, if sampling across partitions is
                required the name mus have the partition format, e.g:
                    my_table_YYYYMMDD
            dataset (string): source dataset
            project (string): source project
            partition_start (date like): start of partition (inclusive) to
                sample from, only needed for partitioned tables
            partition_end (date like): end of partition (inclusive) to
                sample from, only needed for partitioned tables
            ratio (float): number in (0, 1) the ratio of sampled data to
                original
            num_rows (int): number of rows needed in final sample
            ignore_exists (bool): whether to skip existing samples or not
                if False the table will be recreated.
            dest_table (string): destination table for the sample. if `table`
                is partitioned, this name must also include the same partition
                format
            dest_dataset (string): destination dataset
            dest_project (string): destination project
            columns (string or list): one or more columns that should be stable
                over all rows and partitions
        """
        self.sample_table_stable_random_async(
            table, dataset, project=project,
            partition_start=partition_start, partition_end=partition_end,
            ratio=ratio, num_rows=num_rows, ignore_exists=ignore_exists,
            dest_table=dest_table, dest_dataset=dest_dataset,
            dest_project=dest_project,
            columns=columns
        )
        self.bqt_obj.wait_for_all_jobs()

    @expose_as_api
    def sample_query_stable_random(self, query, ratio=None, num_rows=None,
                                   ignore_exists=True, dest_table=None,
                                   dest_dataset=None, dest_project=None,
                                   columns=[]):
        """Stable random sampling from a query

        Stable here means that a set identified by `columns` will either always
        be get included or excluded. For example if `columns='user_id'` and a
        row with user_id=1234 is selected in the sample, all other rows for
        that user will also be included.

        Note: only one of ratio and num_rows must be provided

        Args:
            query (string): query the results of which is sampled
            ratio (float): number in (0, 1) the ratio of sampled data to
                original
            num_rows (int): number of rows needed in final sample
            ignore_exists (bool): whether to skip existing samples or not
                if False the table will be recreated.
            dest_table (string): destination table for the sample. if `table`
                is partitioned, this name must also include the same partition
                format
            dest_dataset (string): destination dataset
            dest_project (string): destination project
            columns (string or list): one or more columns that should be stable
                over all rows and partitions
        """
        job = self.bqt_obj.query_async(query)
        self.bqt_obj.wait_for_job(job)
        ref = job.job.destination
        self.sample_table_stable_random(
            ref.table_id, ref.dataset_id, project=ref.project,
            ratio=ratio, num_rows=num_rows, ignore_exists=ignore_exists,
            dest_table=dest_table, dest_dataset=dest_dataset,
            dest_project=dest_project, columns=columns
        )

    @expose_as_api
    def sample_table_stratified_async(
            self, table, dataset, project=None,
            partition_start=None, partition_end=None,
            ratio=None, num_rows=None, ignore_exists=True,
            dest_table=None, dest_dataset=None, dest_project=None,
            strata=[]):
        """Stratified sampling of a table [asynchronous]

        Stratified sampling will keep proportions of each subgroup (strata)
        the same. useful when groups are unbalanced.

        Note: if subgroups are too small to sample the required amount the
            final sampling will be less than `ratio` or `num_rows`. This
            implementation does not sample will replacement.
        Note: only one of ratio and num_rows must be provided
        Note: if `dest_table` is not provided, the result will be stored in a
            table called `{table}_sample`

        Args:
            table (string): table name, if sampling across partitions is
                required the name mus have the partition format, e.g:
                    my_table_YYYYMMDD
            dataset (string): source dataset
            project (string): source project
            partition_start (date like): start of partition (inclusive) to
                sample from, only needed for partitioned tables
            partition_end (date like): end of partition (inclusive) to
                sample from, only needed for partitioned tables
            ratio (float): number in (0, 1) the ratio of sampled data to
                original
            num_rows (int): number of rows needed in final sample
            ignore_exists (bool): whether to skip existing samples or not
                if False the table will be recreated.
            dest_table (string): destination table for the sample. if `table`
                is partitioned, this name must also include the same partition
                format
            dest_dataset (string): destination dataset
            dest_project (string): destination project
            strata (string or list): one or more columns that should be used
                to define the groups
        """
        if not ratio and not num_rows:
            raise RuntimeError("One of ratio or size must be provided")
        elif ratio and num_rows:
            raise RuntimeError("Only one of ratio and size must be provided")
        if not strata:
            raise RuntimeError("You must provide `strata` to sample based on")
        if isinstance(strata, (str, u"".__class__)):
            strata = [strata]

        query = """
        WITH sizes AS (
            SELECT {strata}, COUNT(*) AS strata_size
            FROM `{table}`
            GROUP BY {strata}
        ), total_strata AS (
            SELECT COUNT(*) AS num_strata
            FROM sizes
        )
        SELECT a.*
        FROM `{table}` a
            INNER JOIN sizes b ON {stupid_join}
            CROSS JOIN total_strata c
        WHERE RAND() * (strata_size / ({table_size} / num_strata)) <=
              {ratio} * {ratio_factor}
        """
        stupid_join = ' AND '.join([
            '(a.{c} = b.{c} OR (a.{c} IS NULL AND b.{c} IS NULL))'.format(c=c)
            for c in strata
        ])

        project = project or self.bqt_obj.project
        dest_dataset = dest_dataset or dataset
        dest_project = dest_project or project
        tables = BqTable(
            table, start=partition_start, end=partition_end,
            dataset=dataset, project=project
        )
        jobs = []
        ratio_factor = 'num_strata' if num_rows else '1'
        for table in tables:
            sample_table_name = self._get_new_table_name(
                tables, table, dest_table
            )
            if num_rows:
                ratio = num_rows / float(table.num_rows)

            table_query = query.format(
                table=repr(table), strata=', '.join(strata), ratio=ratio,
                table_size=table.num_rows, stupid_join=stupid_join,
                ratio_factor=ratio_factor
            )
            jobs.append(self.bqt_obj.create_table_async(
                table_query, dest_dataset, sample_table_name,
                project=dest_project,
                write_disposition='WRITE_TRUNCATE', ignore_exists=ignore_exists
            ))

        return jobs

    @expose_as_api
    def sample_table_stratified(
            self, table, dataset, project=None,
            partition_start=None, partition_end=None,
            ratio=None, num_rows=None, ignore_exists=True,
            dest_table=None, dest_dataset=None, dest_project=None,
            strata=[]):
        """Stratified sampling of a table

        Stratified sampling will keep proportions of each subgroup (strata)
        the same. useful when groups are unbalanced.

        Note: if subgroups are too small to sample the required amount the
            final sampling will be less than `ratio` or `num_rows`. This
            implementation does not sample will replacement.
        Note: only one of ratio and num_rows must be provided
        Note: if `dest_table` is not provided, the result will be stored in a
            table called `{table}_sample`

        Args:
            table (string): table name, if sampling across partitions is
                required the name mus have the partition format, e.g:
                    my_table_YYYYMMDD
            dataset (string): source dataset
            project (string): source project
            partition_start (date like): start of partition (inclusive) to
                sample from, only needed for partitioned tables
            partition_end (date like): end of partition (inclusive) to
                sample from, only needed for partitioned tables
            ratio (float): number in (0, 1) the ratio of sampled data to
                original
            num_rows (int): number of rows needed in final sample
            ignore_exists (bool): whether to skip existing samples or not
                if False the table will be recreated.
            dest_table (string): destination table for the sample. if `table`
                is partitioned, this name must also include the same partition
                format
            dest_dataset (string): destination dataset
            dest_project (string): destination project
            strata (string or list): one or more columns that should be used
                to define the groups
        """
        self.sample_table_stratified_async(
            table, dataset, project=project,
            partition_start=partition_start, partition_end=partition_end,
            ratio=ratio, num_rows=num_rows, ignore_exists=ignore_exists,
            dest_table=dest_table, dest_dataset=dest_dataset,
            dest_project=dest_project,
            strata=strata
        )
        self.bqt_obj.wait_for_all_jobs()

    @expose_as_api
    def sample_query_stratified(self, query, ratio=None, num_rows=None,
                                ignore_exists=True, dest_table=None,
                                dest_dataset=None, dest_project=None,
                                strata=[]):
        """Stratified sampling of a query

        Stratified sampling will keep proportions of each subgroup (strata)
        the same. useful when groups are unbalanced.

        Note: if subgroups are too small to sample the required amount the
            final sampling will be less than `ratio` or `num_rows`. This
            implementation does not sample will replacement.
        Note: only one of ratio and num_rows must be provided

        Args:
            query (string): query the results of which is sampled
            ratio (float): number in (0, 1) the ratio of sampled data to
                original
            num_rows (int): number of rows needed in final sample
            ignore_exists (bool): whether to skip existing samples or not
                if False the table will be recreated.
            dest_table (string): destination table for the sample. if `table`
                is partitioned, this name must also include the same partition
                format
            dest_dataset (string): destination dataset
            dest_project (string): destination project
            strata (string or list): one or more columns that should be used
                to define the groups
        """
        job = self.bqt_obj.query_async(query)
        self.bqt_obj.wait_for_job(job)
        ref = job.job.destination
        self.sample_table_stratified(
            ref.table_id, ref.dataset_id, project=ref.project,
            ratio=ratio, num_rows=num_rows, ignore_exists=ignore_exists,
            dest_table=dest_table, dest_dataset=dest_dataset,
            dest_project=dest_project, strata=strata
        )

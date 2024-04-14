import arrow
from google.cloud.exceptions import NotFound

from bqt.lib.bqt_instance_manager import BqtInstanceManager
from bqt.lib.connection import BqConnection


class BqTable(object):
    """This class represents a table, a table can be:
        1. a single table or single partition of a partitioned table
        2. a partitioned table range, with start and end ranges
        3. a partitioned table and all it's partitions

    It's an iterator in all the above cases, so all function in the library
    can always do this:

    for table in bq_table_obj:
        # do something with the table
    """

    partition_formats = {
        'YYYYMMDD': 'days',
        'YYYYMM': 'months',
        'YYYYMMDDHH': 'hours',
        'YYYYMMDDTHH': 'hours'
    }

    def __init__(self, name, start=None, end=None, dataset=None, project=None):
        """Initializer,

        Possible input combinations are:
            1. name: BqTable, clone the object, this is useful to be able to
                safely wrap any object around BqTable
            2. name: string without partition, start and end are None
                e.g. my_table
                -> single table
            3. name: string with partition, start and end are not None
                e.g. my_table_YYYYMMDD
                -> table range with defined bounds
            4. name: string with partition, start and end are None
                e.g. my_table_YYYYMMDD
                -> table range with all partitions

        dataset, project:
            if given, name is just the table name, if not given will into name
            for `.` separator (or `:` for legacy)

        Args:
            name (mixed): name of the table
            start (date like): starting partition, inclusive
            end (date like): ending partition, inclusive
            dataset (string): dataset name table belongs to
            project (string): project name table belongs to
        """
        if isinstance(name, BqTable):
            self.name = name.name
            start = name.start
            end = name.end
            self.dataset = name.dataset
            self.project = name.project
        elif isinstance(name, BqSingleTable):
            self.name = name.name
            self.dataset = name.dataset
            self.project = name.project
            start = end = None
        else:
            self.name = name
            self.dataset = dataset
            self.project = project

        # this just checks to see if both start and end are there not just one
        if ((1 if start else 0) + (1 if end else 0)) % 2 != 0:
            raise ValueError("Both start and end dates must be specified")

        self.start = arrow.get(start) if start else None
        self.end = arrow.get(end) if end else None
        if isinstance(name, BqTable):
            self._all_partitions = name._all_partitions
        else:
            self._all_partitions = None

        self.pos = 0

    def _init_iterator(self):
        """Initialize the iterator, which makes an API call to BQ to get the
        list of tables to iterate over
        """
        if self._all_partitions is not None:
            return
        if self.partition_format:  # some sort of range
            self._all_partitions = self._get_table_partitions_from_bq()
            if self.start:  # it's a specific range, filter
                self._all_partitions = [
                    x for x in self._all_partitions
                    if self._is_in_range(x)
                ]
        else:  # not a range, single table
            self._all_partitions = [self.name]
        self.pos = 0

    def _is_in_range(self, partition):
        """Is the given partition name inside the range specified for
        the object?
        """
        date_obj = arrow.get(
            partition[-len(self.partition_format):],
            self.partition_format
        )
        return date_obj >= self.start and date_obj <= self.end

    def _get_table_partitions_from_bq(self):
        """Return a list of table partitions from BQ"""
        query = """
        SELECT table_id
        FROM `{project}.{dataset}.__TABLES__`
        WHERE RTRIM(
            table_id,
            SPLIT(table_id, '_')[ORDINAL(ARRAY_LENGTH(SPLIT(table_id, '_')))]
        ) = '{table_prefix}'
        ORDER BY table_id
        """.format(
            project=self.project, dataset=self.dataset,
            table_prefix=self.name_without_partition
        )
        bqt_obj = BqtInstanceManager.get_iff_one_exists()
        if bqt_obj:
            client = bqt_obj.bq_client
        else:
            client = BqConnection.get_client(self.project)
        # this is an exception that we're not queuing the job
        # I think in general queries that are just for getting state
        # don't need to be queued for two reasons:
        #   1. They shouldn't count towards concurrency
        #   2. They should never be expensive queries
        query_results = client.query(query)
        return [
            r.table_id for r in query_results
            if self._check_suffix(r.table_id, self.partition_format)
        ]

    def _check_suffix(self, table_id, partition_format):
        """Checks to make sure `table_id` ends with the `partition_format` type
        suffix

        Args:
            table_id (str): full table name
            partition_format (str): valid partition format or None
        Returns:
            bool, True if suffix matches partition format or no partition
            format exists. False otherwise
        """
        if not partition_format:
            return True
        try:
            partition = table_id.split('_')[-1]
            arrow.get(partition, partition_format)
            return len(partition) == len(partition_format)
        except arrow.parser.ParserError:
            return False

    @property
    def partition_format(self):
        """Type of the partition, if any. e.g. YYYYMMDD"""
        for key in self.partition_formats:
            if self.name[-len(key):] == key:
                return key
        return None

    @property
    def name_without_partition(self):
        """Return the name without the partition part.

        e.g. my_table_YYYYMMDD -> my_table_
        """
        if self.partition_format:
            return self.name[:-len(self.partition_format)]
        else:
            return self.name

    @property
    def table_count(self):
        self._init_iterator()
        return len(self._all_partitions)

    def __len__(self):
        return self.table_count

    def __iter__(self):
        self._init_iterator()
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if len(self._all_partitions) == self.pos:
            self.pos = 0
            raise StopIteration
        self.pos += 1
        return BqSingleTable(
            self.project, self.dataset,
            self._all_partitions[self.pos - 1]
        )

    def __repr__(self):
        """Human readable interpretation of the table(s)"""
        if self.start:
            return "<Table:%s from:%s to:%s>" % (
                self.name,
                self.start.format(self.partition_format),
                self.end.format(self.partition_format)
            )
        return "<Table:%s>" % self.name

    @classmethod
    def from_full_name(cls, full_name, start=None, end=None):
        """Parses a complete path to table and return it as BQTable

        Args:
            full_name (string): name in format `project.dataset.table`
            start (date like): start date if table is partitioned
            end (date like): end date if table is partitioned
            full_range (bool): if table is partitioned find start and end from
                bigquery so that it covers the full table range
        Returns:
            tuple(project name, dataset name, BQTable instance)
        """
        project, dataset, table = cls.break_full_name(full_name)
        return cls(
            table, start=start, end=end,
            project=project, dataset=dataset
        )

    @classmethod
    def create_new_range(cls, name, dataset, project, start, end):
        """Create a new BqTable object that represents a non-existent range
        of tables. This is useful for when you need to create an object to then
        create tables for which is not possible with the constructor

        Args:
            name (string): table name
            dataset (string): dataset table belongs to
            project (string): project table belongs to
            start (date like): start of the partition
            end (date like): end of the partition
        Returns
            BqTable
        """
        bq_table = cls(
            name, dataset=dataset, project=project, start=start, end=end
        )
        bq_table._all_partitions = [
            bq_table.name_without_partition +
            bq_table.start.shift(days=i).format(bq_table.partition_format)
            for i in range((bq_table.end - bq_table.start).days + 1)
        ]
        bq_table.pos = 0
        return bq_table

    @classmethod
    def clone_with_new_name(cls, bq_table, new_name, new_dataset, new_project):
        """Clone an existing BqTable object but change the URI

        Args:
            bq_table (BqTable): object to clone
            new_name (string): new table name
            new_dataset (string): new dataset name
            new_project (string): new project name
        Returns:
            BqTable with new URI
        """
        new_bq = cls(
            new_name, dataset=new_dataset, project=new_project,
            start=bq_table.start, end=bq_table.end
        )
        partition_format = bq_table.partition_format
        if not partition_format:
            new_bq._all_partitions = [new_name]
        else:
            name_without_partition = new_bq.name_without_partition
            new_bq._all_partitions = [
                name_without_partition + table.name[-len(partition_format):]
                for table in bq_table
            ]
        new_bq.pos = 0
        return new_bq

    @staticmethod
    def break_full_name(full_name):
        """turns a full table path to it's parts"""
        try:
            if ':' in full_name:
                project, rest = full_name.split(':')
                dataset, table = rest.split('.')
            else:
                project, dataset, table = full_name.split('.')
            return project, dataset, table
        except Exception:
            raise ValueError("`%s` is not a valid BQ table path")

    @classmethod
    def find_partition_type(cls, table_name):
        """Find the type of partitioning on a table, accepts any of:
            path.to.table_YYYYMMDD
            table_YYYYMMDD
            path.to.table_20181213
            table_20181213

        Returns:
            tuple(
                [0] partition type, in format YYYYMMDD or none if detection
                    fails
                [1] table_name without the partition part or table_name if
                    detection fails
            )
        """
        partition = table_name.split('_')[-1]

        if len(partition) > 11:
            return None, table_name
        if partition in cls.partition_formats.keys():
            return partition, '_'.join(table_name.split('_')[:-1]) + '_'

        has_T = 'T' in partition
        partition = partition.replace('T', '')

        try:
            year = int(partition[0:4])
            if year < 1995 or year > 2035:
                return None, table_name

            month = int(partition[4:6])
            if month < 0 or month > 12:
                return None, table_name
            month = partition[4:6]

            day = None
            if len(partition) >= 8:
                day = int(partition[6:8])
                if day < 1 or day > 31:
                    return None, table_name
                day = partition[6:8]

            hour = None
            if len(partition) == 10:
                hour = int(partition[8:10])
                if hour < 0 or hour > 23:
                    return None, table_name
                hour = partition[8:10]

            # this validates the combination
            arrow.get('%s-%s-%sT%s:00:00' % (
                year, month, day or '01', hour or '00'
            ))
            prefix = '_'.join(table_name.split('_')[:-1]) + '_'
            if hour:
                return (
                    'YYYYMMDDHH' if not has_T else 'YYYYMMDDTHH'
                ), prefix
            elif day:
                return 'YYYYMMDD', prefix
            return 'YYYYMM', prefix
        except ValueError:
            return None, table_name

    @classmethod
    def get_latest_partition(cls, table_prefix, dataset, project, client=None):
        """Return a list of table partitions from BQ"""
        query = """
        SELECT table_id
        FROM `{project}.{dataset}.__TABLES__`
        WHERE RTRIM(
            table_id,
            SPLIT(table_id, '_')[ORDINAL(ARRAY_LENGTH(SPLIT(table_id, '_')))]
        ) = '{table_prefix}'
        ORDER BY table_id DESC
        LIMIT 1
        """.format(
            project=project, dataset=dataset, table_prefix=table_prefix
        )
        bqt_obj = BqtInstanceManager.get_iff_one_exists()
        if not client and bqt_obj:
            client = bqt_obj.bq_client
        elif not client:
            client = BqConnection.get_client(project)
        # this is an exception that we're not queuing the job
        # I think in general queries that are just for getting state
        # don't need to be queued for two reasons:
        #   1. They shouldn't count towards concurrency
        #   2. They should never be expensive queries
        latest_table = [r.table_id for r in client.query(query)]
        if not latest_table:
            return None
        return BqSingleTable(
            project, dataset, latest_table[0], client=client
        )


class BqSingleTable(object):
    """Represents a single BQ table"""
    def __init__(self, project, dataset, table, client=None):
        self.project = project
        self.dataset = dataset
        self.name = table

        self.client = client or BqConnection.get_client(project)
        self.dataset_obj = BqConnection.get_dataset(
            dataset, client=self.client)
        self.table_ref = self.dataset_obj.table(table)

        self._table_obj = None

    @property
    def table_obj(self):
        if self._table_obj is None:
            self._table_obj = self.client.get_table(self.table_ref)
        return self._table_obj

    @property
    def exists(self):
        """Does this table exist?"""
        try:
            self._table_obj = None  # force it to refresh
            self.table_obj
            return True
        except NotFound:
            return False

    @property
    def age(self):
        """Time in seconds since the table creation"""
        try:
            self._table_obj = None  # force it to refresh
            return (
                arrow.get() - arrow.get(self.table_obj.created)
            ).total_seconds()
        except NotFound:
            return None

    @property
    def num_rows(self):
        return self.table_obj.num_rows

    @property
    def columns(self):
        return [f.name for f in self.table_obj.schema]

    def __str__(self):
        return self.name

    def __repr__(self):
        return '%s.%s.%s' % (self.project, self.dataset, self.name)

    @property
    def schema_raw(self):
        """Lis of BQ SchemaFields representing the table schema.
        Not typically needed by end user.

        Example:
            [SchemaField(u'genre_gid', u'STRING', u'REQUIRED',
                         u'Genre GID in hexadecimal string', ()),
             SchemaField(u'date', u'STRING', u'REQUIRED',
                         u'Date of entity creation', ())]
        """
        return self.table_obj.schema

    @property
    def description(self):
        """Description of the table in metadata (string)
        """
        return self.table_obj.description

    @property
    def schema(self):
        """BQ schema returned as a list of dicts (which can in turn be saved as
        json if preferred for saving to a file)

        Example:
            [{'description': u'Genre GID in hexadecimal string',
              'mode': u'REQUIRED',
              'name': u'genre_gid',
              'type': u'STRING'},
             {'description': u'Date of entity creation',
              'mode': u'REQUIRED',
              'name': u'date',
              'type': u'STRING'}]
        """
        return [f.to_api_repr() for f in self.table_obj.schema]

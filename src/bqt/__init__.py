from bqt.lib.connection import BqConnection
from bqt.lib.print_tools import PrintTools
from bqt.lib.bqt_instance_manager import BqtInstanceManager
from bqt.lib.utils import resolve_query_to_string, read_return_query
from bqt.alter_table import AlterTable
from bqt.job_history import JobHistory
from bqt.parameter_manager import ParameterManager
from bqt.query_analyzer import QueryAnalyzer
from bqt.lib.query import Query
from bqt.reader import Reader, LocalCache
from bqt.sampler import Sampler
from bqt.stats import Stats
from bqt.writer import Writer
from bqt.lib.project_analyzer import ProjectAnalyzer

import collections
import copy
from google.cloud.exceptions import NotFound
from random import random
import tempfile
import warnings
from google.cloud import storage

# more info on this here:
# https://github.com/googleapis/google-auth-library-python/issues/271
warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials"
)

# Set package information
__author__ = 'Behrooz Afghahi'
__license__ = 'Apache 2.0'
__email__ = 'behrooza@spotify.com'
__description__ = 'BigQuery Utilities'
__uri__ = 'https://ghe.spotify.net/science-box/bqt'
# having issues with doing this dynamically, for now need to remember to bump
# this :D
__version__ = '1.1.1'


class BadConfigException(Exception):
    pass


class BqT(object):
    """This is the main entry point to the library and exposes all functionality

    The intention is that all classes will have their functionality exposed
    here as methods to make it easier for end users to use this tool.

    This is done automatically through the decorator `expose_as_api`. and then
    in this class we would automatically call the exposed method
    """

    # cause we suggest importing it as `from bqt import bqt`
    # this will make the attributes available regardless
    __author__ = __author__
    __license__ = __license__
    __email__ = __email__
    __description__ = __description__
    __uri__ = __uri__
    __version__ = __version__

    # Some useful time constants (all in seconds)
    MONTH = 2592000
    WEEK = 604800
    DAY = 86400
    HOUR = 3600
    MINUTE = 60

    # default configuration options, also acts as config template
    # NOTE: do not use `.` in the config name, it'll break things cause we use
    #       dot as a path seperator for setting/getting values
    _default_config = {
        'cache': {
            # time to live of the cached results in seconds, -1 = never expire
            'ttl': -1,
            'bq': {
                'table_prefix': 'bqt_cache_',
                'project': None,
                'dataset': None
            },
            'local': {
                'temp_dir': tempfile.gettempdir()
            }
        },
        'verbose': True,
        # BQ Queries costs $5 per TB. First 1 TB per month is free
        # see here for more: https://cloud.google.com/bigquery/pricing
        'bq_price': 5. / 2 ** 40,  # USD/Byte
        'default_location': 'EU',
        'fastbqt': {
            'temp_dir': tempfile.gettempdir(),
            'staging_bucket': 'fastbqt-staging-{}-{}',
            'staging_bucket_deletion_age': 1,
            'fastquery_blob_name': 'fastquery_{}_{}',
            'fastquery_blob_retries': 10,
            'fastload_blob_name': 'fastload_{}_{}',
            'fastload_chunk_size': 1000000
        },
        # chunk size if query() asks for a generator
        'generator_row_size': 10000,
        'bq_insight': {
            'parallelism': 20,
            'fuzzy_matching_threshold': 70,
            'table_annotation_keywords': 'policy.accessTier',
            'field_annotation_keywords': 'policy.semanticType',
            'semantic_types':
                ['_age_', '_gender_', 'noUsernameURI',
                 'personalDataURI', 'personalName', 'email', '_city_',
                 'birthday', 'deviceId', 'deviceName', 'partnerId',
                 'socialMediaUserId', 'playlistName', 'anomaly', 'employee',
                 'internalUniqueId', 'username', 'freeTextField',
                 'sensitiveContent', 'phoneNumber', 'voice', 'picture',
                 'geolocation', 'postalCode', 'fullAddress', 'ipAddress',
                 'paymentInfo', 'criminalRecord', 'fraudRecord', 'healthInfo',
                 '__religion__', 'ethnicity', 'politicalOpinion',
                 'sexualOrientation', 'income', 'personality', '_ssn_',
                 'cryptoKey', 'password', 'externalId', 'advertisingId']
        }
    }

    # list of parameters from this class to exclude from dir(bqt) calls
    _private_attr = [
        'search_for_api', '_update_config', '_default_config', '_private_attr'
    ]

    def __init__(self, project=None, location=None, config=None):
        """Initialize the BqT object, this is the entry point for all API calls

        Args:
            project (string): project name to use for running jobs and billing
            location (string): default location of datasets, etc.
            config (dict): extra configurations, optional.
                @see BqT._default_config for all options
        """
        self._project = project
        self._location = location

        self._custom_scope = None
        self._bq_client = None
        self._gcs_client = None

        self.config = copy.deepcopy(self._default_config)
        self._update_config(self.config, config or {})
        if self._project:
            self._test_config()
        PrintTools.mute = not self.verbose

        self.reader = Reader(self)
        self.writer = Writer(self)
        self.job_history = JobHistory(self)
        self.parameter_manager = ParameterManager(self)
        self.stats = Stats(self)
        self.sampler = Sampler(self)
        self.project_analyzer = ProjectAnalyzer(self)

        # Adding something here makes it searchable as an API method on this
        # class
        self.search_for_api = [
            self.reader, self.writer, self.job_history, self.parameter_manager,
            self.stats, self.sampler, self.project_analyzer
        ]

        self._run_cleanups()
        BqtInstanceManager.add(self)  # add this object to active object

    def __del__(self):
        BqtInstanceManager.delete(self)  # remove this object from active ones

    @property
    def version(self):
        return self.__version__

    @property
    def project(self):
        if self._project:
            return self._project

        self._project = BqConnection.get_default_project()
        self._test_config()
        return self._project

    @property
    def location(self):
        return self._location

    @property
    def custom_scope(self):
        return self._custom_scope

    def change_project(self, new_project):
        """Change the billing and default project for this BqT object

        Args:
            new_project (string): new BigQuery project
        """
        self._project = new_project
        self._bq_client = None  # initialize on next use

    def change_location(self, new_location):
        """Change the default location of all new tables, etc. to this location

        Args:
            new_location (string): new BigQuery default location
        """
        self._location = new_location
        self._bq_client = None  # initialize on next use

    def change_custom_scope(self, new_custom_scope):
        """Add non-BQ custom scope(s) (e.g., Google Drive) to authenticate with."

        Args:
            new_custom_scope (list): list containing services to include
        """
        self._custom_scope = new_custom_scope
        self._bq_client = None
        BqConnection.pop_client(self.project)

    @property
    def bq_client(self):
        if self._bq_client is None:
            self._bq_client = BqConnection.get_client(
                self.project, location=self.location, custom_scope=self.custom_scope
            )
        return self._bq_client

    @property
    def gcs_client(self):
        if self._gcs_client is None:
            self._gcs_client = storage.Client(self.project)
        return self._gcs_client

    def _update_config(self, base, user):
        for k in user:
            v = user[k]
            if isinstance(v, collections.Mapping):
                base[k] = self._update_config(base.get(k, {}), v)
            else:
                base[k] = v
        return base

    def get_config(self, path):
        """Return the config value at the given path.

        Example: to get cache time to live at path `cache.ttl`:
            bqt.get_config('cache.ttl')

        Args:
            path (string): path to config value
        Returns:
            mixed, value at the given path
        """
        _vars = path.split('.')
        conf = self.config
        path = []
        for var in _vars:
            path.append(var)
            if var not in conf:
                raise ValueError(
                    "Config path `%s` doesn't exist" % ".".join(path)
                )
            conf = conf[var]
        return conf

    def set_config(self, path, value=None):
        """Set the config value at the given path.

        Note: path can be a dictionary of configs, this is useful to set
              multiple values at the same time, the format then is:
              {path: value, path: value, ...}

        Example: to set cache time to live at path `cache.ttl`:
            bqt.set_config('cache.ttl', 1000)

        Example: to set both cache ttl and prefix:
            bqt.set_config({
                'cache.ttl': 1000,
                'cache.local.temp_dir': '.....'
            })

        Args:
            path (mixed): string to set a single config or dict(path->value)
            balue (mixed): value to set config to, only used when path is
                           a string
        """
        if not isinstance(path, dict):
            changes = {path: value}
        else:
            changes = path

        for change in changes:
            value = changes[change]
            _vars = change.split('.')
            conf = self.config
            sub_path = []
            for i, var in enumerate(_vars):
                sub_path.append(var)
                if var not in conf:
                    raise ValueError(
                        "Config path `%s` doesn't exist" % ".".join(sub_path)
                    )
                if i == len(_vars) - 1:
                    conf[var] = value
                else:
                    conf = conf[var]

    def _test_config(self):
        """These tests can be further down in the API too but having them here
        feels less confusing to users as they'd know something is wrong with
        their config as soon as they create the BqT object

        Raises:
            BadConfigException
        """
        # test to make sure the bq cache setup exists
        dataset = self.get_config('cache.bq.dataset')
        if dataset:
            cache_client = BqConnection.get_client(
                self.get_config('cache.bq.project') or self.bq_client.project,
                custom_scope=self.custom_scope
            )
            ds_object = BqConnection.get_dataset(dataset, client=cache_client)
            try:
                cache_client.get_dataset(ds_object)
            except NotFound:
                msg = "BQ cache dataset `%s.%s` doesn't exist!" % (
                    self.bq_client.project, dataset
                )
                self.print_msg(msg, _type='error', bold=True)
                raise BadConfigException(msg)

    def _run_cleanups(self):
        # cleanup old cached files for GDPR
        if random() < 0.25:  # static 25% chance of cleanup happening
            lcache = LocalCache(self, "")
            lcache.cleanup_old_caches_cause_gdpr()

    @property
    def verbose(self):
        return self.get_config('verbose')

    @verbose.setter
    def verbose(self, value):
        self.set_config('verbose', bool(value))
        PrintTools.mute = not bool(value)

    def print_progress(self, msg, _type=None, bold=False, underline=False):
        """Print a temporary [progress report] message

        This function should be used when the message is temporary since it
        will get replaced by the next message that is printed.

        Args:
            msg (string): message to print
            _type (string): type of message, indicates color, possible values:
                info, success, warn, error
            bold (bool): bold text or not
            underline (bool); underline text or not
        """
        PrintTools(msg, temp=True,
                   _type=PrintTools.TYPE_MAPPING.get(_type),
                   bold=bold, underline=underline)

    def print_msg(self, msg, _type=None, bold=False, underline=False):
        """Print a message

        Args:
            msg (string): message to print
            _type (string): type of message, indicates color, possible values:
                info, success, warn, error
            bold (bool): bold text or not
            underline (bool); underline text or not
        """
        PrintTools(msg, _type=PrintTools.TYPE_MAPPING.get(_type),
                   bold=bold, underline=underline)

    def __getattr__(self, name):
        """This exposes all methods created in pluging classes to the outside
        world.

        Note: only method decorated with @expose_as_api and listen in
            BqT.search_for_api are discoverable
        """
        for clss in self.search_for_api:
            if hasattr(clss, name):
                method = getattr(clss, name)
                if hasattr(method, 'exposed') and getattr(method, 'exposed'):
                    return method
        raise AttributeError(
            "Method `%s` was not found in any API class" % name)

    def __dir__(self):
        """Custom implementation of __dir__ to add methods discovered from
        plugin classes
        """
        first_level = [c for c in dir(BqT) if c not in self._private_attr]

        second_level = []
        for clss in self.search_for_api:
            for attr in dir(clss):
                method = getattr(clss, attr)
                if hasattr(method, 'exposed') and getattr(method, 'exposed'):
                    second_level.append(attr)

        return list(set(first_level) | set(second_level))

    def __getstate__(self):
        """Used for pickling, otherwise the __getattr__ breaks the process
        """
        return vars(self)

    def __setstate__(self, state):
        """Used for pickling, otherwise the __getattr__ breaks the process
        """
        vars(self).update(state)

    def alter_table(self, dataset, table, start=None, end=None):
        """Return an alter table object to be used for table alternation.

        @see AlterTable for more information

        Example:
            bqt = BqT(...)
            alterer = bqt.alter_table('dataset', 'table_YYYYMMDD')
            alterer.delete_column('bad_column')
            alterer.apply()

        Args:
            dataset (string): dataset of the table to be altered
            table (string): table name with or without partitions
            start, end (date like): [optional], if provided, limit the
                alteration to [start, end]
        Returns:
            AlterTable instance you can use to make changes to the table
        """
        return AlterTable(self, dataset, table, start=start, end=end)

    def query_analyzer(self, query):
        """Return an instance of the QueryAnalyzer

        WARNING: this functionality is very experimental and there are no
            guarantees of correctness

        This object can be used to get useful information about a query by
        directly parsin it. This method will not contact BigQuery.

        Args:
            query (string): query to analyze
        Return
            QueryAnalyzer object
        """
        return QueryAnalyzer(self, query)

    def create_staging_bucket(self, bucket_name=None, location=None):
        loc = (location or self.get_config("default_location")).lower()
        if bucket_name is None:
            bucket_name = self.get_config("fastbqt.staging_bucket") \
                .format(loc, self.project)
        bucket = storage.Bucket(client=self.gcs_client, name=bucket_name)
        if not bucket.exists():
            bucket.create(project=self.project, location=location)
            days = self.get_config("fastbqt.staging_bucket_deletion_age")
            bucket.add_lifecycle_delete_rule(age=days)
            bucket.update()
            PrintTools("Staging bucket gs://{} created".format(bucket_name),
                       _type=PrintTools.INFO)
        else:
            PrintTools("Staging bucket: gs://{}".format(bucket_name),
                       _type=PrintTools.INFO)
        return self.gcs_client.get_bucket(bucket)

    def parse_string(self, string,
                     _format=True, insert_params=True, load_file=True):
        """Parses a string in the usual way to:
            - load SQL if string is a file name
            - replaces all parameters

        Args:
            string (str): string to parse
            _format (bool): whether to format the SQL with spaces and tabs
            insert_params (bool): whether to replace params
            load_file (bool): whether to load as a file if one exists
        Returns:
            string altered with things bqt can do SQL queries and identifiers
        """
        if load_file:
            string = resolve_query_to_string(string)
        string, _ = Query.remove_and_report_standard(string)
        if insert_params:
            string = self.parameter_manager.insert_params(string)
        if _format:
            string = Query(string).format

        return string


# create a default bqt object for easier use
bqt = BqT()

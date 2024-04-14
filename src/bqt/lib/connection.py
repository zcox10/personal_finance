from collections import defaultdict
import os
import subprocess
import re

from google.cloud import _helpers
from google.cloud import bigquery

from bqt.lib.print_tools import PrintTools


class BqConnection(object):
    """Class that manages BQ connections

    This singleton class is shared betwee
    """

    DEFAULT_BQ_SCOPE = (
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform'
    )
    # holds a single instance of client connection per project
    _clients = {}

    # keeps all openned dataset connections so we don't have to recreate
    # them
    _datasets = defaultdict(dict)

    @classmethod
    def get_client(cls, project, location=None, custom_scope=None):
        """Get the client connection

        Args:
            project (string): project name to fetch
            location (string): default location of datasets, etc.
            custom_scope (list): Custom services to authorize bigquery.Client against

        Returns:
            bigquery.Client
        """
        if hasattr(bigquery.Client, 'SCOPE') and type(bigquery.Client.SCOPE) is tuple:
            if custom_scope:
                bigquery.Client.SCOPE = tuple(set(cls.DEFAULT_BQ_SCOPE).union(set(custom_scope)))
            else:
                bigquery.Client.SCOPE = cls.DEFAULT_BQ_SCOPE
        else:
            PrintTools(
                "Error getting bigquery.Client.SCOPE attribute, continuing without Google Drive support",
                _type=PrintTools.WARNING
            )
        if project in cls._clients and (
                location is None or location == cls._clients[project].location
        ):
            return cls._clients[project]

        client = bigquery.Client(project=project, location=location)
        cls._clients[project] = client
        return client

    @classmethod
    def pop_client(cls, project):
        """Pop (return and remove) the bigquery.Client for a given project

        Args:
            project (string): project name to fetch
        Returns:
            bigquery.Client (None if no client present for given project)
        """
        return cls._clients.pop(project, None)

    @classmethod
    def get_dataset(cls, dataset, project=None, client=None):
        """Get dataset object given the dataset and project name/object

        Note: only one of project and client needs to be provided

        Args:
            dataset (string): dataset name to fetch object for
            project (string): project name dataset belongs to
            client (bigquery.Client): BQ client object dataset belongs to
        Returns:
            biguqery.Dataset object
        """
        if not client and not project:
            raise RuntimeError("One of `client` or `project` must be provided")
        if not project:
            project = client.project

        if dataset in cls._datasets[project]:
            return cls._datasets[project][dataset]

        if not client:
            client = cls.get_client(project)
        dataset_obj = client.dataset(dataset)
        cls._datasets[project][dataset] = dataset_obj
        return dataset_obj

    @classmethod
    def get_default_project(cls):
        """Return the user's default GCP project"""
        project = _helpers._determine_default_project()
        if not project:
            project = os.environ.get('GBQ_PROJECT_ID')
        if not project:
            try:
                result = subprocess.check_output(
                    ['gcloud', 'config', 'list', 'project'],
                    stderr=subprocess.STDOUT
                ).decode('utf-8')
                try:
                    project = re.search(
                        r"project = (.*)", result
                    ).group(1).split("\n")[0]
                except TypeError:  # Python 3 reading it as bytes
                    project = re.search(
                        r"project = (.*)", result.decode('utf-8')
                    ).group(1).split("\n")[0]
            except AttributeError:
                pass
            except OSError:
                pass
        if not project or len(project) == 0:
            project = PrintTools.input(
                "You have no default BQ project set,"
                "what do you want to set it to? "
            )
            try:
                subprocess.call(
                    ['gcloud', 'config', 'set', 'project', project]
                )
            except OSError:
                PrintTools(
                    "Unable to set project. You may need to enter in a default"
                    "project again next time.\nTo avoid having to do this "
                    "again, try setting the env variable GBQ_PROJECT_ID "
                    "to a project.",
                    _type=PrintTools.WARNING
                )
        return project

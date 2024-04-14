import arrow
from google.cloud.exceptions import NotFound
import pandas

from bqt.lib.decorators import expose_as_api
from bqt.lib.print_tools import PrintTools
from bqt.lib.job import BqJobManager, BqJobResult


class JobHistory(object):
    """A class to fetch and work with Bq history, similar to the way it is
    presented in the UI but ability to access and fetch all the information
    in python
    """

    def __init__(self, bqt_obj):
        self.bqt_obj = bqt_obj

    @expose_as_api
    def list_running_jobs(self, max_results=100, all_users=False,
                          as_job_obj=False):
        """Return running jobs

        Args:
            max_results (int): maximum number of jobs to return
            all_users (bool): whether to return jobs running for all users
                or just the current user
            as_job_obj (bool): if True, will return job objects directly
        Return:
            as_job_obj = True
                list(google...job)
            as_job_obj = False
                pandas.DataFrame with job information
        """
        jobs = self.bqt_obj.bq_client.list_jobs(
            max_results=max_results, all_users=all_users,
            state_filter="running"
        )
        if as_job_obj:
            return jobs

        this_notebook = [j.job_id for j in BqJobManager.get_running_jobs()]
        job_info = []
        for job in jobs:
            dest = getattr(job, 'destination', 'n/a')
            if dest != 'n/a':
                dest = '%s.%s.%s' % (
                    dest.project, dest.dataset_id, dest.table_id
                )
            job_info.append({
                'job_id': getattr(job, 'job_id', 'n/a'),
                'user_email': getattr(job, 'user_email', 'n/a'),
                'query': getattr(job, 'query', 'n/a'),
                'destination': dest,
                'status': 'running',
                'write_disposition': getattr(job, 'write_disposition', 'n/a'),
                'created': getattr(job, 'created', 'n/a'),
                'elapsed_seconds': (
                    arrow.utcnow() -
                    arrow.get(getattr(job, 'created', arrow.utcnow()))
                ).total_seconds(),
                'elapsed': PrintTools.human_duration((
                    arrow.utcnow() -
                    arrow.get(getattr(job, 'created', arrow.utcnow()))
                ).total_seconds()),
                'started': getattr(job, 'started', 'n/a'),
                'ended': getattr(job, 'ended', 'n/a'),
                'this_notebook': getattr(job, 'job_id', 'n/a') in this_notebook
            })

        return pandas.DataFrame(job_info)

    @expose_as_api
    def cancel_job(self, job_id, block_till_done=False):
        """Cancels a job

        Args:
            job_id (string): job ID from BigQuery
        Returns:
            the job object
        """
        job = self.bqt_obj.bq_client.cancel_job(job_id)
        return BqJobResult(job, self.bqt_obj)

    @expose_as_api
    def add_job_by_id(self, job_id, poll_interval=20):
        """Adds a job to the Job Manage and returns it's wrapped results

        Args:
            job_id (string): job ID from BigQuery
            poll_interval (int): how long to wait between polls
        Returns:
            BqJobResult
        """
        try:
            job = self.bqt_obj.bq_client.get_job(job_id)
            job_exists = job.exists()
        except NotFound:
            job_exists = False
        if not job_exists:
            PrintTools(
                "Job with ID `%s` not found" % job_id, _type=PrintTools.ERROR
            )
            return
        job_obj = BqJobResult(job, self.bqt_obj)
        return BqJobManager.queue_job(job_obj)

    @expose_as_api
    def wait_for_all_jobs(self, halt_on_error=True, sleep=20):
        """Block (wait) until all jobs are finished"""
        BqJobManager.wait_for_jobs(halt_on_error=halt_on_error, sleep=sleep)
        self.bqt_obj.print_msg("All jobs completed", _type="info")

    @expose_as_api
    def wait_for_job(self, job_obj, halt_on_error=True, sleep=20):
        BqJobManager.wait_for_job(
            job_obj, halt_on_error=halt_on_error, sleep=sleep)

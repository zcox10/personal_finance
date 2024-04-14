import arrow
from google.cloud.exceptions import NotFound
import time
import pandas

try:
    import psutil
except ImportError:
    psutil = None

from bqt.lib.decorators import catch_bq_exceptions
from bqt.lib.print_tools import PrintTools
from bqt.lib.query import Query
import polars as pl


class BqJobDoesntExist(Exception):
    pass


class BqResultsTooBig(Exception):
    pass


class BqJobResult(object):

    STATE_FINISHED_SUCCESS = "finished"
    STATE_FINISHED_ERROR = "finished_with_errors"
    STATE_FINISHED = (STATE_FINISHED_SUCCESS, STATE_FINISHED_ERROR)
    STATE_RUNNING = "running"
    STATE_PENDING = "pending"

    def __init__(self, job, bqt_obj=None, results=None, result_format="pandas"):
        """
        Args:
            job (google.Job*): job class from google API
            bqt_obj (BqT): bqt object creating this job, must be provided.
                If ommited major functionality might not work
            results (pandas.DataFrame): job results if it's already available
                used mainly for cached jobs
        """
        if not job:
            raise ValueError("Can't create a job without a google.cloud.bigquery.job object")
        self.job = job
        self._on_results_callbacks = []
        self._results = results
        self.bqt_obj = bqt_obj
        self.result_format = result_format or "pandas"

        # silence the progress bar if PrintTools is muted since this is the only
        # place where we write to stdout without PrintTools
        self.progress_bar_type = None if PrintTools.mute else "tqdm"

    @classmethod
    def from_storage(cls, info, data, bqt_obj, result_format="pandas"):
        """Create a job from information retrieved from storage

        Args:
            info (dict): job information
            data (pandas.DataFrame): job results
            bqt_obj (BqT): bqt instance creating the job
            result_format (str): Format for the returned result. Options are:
                'pandas': pandas dataframe
                'polars': polars dataframe
        Returns:
            BqJobResult
        """
        try:
            job = bqt_obj.bq_client.get_job(info["job_id"])
            job_exists = job.exists()
        except NotFound:
            job_exists = False

        if not job_exists:
            raise BqJobDoesntExist
        return cls(job, bqt_obj, results=data, result_format=result_format)

    def register_on_results_callback(self, func):
        """Register a callback for when the results are ready

        Args:
            func (callable): func(result_df, BqJobResult) -> None
        """
        if func not in self._on_results_callbacks:
            self._on_results_callbacks.append(func)

    def has_results(self):
        return self.is_finished and not self.is_failed

    def has_drive_error(self):
        if self.get_errors():
            for err in self.get_errors():
                if err.get("message").find("Permission denied while getting Drive credentials") != -1:
                    return True
        return False

    def _wait_for_results(self):
        """Blocks until BQ is done running the query

        Returns:
            Whether the job should fetch results or not
        """
        state = self.state
        while state not in self.STATE_FINISHED:
            try:
                d = PrintTools.human_duration(self.seconds_since_created())
                if state == self.STATE_PENDING:
                    PrintTools(
                        "Your job is queued but not started yet, %s elapsed." % d, _type=PrintTools.INFO, temp=True
                    )
                else:
                    if self.seconds_since_started() < 10:
                        PrintTools("Running job %s:%s" % (self.project, self.job_id), _type=PrintTools.INFO)
                    PrintTools(
                        "%s elapsed, waiting for job to finish." % (d or "0 seconds"), _type=PrintTools.INFO, temp=True
                    )
                time.sleep(10)
                state = self.state
            except KeyboardInterrupt:
                if PrintTools.confirm_action("Do you want to also cancel the query in BQ?"):
                    self.job.cancel()
                    PrintTools("Cancelled job on BigQuery :)")
                else:
                    PrintTools("Stopped waiting for the job, you can resume anytime!", temp=True)
                return False

        if not hasattr(self.job, "to_dataframe"):
            PrintTools("Currently don't support results for this type of job", _type=PrintTools.ERROR)
            return False

        if self.bqt_obj and self.bqt_obj.verbose:
            bytes_processed = self.job.total_bytes_processed or 0
            bytes_billed = self.job.total_bytes_billed or 0
            PrintTools(
                "Query done! Processed: {} Billed: {} Cost: ${:,.2f}".format(
                    PrintTools.human_number(bytes_processed),
                    PrintTools.human_number(bytes_billed),
                    self.bqt_obj.get_config("bq_price") * bytes_billed,
                ),
                _type=PrintTools.SUCCESS,
            )
        return True

    @property
    @catch_bq_exceptions
    def results(self):
        # this is only used for cached results, normal flow always fetches the
        # data from the API
        if self._results is not None:
            return self._results

        should_fetch = self._wait_for_results()
        if not should_fetch:
            return

        results = self._get_query_results()
        for func in self._on_results_callbacks:
            func(results, self)
        return results

    @catch_bq_exceptions
    def results_generator(self, generator_row_size):
        # this is only used for cached results, normal flow always fetches the
        # data from the API
        # generators don't currently support cached results, but having this
        # here makes it work once/if we add that and doesn't have a side effect
        if self._results is not None:
            return self._results

        # this is for returning the results as a generator
        should_fetch = self._wait_for_results()
        if not should_fetch:
            return
        if self.result_format == "polars":
            PrintTools("Generator currently supports only pandas.", _type=PrintTools.WARNING)
        results = self._get_query_results_generator(generator_row_size=generator_row_size)
        return results

    def _get_query_results(self):
        """Get the results as data object defined by self.result_format.

        Based on: (link broken down cause of PEP8)
            https://googlecloudplatform.github.io/google-cloud-python/
            latest/_modules/google/cloud/bigquery/table.html
            #RowIterator.to_dataframe
        Returns:
            data object populated with row data and column headers from
            the query results. The column headers are derived from the
            destination table's schema.
        """

        row_iter = self.job.result()
        _ = self._check_memory()

        if self.result_format == "pandas":
            extracted_result = self.job.to_dataframe(progress_bar_type=self.progress_bar_type)
        elif self.result_format == "polars":
            column_headers = [field.name for field in row_iter.schema]
            extracted_result = pl.from_arrow(row_iter.to_arrow(), schema=column_headers)
        else:
            raise ValueError("Invalid result format!")

        PrintTools("Finished downloading results", _type=PrintTools.SUCCESS)
        return extracted_result

    def _get_query_results_polars(self):
        _ = self._check_memory()
        row_iter = self.job.result()
        arrow_result = row_iter.to_arrow(progress_bar_type=self.progress_bar_type)
        return pl.from_arrow(arrow_result)

    def _check_memory(self):
        total, size_bytes = self.get_size()
        if size_bytes and not self._fits_in_memory(size_bytes, verbose=True):
            raise BqResultsTooBig("Results you're trying to fetch don't fit into your memory")
        return total

    def _get_query_results_generator(self, generator_row_size):
        """Get the results as a generator of pandas.DataFrame object"""
        row_iter = self.job.result()
        rows = []
        column_headers = [field.name for field in row_iter.schema]
        for row in iter(row_iter):
            rows.append(row.values())
            if len(rows) == generator_row_size:
                yield pandas.DataFrame(rows, columns=column_headers)
                rows = []
        # all the left-over rows
        if rows:
            yield pandas.DataFrame(rows, columns=column_headers)

    def get_size(self):
        """Returns the size of the results

        Returns:
            tuple (number of rows, size in bytes)
        """
        des = self.job.destination
        if not des or not self.bqt_obj:
            return None, None
        table = self.bqt_obj.bq_client.get_table(des)
        return table.num_rows, table.num_bytes

    def _fits_in_memory(self, table_size, verbose=False, warning_thresh=0.8):
        """Checks to see if a table will fit into memory

        Args:
            table_size (int): size of the table in bytes
            verbose (bool): whether to print messages or not
            warning_thresh (float): (0, 1) float, will print a warning if
                the results would fill more than this percentage of the
                memory (verbose also needs to be True)
        Returns:
            bool, whether results fit into memory or not
        """
        # skip check if psutil is not installed
        if psutil is None:
            return True

        mem = psutil.virtual_memory()
        sizes = [" Bytes", " KB", " MB", " GB"]
        if mem.available < table_size:
            if verbose:
                PrintTools(
                    "The results you're fetching is %s but you have %s of free"
                    " memory, aborting"
                    % (
                        PrintTools.human_number(table_size, millnames=sizes),
                        PrintTools.human_number(mem.available, millnames=sizes),
                    ),
                    _type=PrintTools.ERROR,
                )
            return False
        elif mem.available * warning_thresh < table_size and verbose:
            PrintTools(
                "The results you're fetching will fill more than %s%% of your"
                " memory (available: %s, result size: %s)"
                % (
                    warning_thresh * 100,
                    PrintTools.human_number(mem.available, millnames=sizes),
                    PrintTools.human_number(table_size, millnames=sizes),
                ),
                _type=PrintTools.WARNING,
            )

        return True

    def get_errors(self):
        return self.job.errors

    def human_readable_errors(self):
        errors = []
        for err in self.get_errors():
            errors.append("   * %s: %s" % (err["reason"], err["message"]))
        if not errors:
            return ""
        return "Job Errors:\n" + "\n".join(errors)

    @property
    def is_finished(self):
        self.job.reload()
        return self.job.state == "DONE"

    @property
    def is_failed(self):
        return bool(self.job.errors)

    @property
    def job_id(self):
        return self.job.job_id

    @property
    def project(self):
        return self.job.project

    @property
    def query(self):
        query = Query(self.job.query)
        return query.colorize()

    @property
    def state(self):
        """Return the most up-to-date job status

        Returns:
            string, one of BqJobResult.STATUS_*
        """
        if self.is_finished:
            if self.get_errors():
                return self.STATE_FINISHED_ERROR
            return self.STATE_FINISHED_SUCCESS
        elif self.time_started:
            return self.STATE_RUNNING
        return self.STATE_PENDING

    def elapsed_seconds(self):
        return (arrow.get(self.job.ended) - arrow.get(self.job.created)).total_seconds()

    @property
    def time_created(self):
        """Time the job was created, this might be different that when the job
        was actually started if the job was queued for later
        """
        return self.job.created

    def seconds_since_created(self):
        return (arrow.utcnow() - self.time_created).total_seconds()

    @property
    def time_started(self):
        """Time the job was started, this might later than when the job was
        created
        """
        return self.job.started

    def seconds_since_started(self):
        return (arrow.utcnow() - self.time_started).total_seconds()

    @property
    def time_ended(self):
        return self.job.ended

    def serialize(self):
        return {"job_id": self.job_id}


class BqJobWithoutResults(BqJobResult):
    """A job class wrapper for jobs that don't have a result, e.g. copy table"""

    def has_results(self):
        return False

    @property
    def results():
        return None


class BqJobManager(object):
    # list of running jobs, static
    _jobs = []

    # Maximum number of jobs to run at once
    _concurrency = 10

    @classmethod
    def _job_in_queue(cls, job_obj):
        return job_obj.job_id in [j.job_id for j in cls._jobs]

    @classmethod
    def get_running_jobs(cls):
        """Return the list of running jobs"""
        return cls._jobs[:]

    @classmethod
    def queue_job(cls, job_obj):
        """Queue a job

        Args:
            job_obj (BqJobResult): job class to queue, can be any subclass of
                BqJobResult
        Returns:
            job_obj
        """
        if not job_obj:
            raise ValueError("Can't queue None :(")
        if cls._job_in_queue(job_obj):
            return job_obj

        cls.wait_for_jobs(min_free_slots=1)  # need one slot for the new job
        cls._jobs.append(job_obj)
        return job_obj

    @classmethod
    def wait_for_jobs(cls, sleep=2, halt_on_error=True, min_free_slots=None):
        """Block until all jobs are finished

        Args:
            sleep (int): seconds to sleep between status polls
            halt_on_error (bool): Raise and exit on failed jobs or not
            min_free_slots (int): [optional] if provided, this call will block
                until at least this many slots are free, otherwise it'll block
                untill at jobs have finished
        """
        if not min_free_slots:
            min_free_slots = cls._concurrency
        _jobs = cls._jobs[:]
        while len(_jobs) > cls._concurrency - min_free_slots:
            try:
                done_jobs = cls.get_done_jobs(_jobs)
                errors = [j for j in done_jobs if j.get_errors()]
                _jobs = [j for j in _jobs if j not in done_jobs]
                if errors:
                    cls._print_job_errors(errors)
                    if halt_on_error:
                        # this ensures we remove jobs that failed this time
                        # from the next run
                        cls._jobs = _jobs
                        raise RuntimeError("Couldn't continue because some jobs failed")
                time.sleep(sleep)
            except KeyboardInterrupt:
                if PrintTools.confirm_action("Do you want to also cancel **all** jobs in BQ?"):
                    for j in _jobs:
                        j.job.cancel()
                    cls._jobs = []
                    PrintTools("Cancelled all jobs on BigQuery :)")
                    return
                break
        cls._jobs = _jobs

    @classmethod
    def get_done_jobs(cls, jobs):
        """Get list of jobs that done within all jobs indicated by `jobs`

        Args:
            jobs (list[BqJobResult])
        Returns:
            list[BqJobResult]
        """
        done_jobs = []
        for j in jobs:
            try:
                if j.is_finished:
                    done_jobs.append(j)
            except NotFound:
                continue
        return done_jobs

    @classmethod
    def wait_for_job(cls, job_obj, halt_on_error=True, sleep=2):
        state = job_obj.state
        while state not in job_obj.STATE_FINISHED:
            try:
                d = PrintTools.human_duration(job_obj.seconds_since_created())
                if state == job_obj.STATE_PENDING:
                    PrintTools(
                        "Your job is queued but not started yet," " %s elapsed" % d, _type=PrintTools.INFO, temp=True
                    )
                else:
                    if job_obj.seconds_since_started() < sleep:
                        PrintTools("Running job %s:%s" % (job_obj.project, job_obj.job_id), _type=PrintTools.INFO)
                    PrintTools(
                        "%s elapsed, waiting for job to finish." "" % (d or "0 seconds"),
                        _type=PrintTools.INFO,
                        temp=True,
                    )
                time.sleep(sleep)
                state = job_obj.state
            except KeyboardInterrupt:
                if PrintTools.confirm_action("Do you want to also cancel the job in BQ?"):
                    job_obj.job.cancel()
                    PrintTools("Cancelled job on BigQuery :)")
                break

        if cls._job_in_queue(job_obj):
            cls._jobs = [j for j in cls._jobs if j.job_id != job_obj.job_id]
        if job_obj.get_errors():
            cls._print_job_errors([job_obj])
            if halt_on_error:
                raise RuntimeError("Couldn't continue because some jobs failed")
        else:
            PrintTools("Job finished successfully!", _type=PrintTools.SUCCESS)

    @classmethod
    def _print_job_errors(cls, jobs, halt=True):
        print("Some jobs failed with errors :(")
        for job in jobs:
            for err in job.get_errors():
                print("   * %s: %s" % (err["reason"], err["message"]))

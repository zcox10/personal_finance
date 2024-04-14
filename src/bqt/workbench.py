import os

# used to prefix BQ job ids when running in Workbench
WORKBENCH_JOB_ID_PREFIX_TEMPLATE = "wb-{profile}-{context}-"

# env var used to check if we are in Workbench, and to get the profile name
WORKBENCH_PROFILE_ENV_VAR = "WORKBENCH_PROFILE_PARENT"


def get_workbench_job_id_prefix(context):
    """Get the job id prefix for Workbench

    Args:
        context (str): The context of the job, e.g. python, sql, nbsql, etc.

    Returns:
        str: The job id prefix
    """
    profile = os.getenv(WORKBENCH_PROFILE_ENV_VAR, "unknown")
    return WORKBENCH_JOB_ID_PREFIX_TEMPLATE.format(profile=profile, context=context)


def is_workbench():
    """Check if we are running in Workbench by checking
        if the WORKBENCH_PROFILE_PARENT env var is set

    Returns:
        bool, True if running in Workbench
    """
    return os.getenv(WORKBENCH_PROFILE_ENV_VAR) is not None

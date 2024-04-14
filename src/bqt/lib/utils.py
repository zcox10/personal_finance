import os
from typing import Set

# Spotify-specific mapping of field names and semnatic types as specified by DPO
# Subset of the fields from http://go/data-annotate-semantic-type
FIELD_DESCRIPTION_MAP = {
    # broad
    'userid': "{ policy: { semanticType: userId } }",
    'gender': "{ policy: { semanticType: gender } }",
    'country': "{ policy: { semanticType: country } }",
    'regcountry': "{ policy: { semanticType: country } }",
    'registrationcountry': "{ policy: { semanticType: country } }",
    'reportingcountry': "{ policy: { semanticType: country } }",
    'trackuri': "{ policy: { semanticType: noUsernameURI } }",
    'artisturi': "{ policy: { semanticType: noUsernameURI } }",
    # narrow
    "personalname": "{ policy: { semanticType: personalName } }",
    "email": "{ policy: { semanticType: email } }",
    "birthday": "{ policy: { semanticType: birthday } }",
    "birthdate": "{ policy: { semanticType: birthday } }",
    "employee": "{ policy: { semanticType: employee } }",
    "username": "{ policy: { semanticType: username } }",
    # strict
    "sexualorientation": "{ policy: { semanticType: sexualOrientation } }",
    "ethnicity": "{ policy: { semanticType: ethnicity } }",
    # other
}


SQL_FILE_EXTS = {"sql", "bqsql"}
JINJA_FILE_EXTS = {"j2", "jinja", "jinja2"}
SUPPORTED_FILE_EXTS: Set[str] = set.union(SQL_FILE_EXTS, JINJA_FILE_EXTS)


def resolve_query_to_string(query_or_path: str) -> str:
    """
    Add flexibility to resolve multiple input types to a SQL query string.

    When a query string is the input, this will return the query string. When a valid SQL file
    path is the input, this will read the query in that file, and return it as a string.

    Args:
        query_or_path (str): Either a SQL query string or a valid path to a SQL or jinja SQL file

    Returns:
        query (str): A SQL query string to use in various bqt methods
    """

    # Check if the input ends with a valid extension
    if query_or_path.split('.')[-1] not in SUPPORTED_FILE_EXTS:
        # Return original query
        return query_or_path

    # Read and return the query from the file
    full_path_to_query = os.path.abspath(query_or_path)

    try:
        with open(full_path_to_query) as f:
            return f.read()
    except IOError:
        raise


def read_return_query(query_or_path):
    """
    Add flexibility to resolve multiple input types to a SQL query string.

    When a query string is the input, this will return the query string. When a valid SQL file
    path is the input, this will read the query in that file, and return it as a string.

    Args:
        query_or_path (str): Either a SQL query string or a valid path to a SQL or jinja SQL file

    Returns:
        query (str): A SQL query string to use in various bqt methods
    """
    return resolve_query_to_string(query_or_path)

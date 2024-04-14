try:
    from IPython.display import HTML
except ImportError:
    HTML = None

from pygments import highlight
from pygments.lexers.sql import SqlLexer
from pygments.formatters import HtmlFormatter
import re
import sqlparse

from bqt.lib.print_tools import PrintTools


class Query:
    """Class for transforming the output of query strings."""

    std_sql = re.compile('')

    def __init__(self, raw_query):
        self.raw_query = raw_query

    @property
    def format(self):
        """Return a string with the formatted query"""
        formatted_query = sqlparse.format(self.raw_query,
                                          reindent=True,
                                          keyword_case='upper')
        return formatted_query

    def colorize(self, highlighted_lines=None):
        """Return the formatted query as HTML with highlighted syntax.

        Args:
            highlighted_lines (list): List of integers that specify
        which lines in the code should be highlighted. Default: None

        Returns:
            IPython.core.display.HTML object if run within a notebook.
            A string with the formatted query if not in a notebook.
        """
        # Return the formatted query if not running inside a notebook.
        if HTML is None or not PrintTools.in_ipynb():
            return self.format

        if highlighted_lines is None:
            highlighted_lines = []
        colorized_query = HTML(
            highlight(self.format,
                      SqlLexer(),
                      HtmlFormatter(full=True,
                                    style='colorful',
                                    hl_lines=highlighted_lines))
        )

        return colorized_query

    @classmethod
    def remove_and_report_standard(self, query):
        """Remove and report whether there's a standard SQL annotation
        in the query.

        Random Rant: adding an annotation that is not compatible with a syntax
        of a language is stupid.

        Args:
            query (string): query to search in
        Returns:
            tuple(cleaned_query, bool whether the tag was found or not)
        """
        if '#standardsql' in query.lower():
            pos = query.lower().index('#standardsql')
            return query[0:pos] + query[pos + 12:], True
        return query, False

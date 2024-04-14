from IPython.core.magic import (Magics, magics_class, cell_magic)
from bqt import bqt
from bqt.workbench import get_workbench_job_id_prefix, is_workbench
import IPython
from IPython.core.magic_arguments import (argument, magic_arguments,
                                          parse_argstring)


class ArgumentError(Exception):
    pass


"""
.. function:: %%bqt
    IPython cell magic to run a query and display the result as a DataFrame
    %%bqt <target_var> [-q --query_method]
        <query>
"""


@magics_class
class BqtMagic(Magics):

    @magic_arguments()
    @argument('target_var', nargs="?", help='Variable to save dataframe output. Default saves to variable temp.')
    @argument('-q', '--query_method', help='Method to execute query.', default="query")
    @argument('--result_format', help='Format for the returned result. Options are: pandas, polars.',
              default="pandas")
    @argument('--cache', help='One of local, bq or none', default="local")
    @cell_magic
    def bqt(self, line, query):
        data_preview = None
        query_result = None
        args = parse_argstring(self.bqt, line)
        try:
            cache = args.cache if args.cache != "none" else None

            # build job_id_prefix if query is run within Workbench
            job_id_prefix = get_workbench_job_id_prefix("nbsql") if is_workbench() else None

            if args.query_method == "query":
                query_result = bqt.query(query, result_format=args.result_format, cache=cache, job_id_prefix=job_id_prefix)
                if query_result is not None:
                    IPython.get_ipython().push({"temp" if args.target_var is None
                                                else args.target_var: query_result})
                    data_preview = query_result.head()
            else:
                raise ArgumentError("There is no support for this method at this time or it does not exist.")
        except Exception as e:
            print(e)
            return e
        return data_preview


def load_ipython_extension(ipython):
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """
    # You can register the class itself without instantiating it.  IPython will
    # call the default constructor on it.
    ipython.register_magics(BqtMagic)

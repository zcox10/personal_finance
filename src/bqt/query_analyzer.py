"""
WARNING: this is an experimental feature. zero operational guarantees are made
    don't even expect the advertised functionality to work on all queries.
    I tried my best to cover as many cases as possible but parsing a query into
    useable chunks has a lot of corner cases and exceptions that might or might
    not have been handled here properly.
"""
from bqt.lib.query_parser import SqlInfo


class QueryAnalyzer(object):

    def __init__(self, bqt_obj, query):
        self.bqt_obj = bqt_obj
        self.query = query
        self.part_index = 0
        self._sm = None

    @property
    def sqlinfo(self):
        """Access to the underlying SqlInfo object with the tree like structure
        """
        if self._sm:
            return self._sm

        self._sm = SqlInfo(self.query)
        self._sm.extract_info()

        return self._sm

    @property
    def _part(self):
        """Access to a specific part of a query"""
        return self.sqlinfo.parts[self.part_index]

    @property
    def selected_columns(self):
        """Returns the columns that are selected as a result of the query

        Returns:
            list(SelectColumn) or None if no select statements can be parsed
        Raises:
            QueryParsingFailed when parsing of the query fails
        """
        if self._part.selects:
            return self._part.selects[0].columns
        return None

    def get_all_tables(self, part=None):
        """Returns all the tables used in the query FROM/JOIN statements

        Note: this call also returns all table aliases, e.g. from WITH clauses
        and it's up to the caller to differentiate between actual tables and
        table aliases although you can write a similar method that would do
        that based on the data in the query

        Args:
            part (set): part of the query to analyze, default to all the query
        Returns:
            set(string) of all table names
        """
        part = part or self._part
        tables = set()
        for select in part.selects:
            tables |= self._traverse_select(select)

        for subq in part.sub_queries:
            tables |= self.get_all_tables(subq)

        return tables

    def _traverse_select(self, select):
        """Aux method to traverse a select statement for FROM/JOIN clauses"""
        tables = set([select.table])
        for join in select.joins:
            tables.add(join.table)
        return tables

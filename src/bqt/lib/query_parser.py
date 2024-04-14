"""
WARNING: this is an experimental feature. zero operational guarantees are made
    don't even expect the advertised functionality to work on all queries.
    I tried my best to cover as many cases as possible but parsing a query into
    useable chunks has a lot of corner cases and exceptions that might or might
    not have been handled here properly.
"""

import sqlparse
from sqlparse import tokens as sql_tokens
from uuid import uuid4
import re


class QueryParsingFailed(Exception):
    pass


class SqlInfo(object):
    # list of things to ignore when parsing query parts
    ignore_comp = (
        sql_tokens.Punctuation, sql_tokens.Whitespace,
        sql_tokens.Newline, sql_tokens.Comment
    )
    # list of things that can safely be ignored from the start
    ignore_white = (
        sql_tokens.Comment, sql_tokens.Newline, sql_tokens.Whitespace
    )

    # regex to extract table name and alias for legacy and standard
    table_regex = re.compile(
        r'[`\[]?([^\`\]\s]+)[`\]]?\s*(AS|as)?\s*([a-zA-Z0-9_]+)?'
    )
    # the sqlparse lib doesn't recognize `* EXCEPT(a, b, c)` syntax
    # using this to exctract that
    except_regex = re.compile(r'(EXCEPT|except)\(([^\)]+)\)')

    def __init__(self, query):
        self.query = query
        self.parsed = sqlparse.parse(query)
        self.parts = []

    def extract_info(self):
        """Parse the query and extract all supported information"""
        for stmt in self.parsed:
            if stmt.get_type() != 'SELECT':
                continue  # only support SELECT now
            sql_repr = SqlRepr()
            try:
                self._traverse(stmt, sql_repr)
            except Exception as e:
                raise QueryParsingFailed("%s: %s" % (type(e), e.message))
            self.parts.append(sql_repr)

    def _traverse(self, stmt, sql_repr):
        """Traverse a single query possibly having WITH clauses and sub-queries

        Args:
            stmt (list): list of sqlparse tokens
            sql_repr (SqlRepr): the sql representation to update
                with information
        """
        tokens = [
            t for t in stmt if t.ttype not in self.ignore_white
        ]
        total = len(tokens)

        # some indexes into the query so we can traverse it later
        indexes = []
        index_keywords = (
            'select', 'from', 'where', 'group',
            'having', 'with', 'order', 'limit'
        )
        for i, t in enumerate(tokens):
            last_i = len(indexes) - 1
            if (t.ttype in (sql_tokens.Keyword, sql_tokens.Keyword.DML) and
                    str(t).lower() in index_keywords):
                keyword = str(t).lower()
                if keyword == 'select':
                    indexes.append({
                        'select': None, 'from': None, 'where': None,
                        'group': None, 'having': None, 'with': None,
                        'order': None, 'limit': None
                    })
                    last_i += 1
                indexes[last_i][keyword] = i
            elif isinstance(t, sqlparse.sql.Where):
                indexes[last_i]['where'] = i

        # handle with clauses, these are WITH followed by one or more
        # `AS (...)` clauses they will all endup in the subqueries of sql_repr
        i = 0
        while i < total:
            t = tokens[i]
            next_t = tokens[i + 1] if i + 1 < total else None
            if t.ttype == sql_tokens.Keyword.CTE and type(next_t) in (
                    sqlparse.sql.Identifier, sqlparse.sql.IdentifierList):
                clauses = (
                    [next_t] if isinstance(next_t, sqlparse.sql.Identifier)
                    else list(next_t.get_identifiers())
                )
                for clause in clauses:
                    sql_repr.sub_queries.append(
                        self._handle_with_clause(clause))
                i += 2  # skip next_t cause we just parsed it
            else:
                i += 1

        # handle selects (cause there can be UNIONs and more than one SELECT)
        for j, idxs in enumerate(indexes):
            # this is currently supported dict of information
            sql_repr.selects.append({
                'select_columns': [],
                'table': None,
                'table_alias': None,
                'joins': [],
                'where': []
            })
            next_select = (
                indexes[j + 1]['select'] if j + 1 < len(indexes) else total
            )
            select_columns = []
            table_and_joins = []
            where = []
            # parse top to bottom finding sections based on the next one
            # i.e. columns are tokens that are before FROM
            for i in range(idxs['select'] + 1, next_select):
                t = tokens[i]
                if i < (idxs['from'] or next_select):
                    # select columns
                    select_columns.append(t)
                elif i < min(
                        idxs['where'] or next_select,
                        idxs['group'] or next_select,
                        idxs['order'] or next_select,
                        idxs['limit'] or next_select):
                    # from and joins
                    table_and_joins.append(t)
                elif i < min(
                        idxs['group'] or next_select,
                        idxs['order'] or next_select,
                        idxs['limit'] or next_select):
                    # where clauses
                    where.append(t)
                else:
                    # don't care about group by, having, oder by and limit
                    continue
            self._add_select_columns(select_columns, sql_repr.selects[j])
            self._add_table_and_joins(
                table_and_joins, sql_repr.selects[j], sql_repr
            )
            self._add_where(where, sql_repr.selects[j])

        # turn the info dict into an object
        # TODO: refactor to use the object instead from the start
        objected = []
        for si in sql_repr.selects:
            if isinstance(si, SelectStatement):
                objected.append(si)
            else:
                objected.append(self._select_dict_to_obj(si))
        sql_repr.selects = objected

    def _handle_with_clause(self, with_ident):
        """Handle a with clause, this creates a new SqlRepr that represent it

        Args:
            with_ident (list): list of tokens that is the WITH clause
                (minus WITH itself)
        Returns:
            SqlRepr that represents this sub query
        """
        name = [t.value for t in with_ident if t.ttype == sql_tokens.Name][0]
        definition = [
            t for t in with_ident if isinstance(t, sqlparse.sql.Parenthesis)][0]
        definition = [
            t for t in definition if t.ttype != sql_tokens.Punctuation]
        sql_repr = SqlRepr()
        sql_repr.idx = name
        self._traverse(definition, sql_repr)
        return sql_repr

    def _handle_subquery(self, subquery):
        """Handle a subquery in parans

        Args:
            subquery: list of tokens that represent the subquery
        Returns:
            SqlRepr that represents this sub query
        """
        sql_repr = SqlRepr()
        self._traverse(subquery.tokens, sql_repr)
        return sql_repr

    def _add_select_columns(self, defs, select_dets):
        """Cleans up and adds the selected columns to `select_dets`

        Args:
            defs (list): list of definitions that are the selected columns
            select_dets (dict): SELECT details to be updated
        """
        cols = len(defs)
        for i in range(cols):
            d = defs[i]
            if isinstance(d, sqlparse.sql.IdentifierList):
                tok_len = len(d.tokens)
                for j in range(tok_len):
                    t = d.tokens[j]
                    if t.ttype in self.ignore_comp:
                        continue
                    # sqlparse doesn't correctly identify EXCEPT(...)
                    # need to extract that manually and update the last
                    # element which is `*`
                    if (isinstance(t, sqlparse.sql.Function) and
                            str(t.tokens[0]).lower() == 'except'):
                        select_dets['select_columns'][-1].excluded = (
                            self._parse_except(str(t)))
                        continue

                    select_dets['select_columns'].append(SelectColumn(t))
            else:
                # wildcard gets here
                select_dets['select_columns'].append(SelectColumn(d))

    def _add_table_and_joins(self, defs, select_dets, sql_repr):
        """parses all FROM ... JOIN ... sections

        Args:
            defs (list): list of all tokens
            select_dets (dict): SELECT details to be updated
            sql_repr (SqlRepr): sql class to put subqueries in
        """
        i = 0
        total = len(defs)
        while i < total:
            t = defs[i]
            next_t = defs[i + 1] if i + 1 < total else None
            # FROM section
            if t.ttype == sql_tokens.Keyword and str(t).upper() == 'FROM':
                def_ = [
                    e for e in next_t.tokens
                    if isinstance(e, sqlparse.sql.Parenthesis)
                ]
                # handle a subquery in FROM (...)
                if isinstance(next_t, sqlparse.sql.Parenthesis):
                    subq = self._handle_subquery(next_t)
                    select_dets['table'] = subq.idx
                    sql_repr.sub_queries.append(subq)
                elif (isinstance(next_t, sqlparse.sql.Identifier) and
                        next_t.get_alias() and def_):
                    # we get here when we have subquery and alias
                    subq = self._handle_subquery(def_[0])
                    select_dets['table'] = subq.idx
                    select_dets['table_alias'] = next_t.get_alias()
                    sql_repr.sub_queries.append(subq)
                else:  # otherwise just a name
                    table_name, table_alias = self._parse_table_name(
                        str(next_t))
                    select_dets['table'] = table_name
                    select_dets['table_alias'] = table_alias
                i += 2
            # all JOINs
            elif t.ttype == sql_tokens.Keyword and 'JOIN' in str(t).upper():
                # legacy SQL can have an `EACH` after JOIN
                if (next_t.ttype == sql_tokens.Keyword and
                        str(next_t).upper() == 'EACH'):
                    i += 1
                    next_t = defs[i + 1] if i + 1 < total else None
                join_table = next_t
                # handle a subquery in JOIN
                def_ = [
                    e for e in next_t.tokens
                    if isinstance(e, sqlparse.sql.Parenthesis)
                ]
                if isinstance(join_table, sqlparse.sql.Parenthesis):
                    subq = self._handle_subquery(join_table)
                    join_table = subq.idx
                    sql_repr.sub_queries.append(subq)
                elif (isinstance(join_table, sqlparse.sql.Identifier) and
                        join_table.get_alias() and def_):
                    # we get here when we have subquery and alias
                    subq = self._handle_subquery(def_[0])
                    join_table_alias = join_table.get_alias()
                    join_table = subq.idx
                    sql_repr.sub_queries.append(subq)
                else:  # otherwise just a table name
                    join_table = str(join_table)
                # joins can have USING/ON and some conditions on them, get'em
                condition = []  # here because CROSS JOIN doesn't have USING/ON
                if i + 2 < total:
                    next_x2_t = defs[i + 2]
                    if (next_x2_t.ttype == sql_tokens.Keyword and
                            str(next_x2_t).upper() == 'USING'):
                        condition = defs[i + 3].tokens[1:-1]
                        condition = [(x, '=', x) for x in condition]
                        i += 3
                    elif (next_x2_t.ttype == sql_tokens.Keyword and
                            str(next_x2_t).upper() == 'ON'):
                        j = i + 3
                        # while we haven't reached the end or the next JOIN
                        # extract conditions
                        while j < total and not (
                                defs[j].ttype == sql_tokens.Keyword and
                                'JOIN' in str(defs[j]).upper()):
                            if isinstance(defs[j], sqlparse.sql.Comparison):
                                cmp_type = sql_tokens.Token.Operator.Comparison
                                condition.append([
                                    str(x) if x.ttype == cmp_type else x
                                    for x in defs[j].tokens
                                    if x.ttype not in self.ignore_comp
                                ])
                            j += 1
                        i = j
                    else:
                        i += 2
                else:
                    i += 1
                # done with all, add the join to the details
                join_table_name, join_table_alias = self._parse_table_name(
                    join_table)
                select_dets['joins'].append(
                    Join(join_table_name, join_table_alias, condition))
            else:
                i += 1

    def _add_where(self, defs, select_dets):
        """Adds conditions in the wehre clause to `select_dets`"""
        # not sure what to do with the WHERE yet, so just keeping it as is
        select_dets['where'] = [
            [
                t for t in d.tokens
                if t.ttype not in self.ignore_comp and
                str(t).lower() != 'where'
            ] for d in defs if isinstance(d, sqlparse.sql.Where)
        ]

        # there should only be one WHERE clause
        if len(select_dets['where']):
            select_dets['where'] = select_dets['where'][0]

    def _parse_table_name(self, name):
        """Extract table name and alias for both legacy and standard

        Args:
            name (string): one of:
                `table` AS alias, `table`, `table`, `table` alias
                -- and similarly for legacy
        Returns:
            tuple(name, alias)
        """
        name_match = self.table_regex.match(name)
        if not name_match:
            return name, None
        else:
            groups = name_match.groups()
            return groups[0], groups[2]

    def _parse_except(self, exc):
        """Parse EXCEPT(...)

        Args:
            exc (string): except clause like:
                EXCEPT(a, b, c)
        Returns:
            list of columns that are excluded, e.g. ['a', 'b', 'c']
        """
        match = self.except_regex.match(exc)
        if not match:
            return []
        else:
            return [x.strip() for x in match.groups()[1].split(',')]

    def _select_dict_to_obj(self, select_dict):
        return SelectStatement(**select_dict)

    @classmethod
    def remove_table_range(cls, query):
        return re.sub(
            r'(TABLE_DATE_RANGE|table_date_range)\((\[[^\]]+\])',
            r'\2',
            query
        )


class SelectStatement(object):
    """Represents informations about a single SELECT statement"""
    def __init__(self, select_columns, table, table_alias, joins, where):
        self.columns = select_columns
        self.table = table
        self.table_alias = table_alias
        self.joins = joins
        self.where = where


class SqlRepr(object):
    """Represents a standalone query"""
    def __init__(self):
        # ID of this query so subqueries can be found
        self.idx = str(uuid4())
        # list of subqueries each one is another SqlRepr
        self.sub_queries = []
        # list of select statements
        self.selects = []


class SelectColumn(object):
    """Class representing a single SELECT column"""
    def __init__(self, token):
        # the original sqlparse token
        self.t = token
        # if it's wildcard, list of excluded columns
        self.excluded = None

    @property
    def is_wildcard(self):
        return self.t.ttype == sql_tokens.Wildcard

    @property
    def name(self):
        return self.t.get_name()

    @property
    def real_name(self):
        return self.t.get_real_name()

    @property
    def has_alias(self):
        return self.t.has_alias()

    @property
    def is_complex(self):
        """Whether this column is complex, i.e. a function or comparison

        A none complex column is a single identifier
        """
        return not isinstance(self.t, sqlparse.sql.Identifier)

    @property
    def is_func(self):
        return isinstance(self.t, sqlparse.sql.Function)

    @property
    def is_boolean_comparison(self):
        return isinstance(self.t, sqlparse.sql.Comparison)

    def __repr__(self):
        return str(self.t)


class Join(object):
    """Represents a join"""
    def __init__(self, table, table_alias, condition):
        # cleaned table URI
        self.table = table
        # cleaned table alias
        self.alias = table_alias
        # list of conditions
        self.condition = condition

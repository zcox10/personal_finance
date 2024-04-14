import arrow
from arrow.arrow import Arrow
from arrow.parser import ParserError
import collections
import datetime
import hashlib
from jinja2 import Environment
import numbers
import re
from six import string_types
import string

from bqt.lib.decorators import expose_as_api
from bqt.lib.table import BqTable
from bqt.lib.short_cache import ShortCache


class InvalidDatePartException(Exception):
    pass


class InvalidDateOffsetException(Exception):
    pass


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'


class ParameterManager(object):

    # these two lists are used to find date formats automatically, they only
    # cover a short range of date formats because these are mutuality
    # exclusive.
    # NOTE: THEY NEED TO be indexed the same
    valid_date_formats = [
        'YYYYMMDD', 'YYYYMM', 'YYYY',
        'YYYY-MM-DD', 'YYYY-MM', 'YYYY/MM/DD', 'MM/DD/YYYY',
        'YYYYMMDDTHH', 'YYYYMMDDHH', 'YYMMDD', 'MMDD'
    ]
    valid_date_formats_strf = [
        '%Y%m%d', '%Y%m', '%Y', '%Y-%m-%d', '%Y-%m', '%Y/%m/%d', '%m/%d/%Y',
        '%Y%m%dT%H', '%Y%m%d%H', '%y%m%d', '%m%d'
    ]

    # used for dynamic date ranges that don't specify a date format
    default_date_format = 'YYYY-MM-DD'

    # used to extract offset modifiers from params, e.g. {date[-1M]}
    offset_test = re.compile(r"[^\[]+\[[^\]]*\][^\[]*")
    OFFSET_REGEX = r"\[(([\-\+]{0,1}\d+(D|H|W|M|Y))*)\]"
    OFFSET_MODIFIER_REGEX = r'([\-\+]{0,1}\d+)(D|H|W|M|Y)'
    OFFSET_CONVERSATION = {  # anything here must also be included in the regex
        'D': 'days', 'H': 'hours', 'W': 'weeks', 'M': 'months', 'Y': 'years'
    }

    def __init__(self, bqt_obj):
        self.bqt_obj = bqt_obj
        self.params = {}
        self.short_cache = ShortCache()

    @expose_as_api
    def set_param(self, name, value):
        """Set a parameter to be used in queries

        Args:
            name (string): name of the parameter
            value (mixed): value of the parameter
        """
        if value is None:
            raise RuntimeWarning(
                "You're setting a parameter to None, that's weird")
        self.params[name] = value

    @expose_as_api
    def set_params(self, **kwargs):
        """Set a parameter to be used in queries

        Args:
            one or more key-value pairs passed into the function
        """
        for key in kwargs:
            self.set_param(key, kwargs[key])

    @expose_as_api
    def get_param(self, name):
        """get the value of the parameter given by `name`"""
        return self.params.get(name)

    @expose_as_api
    def del_param(self, name):
        if name in self.params:
            del self.params[name]

    def _insert_magic_params(self, template):
        """Replace all magic parameters in the template.

        Currently this is:
            {LATEST} which gets replaced with the latest table partition
            {LATEST[offset]} which gets replaced with the latest table partition
                with offset
        Args:
            template (string): template with magic params in it
        Returns:
            tempalte with magic params replaced
        """
        # This matches to [0] project, [1] dataset
        # [2] table name with the magic param in it
        # [3] table name without magic param or partition
        # [4] (optional) offset.
        matches = re.findall(
            r"[`\[]([a-zA-Z0-9-_]+)[:\.]([a-zA-Z0-9-_]+)\.(([a-zA-Z0-9-_]+)"
            r"\{LATEST(\[.+\])?\})[`\]]", template)

        if not matches:
            return template
        for match in matches:
            cache_key = '.'.join(match)
            replace_from = match[2]
            replace_to = self.short_cache.get(cache_key)

            if not replace_to:
                latest = BqTable.get_latest_partition(
                    match[3], match[1], match[0], client=self.bqt_obj.bq_client
                )
                if latest is None:
                    raise ValueError(
                        "Table `%s.%s.%s{LATEST}` wasn't found in BigQuery" %
                        (match[0], match[1], match[3])
                    )
                replace_to = latest.name
                self.short_cache.set(cache_key, replace_to)

            # If an offset was found
            if match[4] != '':
                latest_partition = replace_to.replace(match[3], '')
                _, __, offset = self._extract_offset(cache_key)
                offset_latest_partition = self._as_date(
                    latest_partition) + offset
                partition_type, _ = BqTable.find_partition_type(
                    replace_to
                )
                replace_to = match[3] + offset_latest_partition.format(
                    partition_type)
            template = template.replace(replace_from, replace_to)

            if self.bqt_obj and self.bqt_obj.verbose:
                self.bqt_obj.print_msg("LATEST%s: using `%s.%s.%s`" % (
                    match[4], match[0], match[1], replace_to)
                )

        return template

    def get_template_params(self, template):
        """get parameters that needs to be replaced in `template`

        Args:
            template (string): template with placeholder in the format:
                {placeholder}
        Returns:
            dict(placeholder -> value) that can be replaced in the template
        """
        # don't touch the string if no parameters are set
        if not self.params:
            return template, {}

        placeholders = [
            v[1] for v in string.Formatter().parse(template)
            if v[1] is not None
        ]
        values = {}
        for placeholder in placeholders:
            new_placeholder, ph_without_offset, offset = (
                self._extract_offset(placeholder))
            date_format, param_name = self._extract_date_format(
                ph_without_offset
            )

            if new_placeholder != placeholder:  # i.e. with offset
                # replace the name becaue python freaks out about []
                template = template.replace(placeholder, new_placeholder)
                if not date_format:
                    date_format = (
                        self._find_date_format(
                            self.params[ph_without_offset]) or
                        self.default_date_format
                    )
                    param_name = ph_without_offset

            if date_format:
                value = self._param_as_date(param_name) + offset
                values[new_placeholder] = value.format(date_format)
            elif placeholder in self.params:
                if isinstance(self.params[placeholder], Arrow):
                    value = self.params[placeholder] + offset
                    values[placeholder] = value.format(
                        self.default_date_format
                    )
                elif isinstance(self.params[placeholder], (list, set, tuple)):
                    value = self.params[placeholder]
                    if any([type(v) not in (int, float) for v in value]):
                        values[placeholder] = ', '.join(
                            ["'%s'" % v for v in value]
                        )
                    else:
                        values[placeholder] = ', '.join(map(str, value))
                else:
                    values[placeholder] = self.params[placeholder]

        return template, values

    def insert_params(self, template, jinja=True):
        """Insert values into placeholders in `template`

        Args:
            template (string): template with placeholder in the format:
                {placeholder}
            jinja (bool): If False, opt-out of jinja rendering step
        Returns:
            string, template with placeholders filled
        """

        # render jinja first, then handle {placeholders}
        if jinja:
            template = self._render_jinja(template)
        template, values = self.get_template_params(template)
        if values:
            try:
                # python 3
                template = template.format_map(SafeDict(**values))
            except AttributeError:
                # python 2
                template = string.Formatter().vformat(
                    template, (), SafeDict(**values))
        return self._insert_magic_params(template)

    def _extract_offset(self, placeholder):
        """Exract an offset from a date, if one exists

        Args:
            placeholder (string): e.g. "date[-10D]_YYYYMMDD"
        Returns:
            tuple(
                safe new name for placeholder that can be used for formatting,
                placeholder name without the offset part,
                datetime.timedelta() representing the offset
            )
        """
        if not self.offset_test.match(placeholder):
            return placeholder, placeholder, datetime.timedelta()

        offset = re.search(self.OFFSET_REGEX, placeholder)
        if not offset:
            raise InvalidDateOffsetException(
                "Placeholder `%s` doesn't use a valid date offset" %
                placeholder
            )
        modifiers = re.findall(
            self.OFFSET_MODIFIER_REGEX, offset.groups()[0]
        )
        params = {}
        for mod in modifiers:
            params[self.OFFSET_CONVERSATION[mod[1]]] = int(mod[0])
        now = arrow.utcnow()
        hash_name = hashlib.sha224(
            (offset.groups()[0]).encode('utf-8')
        ).hexdigest()
        new_placeholder = placeholder.replace(
            '[%s]' % offset.groups()[0], hash_name
        )
        name_without_offset = placeholder.replace(
            '[%s]' % offset.groups()[0], ''
        )
        return (
            new_placeholder,
            name_without_offset,
            now.shift(**params) - now
        )

    def _extract_date_format(self, placeholder, params=None):
        """Extract the date format from a placeholder of form `{date_YYYYMMDD}`

        Args:
            placeholder (string): placeholder
            params (dict): list of params to search for placeholder in
        Returns:
            tuple(format e.g. YYYYMMDD, placeholder parameter e.g. date)
        """
        if not params:
            params = self.params
        for frmt in self.valid_date_formats:
            if (placeholder[-len(frmt):] == frmt and
                    placeholder[:-len(frmt) - 1] in params and
                    self._param_as_date(placeholder[:-len(frmt) - 1], params)
                    is not None):
                return frmt, placeholder[:-len(frmt) - 1]
        return None, None

    def _param_as_date(self, param_name, params=None):
        """Return the param_name as a date object"""
        if not params:
            params = self.params
        return self._as_date(params[param_name])

    def _as_date(self, value):
        """return value as a date object

        Args:
            value (mixed): value to be checked for date
        Returns:
            Arrow date object
        """
        # if it's a number then it's not a date, we don't support unix TS
        # because it's too ambiguous to parse
        if isinstance(value, numbers.Number):
            return None

        # if it can be cast to an int and the length doesn't make it
        # a YYYYMMDD format then it's not a date
        try:
            int(value)
            return arrow.get(value, 'YYYYMMDD')
        except (ValueError, TypeError):  # it's not an int
            pass  # it might be a date
        except ParserError:  # it's an int but not YYYYMMDD, not a date
            return None

        # can arrow parse it by default?
        try:
            return arrow.get(value)
        except ParserError:
            return None

    @expose_as_api
    def param_range(self, name, _from, _to=None, date_part='days', step_size=1,
                    date_format=default_date_format):
        """Specify a dynamic parameter range

        Example:
            for day in bqt.param_range('date', '2018-01-01', '2018-02-01'):
                bqt.create_table(....)

        Args:
            name (string): name of the parameter and placeholder in template
            _from (mixed): if list, will go over list items,
                           if number or date, will create range over those
            _to (mixed): only used when `_from` is a date or number, marks
                         the end of the range
            date_part (string): one of:
                    days, hours, minutes, months, ...
                specifies which part of the date will create the range.
                for example when 'days', the range will go over days between
                `_from` and `_to`
            step_size (int): step size between the range
                for example, when 2, the elements will be 2 units apart
            date_format (string): Date format to translate the parameter into
                defaults to YYYY-MM-DD
        """
        if (isinstance(_from, collections.Iterable) and
                not isinstance(_from, string_types)):
            values = _from
        elif self._as_date(_from) and self._as_date(_to):
            start = self._as_date(_from)
            end = self._as_date(_to)
            if not self._date_range_valid(start, end, step_size):
                raise ValueError(
                    "You're creating a date range that has zero or negative "
                    "length, does `%s` to `%s` in `%s` increments make"
                    " sense?" % (_from, _to, step_size)
                )
            values = []
            i = 0
            while True:
                pd = start.shift(**{date_part: step_size * i})
                if pd > end:
                    break
                values.append(pd)
                i += 1
        elif isinstance(_from, int) and isinstance(_to, int):
            values = list(range(_from, _to + 1, step_size))

        for value in values:
            if isinstance(value, Arrow):
                value = value.format(date_format)
            self.set_param(name, value)
            yield value
        self.del_param(name)

    def _date_range_valid(self, start, end, step_size):
        if ((step_size > 0 and (end - start).total_seconds() <= 0) or
                (step_size < 0 and (end - start).total_seconds() >= 0) or
                step_size == 0):
            return False
        return True

    def _find_date_format(self, date):
        """Tries to find the format of a date"""
        for i, frmt in enumerate(self.valid_date_formats_strf):
            try:
                datetime.datetime.strptime(date, frmt)
                return self.valid_date_formats[i]
            except ValueError:
                pass
            except TypeError:
                pass
        return None

    def _render_jinja(self, template: str) -> str:
        sql_jinja_template = Environment().from_string(template)
        return sql_jinja_template.render(**self.params)

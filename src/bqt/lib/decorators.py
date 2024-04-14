# -*- coding: utf-8 -*-

from google.cloud.exceptions import NotFound, Forbidden, BadRequest
import re

from bqt.lib.print_tools import PrintTools


def catch_bq_exceptions(method):
    """[Decorator] used to catch BQ python API exception and print a nicer one
    """
    return catch_bad_request(
        catch_table_forbidden(
            catch_table_not_found(
                method
            )
        )
    )


def expose_as_api(method):
    """[Decorator] flag a class method as an API method.

    This means, that the decorated method will automatically be discoverable
    through the BqT class
    """
    method.exposed = True
    return method


def catch_table_not_found(func):
    """Catches and prints google APIs NotFound messages"""
    def dec(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except NotFound as e:
            PrintTools(e.message, _type=PrintTools.ERROR, bold=True)
    return dec


def catch_table_forbidden(func):
    """Catches and prints google APIs Forbidden messages"""
    def dec(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Forbidden as e:
            match = re.findall(
                r"Table ([a-zA-Z\_\-:0-9.]+) The user ([^\s]+)", e.message
            )
            if match:
                table, user_name = match[0]
                table = table.strip(':')

                error = " ".join([
                    PrintTools.apply_color("User", _type=PrintTools.ERROR,
                                           bold=False, underline=False),
                    PrintTools.apply_color(user_name, _type=PrintTools.ERROR,
                                           bold=True, underline=False),
                    PrintTools.apply_color("doesn't have access to",
                                           _type=PrintTools.ERROR,
                                           bold=False, underline=False),
                    PrintTools.apply_color(table, _type=PrintTools.ERROR,
                                           bold=True, underline=False),
                ])
            else:
                error = e.message
            PrintTools(error, _type=PrintTools.ERROR)
    return dec


def catch_bad_request(func):
    """Catches and prints google APIs BadRequest messages

    Note: this method only works on BqJobResult instances
    """
    def dec(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except BadRequest as e:
            match = re.match(r'(.*)\[(\d+):(\d+)\]$', e.message)
            if not match:
                PrintTools(e.message, _type=PrintTools.ERROR, bold=True)
                return
            error, line, char = match.groups()
            line, char = int(line), int(char)
            error = error[:-4]  # remove the ' at ' at the end
            query_lines = self.job.query.split('\n')
            PrintTools(
                ('>' * 40) + ' Syntax Error',
                _type=PrintTools.ERROR, bold=True
            )
            PrintTools(error, _type=PrintTools.ERROR)
            for i, l in enumerate(query_lines):
                PrintTools(
                    l, _type=PrintTools.WARNING if i + 1 == line else None
                )
                if i + 1 == line:
                    PrintTools(
                        (' ' * (char - 1)) + '⬆',
                        _type=PrintTools.ERROR, bold=True
                    )
            PrintTools(('<' * 40), _type=PrintTools.ERROR, bold=True)
    return dec

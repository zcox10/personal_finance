import math
import sys
import traceback

try:
    inp_func = raw_input
except NameError:
    inp_func = input


class _PrintTools(object):
    """A very simple log function tailored to interactive environments.

    This logger has the ability to output temporary messages that indicate
    progress
    """
    last_temp_len = 0

    pad = ''

    INFO = '\033[94m'
    SUCCESS = '\033[92m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    TYPE_MAPPING = {
        "info": INFO,
        "success": SUCCESS,
        "warn": WARNING,
        "error": ERROR
    }

    mute = False

    def __call__(self, msg, temp=False, flush=True,
                 _type=None, bold=False, underline=False):
        if self.mute:
            return

        msg = str(msg) if not isinstance(msg, str) else msg
        msg = self.apply_color(_PrintTools.pad + msg, _type, bold, underline)
        self._clear_last_message()
        if not temp:
            print(msg)
        else:
            sys.stdout.write(msg + "\r")
            _PrintTools.last_temp_len = len(msg)

        if flush:
            sys.stdout.flush()

    def _clear_last_message(self):
        if _PrintTools.last_temp_len:
            sys.stdout.write((" " * _PrintTools.last_temp_len) + "\r")
            _PrintTools.last_temp_len = 0

    def apply_color(self, msg, _type, bold, underline):
        """Applies styling to the text using ANSI terminal color codes

        Args:
            msg (string): message to style
            _type (string, ANSI code): one of PrintTools.TYPE_MAPPING
            bold (bool): bold text or not
            underline (bool): underline text or not
        """
        prefix = ""
        if _type:
            prefix += _type
        if bold:
            prefix += self.BOLD
        if underline:
            prefix += self.UNDERLINE
        return prefix + msg + (self.ENDC if prefix else "")

    @staticmethod
    def in_ipynb():
        """Returns True if the environment is a notebook, False otherwise"""
        try:
            return get_ipython().__class__.__name__ == 'ZMQInteractiveShell'
        except NameError:
            return False

    @staticmethod
    def human_duration(time_in_sec, short=False):
        """Return a duration expressed in seconds as a human readable duration

        Args:
            time_in_sec (int/float): time in seconds
            short (bool): time parts should be shortened to one letter or not
        Returns
            string, e.g. "5 day(s) 2 hour(s) 30 minute(s) 0 second(s)"
        """
        res = []
        time_in_sec = int(time_in_sec)
        day_rem = int(time_in_sec / 86400)
        if day_rem > 0:
            res.append(("%sd" if short else "%s day(s)") % day_rem)
        time_in_sec %= 86400
        hour_rem = int(time_in_sec / 3600)
        if hour_rem > 0:
            res.append(("%sh" if short else "%s hour(s)") % hour_rem)
        time_in_sec %= 3600
        min_rem = int(time_in_sec / 60)
        if min_rem > 0:
            res.append(("%sm" if short else "%s minute(s)") % min_rem)
        time_in_sec %= 60
        if time_in_sec:
            res.append(("%ss" if short else "%s second(s)") % time_in_sec)

        return " ".join(res)

    @staticmethod
    def human_number(n, prepend='', append='',
                     millnames=['', ' K', ' M', ' G']):
        """Returns a large number in a human readable and concise format.

        Example:
            20000 -> 20 K
            20000000 -> 20 M

        Args:
            n (number): to format
            prepend (string): prepend to the text
            append (string): append to the text
            millnames (list): List of names for each order of magnitude
                increase in the number
        Returns:
            string
        """
        n = float(n)
        millidx = max(
            0,
            min(len(millnames) - 1,
                int(math.floor(0 if n == 0 else math.log10(abs(n)) / 3)))
        )

        return '{}{:.1f}{}{}'.format(
            prepend, n / 10 ** (3 * millidx), millnames[millidx], append
        )

    @staticmethod
    def confirm_action(msg):
        PrintTools("")  # to clear the last message
        inp = inp_func(msg + "[y/n]")
        if inp.lower().strip() in ['yes', 'y']:
            return True
        elif inp.lower().strip() in ['no', 'n']:
            return False

        PrintTools(
            "`%s` is not a valid option, aborting ..." % inp,
            _type=_PrintTools.WARNING
        )
        return False

    @staticmethod
    def input(msg):
        return inp_func(msg)

    def start_pad(self, size=4, char=' '):
        _PrintTools.pad = char * size

    def end_pad(self, size=4, allow_multiple=False):
        _PrintTools.pad = ''

    def print_exception(self):
        self._clear_last_message()
        traceback.print_exc(file=sys.stdout)


PrintTools = _PrintTools()

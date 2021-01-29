import os
import re
import fnmatch
import calendar
from collections import namedtuple

File = namedtuple('File', ['path', 'relPath', 'isDir', 'size', 'mtime'])


def mtime_from_datetime(dt):
    return calendar.timegm(dt.utctimetuple())


def match(name_pattern, recursive, file_obj):
    regex_indicator = "re:"
    is_regex = name_pattern.startswith(regex_indicator)
    if is_regex:
        reg_str = name_pattern[len(regex_indicator):]
        return re.match(reg_str, file_obj.relPath)
    else:
        bn = os.path.basename(file_obj.relPath)
        return fnmatch.fnmatch(bn, name_pattern)

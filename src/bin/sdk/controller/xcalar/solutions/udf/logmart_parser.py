import codecs
import re

import dateutil.parser

regex_node_log = r'(?P<timestamp>\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d+.\d+): (?P<message>.+)'

regex_expserver_out = r'(?P<timestamp>\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d+Z.\d+):(?P<message>.+)'

regex_jupyter_out = r'\[\w (?P<timestamp>\d\d:\d\d:\d\d.\d+) \w+] (?P<message>.+)'

regex_sqldf_out = r'(?P<timestamp>\d\d\/\d\d\/\d\d \d\d:\d\d:\d\d) (?P<message>.+)'

regex_supervisor_log = r'(?P<timestamp>\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d+) (?P<message>.+)'

regex_xcmgmtd_log = r'(?P<timestamp>\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d+.\d+): (?P<message>.+)'

regex_xcmonitor_log = r'(?P<timestamp>\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d+.\d+): (?P<message>.+)'

regex_xpu_log = r'(?P<timestamp>\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d+.\d+): (?P<message>.+)'

regex_xpu_out = r'(?P<timestamp>\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d+) (?P<message>.+)'

# regex_controller_log = r'(?P<timestamp>\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d+) - (?P<message>.+)'

regex_dict = {
    'node.0.log': regex_node_log,
    'node.1.log': regex_node_log,
    'node.2.log': regex_node_log,
    'node.3.log': regex_node_log,
    'node.4.log': regex_node_log,
    'node.5.log': regex_node_log,
    'node.6.log': regex_node_log,
    'node.7.log': regex_node_log,
    'node.8.log': regex_node_log,
    'node.9.log': regex_node_log,
    'node.10.log': regex_node_log,
    'node.11.log': regex_node_log,
    'node.12.log': regex_node_log,
    'node.13.log': regex_node_log,
    'node.14.log': regex_node_log,
    'node.15.log': regex_node_log,
    'node.16.log': regex_node_log,
    'node.17.log': regex_node_log,
    'node.18.log': regex_node_log,
    'node.19.log': regex_node_log,
    'node.20.log': regex_node_log,
    'expserver.out': regex_expserver_out,
    'supervisor.log': regex_supervisor_log,
    'sqldf.out': regex_sqldf_out,
    'jupyter.out': regex_jupyter_out,
    'xcmgmtd.log': regex_xcmgmtd_log,
    'xcmonitor.log': regex_xcmonitor_log,
    'xpu.log': regex_xpu_log,
    'xpu.out': regex_xpu_out
}

MAX_STACK = 500    # Max number of lines to include in a stack trace
MAX_CHARS = 15_000    # Max number of characters to include in a message


def format_dict(dict, source):
    if 'timestamp' in dict:
        dict['timestamp'] = dateutil.parser.parse(dict['timestamp'])
    dict['messagelen'] = len(dict['message'])
    dict['message'] = dict['message'][:MAX_CHARS]
    dict['source'] = source
    return dict


def parse(fullPath, inStream):
    """
        Applies the regex pattern (for node.#.log) to every line. Pull the next line and check if the pattern
        applies, if it does not assume the line is part of a stack trace append the line to the current line's
        message attribute. Keep track of the number of lines added and assign it to the lines field.
    :param fullPath: not used.
    :param inStream: The input stream of the file.
    """
    for key, value in regex_dict.items():
        if not fullPath.endswith(key):
            continue
        utf8_reader = codecs.getreader('utf-8')
        utf8_stream = utf8_reader(inStream)

        re_compiled = re.compile(value)
        utf8_reader = codecs.getreader('utf-8')
        utf8_stream = utf8_reader(inStream)
        for line_string in utf8_stream:
            # line_string = line_string.decode('utf-8')
            match = re_compiled.match(line_string)
            try:
                next_line = next(utf8_stream)    # .decode('utf-8')
            except Exception:
                continue
            if match:
                next_match = re_compiled.match(next_line)
                if next_match:
                    match_dict = match.groupdict()
                    match_dict['lines'] = 1
                    yield format_dict(match_dict, key)
                    next_match_dict = next_match.groupdict()
                    next_match_dict['lines'] = 1
                    yield format_dict(next_match_dict, key)
                else:
                    curr_match_dict = match.groupdict()
                    curr_match_dict['lines'] = 1
                    stack = 0
                    while (not re_compiled.match(next_line)) and (
                            stack <= MAX_STACK) and next_line:
                        curr_match_dict['message'] += next_line
                        curr_match_dict['lines'] += 1
                        stack = stack + 1
                        try:
                            next_line = next(utf8_stream).decode('utf-8')
                        except Exception:
                            continue
                    yield format_dict(curr_match_dict, key)
                    next_match = re_compiled.match(next_line)
                    if next_match:
                        next_match_groupdict = next_match.groupdict()
                        next_match_groupdict['lines'] = 1
                        yield format_dict(next_match_groupdict, key)
                    else:
                        continue
            else:
                continue

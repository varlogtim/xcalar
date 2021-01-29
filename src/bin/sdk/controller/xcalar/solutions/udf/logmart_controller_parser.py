import codecs
import re
import dateutil.parser

import xcalar.container.context as ctx

regex_controller_xpu_out = r"""(?P<timestamp>\d\d\d\d-\d\d-\d\d[ T]\d\d:\d\d:\d\d[,.]\d+) - Pid (?P<process_id>\d+) \{(?P<file_name>.+):(?P<line_no>.+)\} (?P<log_level>.*) - universe_id='(?P<universe_id>.*)' event=['"](?P<message>.*)['"]"""

regex_dict = {'xpu.out': regex_controller_xpu_out}

MAX_STACK = 500    # Max number of lines to include in a stack trace
MAX_CHARS = 15_000    # Max number of characters to include in a message


def format_dict(dict, source):
    if 'timestamp' in dict:
        dict['timestamp'] = dateutil.parser.parse(dict['timestamp'])
    dict['messagelen'] = len(dict['message'])
    dict['message'] = dict['message'][:MAX_CHARS]
    dict['source'] = source
    dict['node_id'] = ctx.get_node_id(ctx.get_xpu_id())
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
        # yield{'fullPath_go': fullPath}
        utf8_reader = codecs.getreader('utf-8')
        utf8_stream = utf8_reader(inStream)
        re_compiled = re.compile(value)
        for line_string in utf8_stream:
            # line_string = line_string.decode('utf-8')
            match = re_compiled.match(line_string)
            if match:
                match_dict = match.groupdict()
                match_dict['lines'] = 1
                yield format_dict(match_dict, key)
            else:
                continue

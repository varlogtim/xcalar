# logmart_all_parser.py

import codecs
import re
import xcalar.container.context as ctx
import dateutil.parser
from datetime import datetime
import logging
import subprocess
from collections import deque

logger = logging.getLogger('xcalar')

regex_node_log = r'(?P<timestamp>\d\d\d\d-\d\d-\d\d[T ]\d\d:\d\d:\d\d[,.]\d+.?\d*):? (?P<message>.+)'

regex_xpu_log = r'(?P<timestamp>\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d+.\d+): (?P<message>.+)'
# regex_supervisor_log = r'(?P<timestamp>\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d+) (?P<message>.+)'
regex_caddy_log = r'(?P<ip>.+) - - \[(?P<btimestamp>\d\d\/[A-Z][a-z][a-z]\/\d\d\d\d:\d\d:\d\d:\d\d) \+0000\] (?P<message>.+)'

regex_out = r'(?P<timestamp>[A-Z][a-z]{2} [\d ]\d \d\d:\d\d:\d\d) (?P<message>.+)'

regex_xcmonitor_out = r'(?P<utimestamp>\d{16}): (?P<message>.+)'
regex_controller_xpu_out = r"""(?P<timestamp>\d\d\d\d-\d\d-\d\d[ T]\d\d:\d\d:\d\d[,.]\d+) - Pid (?P<process_id>\d+) \{(?P<file_name>.+):(?P<line_no>.+)\} (?P<log_level>.*) - universe_id='(?P<universe_id>.*)' event=['"](?P<message>.*)['"]"""

regex_xpu_out = r'(?P<timestamp>\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d+) (?P<message>.+)'

regex_dict = {
    'node.log': (regex_node_log, 'core_log'),
    #'supervisor.log': (regex_supervisor_log, 'core_log'), - is this log used?
    'xcmgmtd.log': (regex_xpu_log, 'core_log'),
    #'xcmonitor.log': (regex_xpu_log, 'xce_log'), what does this log do?
    'xpu.log': (regex_xpu_log, 'core_log'),
    'xpu.out': (regex_xpu_out, 'xpu_out'),
    'expserver.out': (regex_out, 'all_out'),
    'sqldf.out': (regex_out, 'all_out'),
    #'jupyter.out': (regex_out, 'all_out'), is this decom?
    'xcmgmtd.out': (regex_out, 'all_out'),
    'node.out': (regex_out, 'all_out'),
    'caddy.out': (regex_out, 'other_out'),
    'xcmonitor.out': (regex_xcmonitor_out, 'other_out'),
    'caddy.log': (regex_caddy_log, 'caddy_log')    
}

MAX_STACK = 500  # Max number of lines to include in a stack trace
MAX_CHARS = 15_000  # Max number of characters to include in a message


def format_dict(dict, source):
    timestamp = None
    if 'timestamp' in dict:
        timestamp = dateutil.parser.parse(dict['timestamp'])
    elif 'utimestamp' in dict:
        timestamp = datetime.utcfromtimestamp(int(dict['utimestamp'])/1000000)
        dict.pop('utimestamp') 
    elif 'btimestamp' in dict:
        timestamp = datetime.strptime(dict['btimestamp'], "%d/%b/%Y:%H:%M:%S" )
        dict.pop('btimestamp')
    if timestamp:
        dict['timestamp'] = timestamp
        dict['message'] = dict['message'][:MAX_CHARS]
        dict['source'] = source
        return dict
    else:
        return None

logger.info(f'>>> i am here')

def parse(fullPath, inStream):
    """
        Applies the regex pattern (for node.#.log) to every line. Pull the next line and check if the pattern
        applies, if it does not assume the line is part of a stack trace append the line to the current line's
        message attribute. Keep track of the number of lines added and assign it to the lines field.
    :param fullPath: not used.
    :param inStream: The input stream of the file.
    """
    logger.info(f'>>> inside parse')

    re_ctl = re.compile(regex_controller_xpu_out)
    
    fullPath = re.sub(r'\.\d+\.', '.', fullPath)

    match = re.compile(r'.+\/node-(\d+)\/').match(fullPath)
    if match:
        node_id = match.groups()[0]
    else:
        node_id = ctx.get_node_id(ctx.get_xpu_id())

    for key, value in regex_dict.items():
        # yield({'key':key})
        if not fullPath.endswith(key):
            continue
        # yield({'key2':keys[1]})
        utf8_reader = codecs.getreader('utf-8')
        utf8_stream = utf8_reader(inStream)

        re_compiled = re.compile(value[0])
        utf8_reader = codecs.getreader('utf-8')
        utf8_stream = utf8_reader(inStream)
        # for rn, line_string in deque(enumerate(utf8_stream),10):
        for rn, line_string in enumerate(utf8_stream):

        # n = 1000
        # root = '/var/log/xcalar/'
        # command = f'tail -n {n} {root}{fullPath}'
        # logger.info(f'>>> {ctx.get_xpu_id()} going into {fullPath}')
        # p = subprocess.Popen(command.split(), stdout=subprocess.PIPE, universal_newlines=True)
        # for rn, line_string in enumerate(p.stdout):
        #     logger.info(f'>>> inside {root}{fullPath}')

            if key == 'xpu.out':
                if re_ctl.match(line_string):
                    pass
            match = re_compiled.match(line_string)
            if match:
                match_dict = match.groupdict()
                match_dict = format_dict(match_dict, f'{key}|{node_id}')
                if match_dict:
                    match_dict['lines'] = 1
                    match_dict['rn'] = rn
                    match_dict['node_id'] = node_id
                    match_dict['type'] = value[1]
                    # yield format_dict(match_dict, f'{key}|{node_id}')
                    yield match_dict
                    continue
            # else:
            ndict = {'lines': 0, 'rn': rn}
            ndict['message'] = line_string[:MAX_CHARS]
            ndict['node_id'] = node_id
            ndict['source'] = f'{key}|{node_id}'
            ndict['type'] = value[1]
            yield ndict

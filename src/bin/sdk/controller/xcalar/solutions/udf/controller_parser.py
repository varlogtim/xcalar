import re
import codecs


def parse(fullpath, instream):
    prog = re.compile(
        r"\[(?P<timestamp>\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d+)\] (?P<process>p\d+) \{(?P<file_line>.*)\} (?P<action>.*) - universe_id='(?P<universe_id>.*)' event='(?P<event>.*)'"
    )
    msg = re.compile(r"(?P<event>[.*]+): (?P<desc>.+)")
    utf8_reader = codecs.getreader('utf-8')
    utf8_stream = utf8_reader(instream)
    for line_string in utf8_stream:
        d = {}
        res = prog.match(line_string)
        if res:
            d['timestamp'] = res.group(1).replace(' ', 'T').replace(',', '.')
            d['process'] = res.group(2)
            d['file_line'] = res.group(3)
            d['action'] = res.group(4)
            d['universe_id'] = res.group(5)
            message = str(res.group(6))
            parts = message.split(', ')
            for i, part in enumerate(parts):
                res_msg = msg.match(part)
                #                     and not str(res_msg.group(1)).startswith('***:') \
                if res_msg and not str(
                        res_msg.group(1)).startswith('State:') and not str(
                            res_msg.group(1)).startswith('"'):
                    key = str(res_msg.group(1))
                    if key == '***':
                        key = 'Alert'
                    d[key] = res_msg.group(2)
                    # notParsed = False
                else:
                    d[f'object{i}'] = part
            d['msg'] = message
            yield d

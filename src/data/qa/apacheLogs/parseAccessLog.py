import sys
import json

import apache_log_parser

def parseAccessLog(fullPath, inStream):
    line_parser = apache_log_parser.make_parser("%h %l %u %t \"%r\" %>s %O \"%{Referer}i\" \"%{User-Agent}i\"")
    for line in inStream.readlines():
        try:
            yield line_parser(line)
        except:
            continue


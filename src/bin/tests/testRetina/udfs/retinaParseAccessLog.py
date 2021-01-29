import apache_log_parser
import json


def parseAccessLog(fileContent, fullPath):
    line_parser = apache_log_parser.make_parser(
        "%h %l %u %t \"%r\" %>s %O \"%{Referer}i\" \"%{User-Agent}i\"")
    lines = fileContent.split("\n")
    outputStr = []
    for line in lines:
        try:
            logData = line_parser(line)
        except Exception:
            continue
        outputStr.append(logData)
    return json.dumps(outputStr, default=str)

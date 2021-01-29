from datetime import datetime


def getDay(datestring):
    try:
        return datetime.strptime(
            datestring, "%Y-%m-%d %H:%M:%S-08:00").strftime("%A %B %d %Y")
    except Exception:
        return ("Error parsing %s" % datestring)

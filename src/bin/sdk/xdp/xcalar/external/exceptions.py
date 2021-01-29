class XDPException(Exception):
    def __init__(self, statusCode, message):
        self.statusCode = statusCode
        self.message = message


class NoSuchImdTableGroupException(Exception):
    def __init__(self, reason):
        self.reason = reason


class IMDGroupException(Exception):
    def __init__(self, reason):
        self.reason = reason


class IMDTableSchemaException(Exception):
    def __init__(self, reason):
        self.reason = reason


class ImdGroupMergeException(Exception):
    def __init__(self, reason):
        self.reason = reason


class ImdGroupSnapshotException(Exception):
    def __init__(self, reason):
        self.reason = reason


class ImdGroupRestoreException(Exception):
    def __init__(self, reason):
        self.reason = reason

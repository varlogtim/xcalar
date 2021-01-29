def parse_ascii_delimiter(s):
    """Parse single ASCII delimiter. Correctly parses escape
    sequences such as \x32, \x20, etc..
    """
    if s.startswith('\\') and len(s) > 1:
        s = s.encode("ascii").decode("unicode_escape")
    if len(s) > 1 or ord(s) > 128:
        raise ValueError("'{}' is not a valid "
                         "single ASCII character".format(s))
    return s


def parse_utf8_delimiter(s):
    """Parse single UTF-8 delimiter. Handles escape sequences and
    single characters. E.g., \u2566, ╦, \\u2566, etc...
    """
    if s.startswith('\\') and len(s) > 1:
        s = s.encode("utf-8").decode("unicode_escape")
    if len(s) > 1:
        raise ValueError("'{}' is not a valid "
                         "single UTF-8 character".format(s))
    return s

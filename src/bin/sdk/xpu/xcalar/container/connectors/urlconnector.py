import os
import urllib.request
import io
from .util import File
import logging
logger = logging.getLogger("xcalar")

class UrlConnector():
    def __init__(self):
        pass

    def _is_global(self):
        return self._node_id is None

    # Default file browser, returns a single directory with original path
    # URL browsers are implemented as import UDFS and utilized from a custom target using this target as base
    # Check solutions repo to review these UDFs
    def get_files(self, path, name_pattern, recursive):
        rb = []
        file_obj = File(
            path=path ,
            relPath=os.path.basename(path),
            isDir=True,
            size=1,
            mtime=0)
        rb.append(file_obj)
        return rb

    def open(self, fileobj, opts):
        url = fileobj
        if(fileobj[0] == "/"):
            url = fileobj[1:]
        handle = urllib.request.urlopen(url)
        reader = io.BufferedReader(handle, 10 * 2**20)
        return reader
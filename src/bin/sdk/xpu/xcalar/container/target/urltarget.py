
from xcalar.container.connectors.urlconnector import UrlConnector
import xcalar.container.target.base as target
@target.register(name = "Url Connector")

class UrlTarget(target.BaseTarget):
    """
    Enter a url pointing to a file to harvest remote data.
    """
    def __init__(self, name, path):
        super(UrlTarget, self).__init__(name)
        self.connector = UrlConnector() # connector object does the real job

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, mode):
        return self.connector.get_files(path, name_pattern, recursive)

    def open(self, fileobj, opts):
        return self.connector.open(fileobj, opts)



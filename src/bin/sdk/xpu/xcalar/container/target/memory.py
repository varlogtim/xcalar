from xcalar.container.connectors.memory import MemoryConnector
import xcalar.container.target.base as target


@target.register(name="Generated")
class MemoryTarget(target.BaseTarget):
    """
    Generated dataset with no backing store. The 'path' for this target
    should be the number of rows desired in the dataset.
    """

    def __init__(self, name, path, **kwargs):
        super(MemoryTarget, self).__init__(name)
        self.connector = MemoryConnector(path, **kwargs)
        self.path = path

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, **user_args):
        return self.connector.get_files(path, name_pattern, recursive)

    def open(self, path, opts):
        return self.connector.open(path, opts)

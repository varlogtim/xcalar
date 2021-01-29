from ..base import Loader

# TODO: Write this


class LegacyLoader(Loader):
    def __init__(self, sources, sample_size, parser_args, xlr_root):
        super(LegacyLoader, self).__init__()

        self.sources = sources
        self.master_file_list = []
        self.local_file_list = []
        self.parser_func

        self.validate_arguments()
        return

    def validate_arguments(self):
        # Confirm that our arguments are as we'd expect. (Uneeded for legacy?)
        self.set_user_defined_function()    # ???
        return

    def set_master_file_list(self):
        # Get the list of all files to load.
        # This needs to handle node affinity as well
        self.sources.target.list_files()
        return

    def set_user_defined_function(self):
        # In the legacy load, the UDF is expected to be a parser which
        # yields disctionaries.
        # 1. find the function
        # 2. load the function
        # 3. validate is a generator.
        # ... there might be further abstractions to make regarding
        # true user defined and system parsers.
        self.parser_func = "the_function"

    def main(self):
        self.initialize_targets()
        self.get_master_file_list()
        self.distribute_files()
        self.load_local()
        return

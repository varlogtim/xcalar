# Set up buffer cache
class XCEDataset(object):
    def __init__(self, xce_client, batch_size=10):
        self.xce_client = xce_client
        self.num_files = 0
        self.num_errors = 0
        self.batch_size = batch_size
        self.empty_buffers = []
        self.full_buffers = []
        self._page_size = None
        self._fixed_schema_page = False

        # file/progress tracking
        self.all_files_num_bytes = 0
        self.cur_file = None
        self.cur_expected_file_size = None
        self.cur_file_num_bytes = None

    def set_schema_as_fixed(self):
        self._fixed_schema_page = True

    def page_size(self):
        if self._page_size is None:
            self._page_size = self.xce_client.get_xdb_page_size()
        return self._page_size

    def get_buffer(self):
        if len(self.empty_buffers) == 0:
            self.empty_buffers = self.xce_client.get_output_buffers(
                self.batch_size)
        return self.empty_buffers.pop()

    def finish_buffer(self, buf, is_error):
        self.full_buffers.append((buf, is_error))

        if len(self.full_buffers) == self.batch_size:
            # Our batch of output files is complete; let XCE know
            self._sync_file()

            self.xce_client.load_output_buffers(
                self.num_files, self.all_files_num_bytes, self.num_errors,
                self.full_buffers, [], self._fixed_schema_page)
            self.full_buffers = []
            self.all_files_num_bytes = 0
            self.num_files = 0
            self.num_errors = 0

    def flush_buffers(self):
        # if we're doing a hard flush, we want to clear our cache of empty buffers
        if self.empty_buffers or self.full_buffers:
            self.xce_client.load_output_buffers(
                self.num_files, self.all_files_num_bytes, self.num_errors,
                self.full_buffers, self.empty_buffers, self._fixed_schema_page)
            self.full_buffers = []
            self.empty_buffers = []
            self.all_files_num_bytes = 0
            self.num_files = 0
            self.num_errors = 0

    def report_error(self, record_num, filename, err):
        self.xce_client.report_file_error(
            filename, "record: {}, {}".format(record_num, err))
        self.num_errors += 1

    def _sync_file(self):
        old_bytes = self.cur_file_num_bytes
        try:
            # we don't allow the file to update progress more than 100%
            self.cur_file_num_bytes = min(self.cur_expected_file_size,
                                          self.cur_file.tell())
        except Exception:
            # tell() can throw many potential exceptions; rather than try to figure out
            # all possible exceptions, we just catch them all here
            pass
        this_file_incremental = self.cur_file_num_bytes - old_bytes
        self.all_files_num_bytes += this_file_incremental

    def finish_file(self):
        old_bytes = self.cur_file_num_bytes
        self.cur_file_num_bytes = self.cur_expected_file_size
        this_file_incremental = self.cur_file_num_bytes - old_bytes
        self.all_files_num_bytes += this_file_incremental

        self.cur_file = None
        self.cur_expected_file_size = None
        self.last_file_size = None

    def start_file(self, fileobj, file_size):
        self.num_files += 1

        self.cur_file = fileobj
        self.cur_expected_file_size = file_size
        self.cur_file_num_bytes = 0

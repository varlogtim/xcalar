import xcalar.container.parent as xce
import xcalar.container.context as ctx
import xcalar.container.xpu_host as xpu_host

from xcalar.compute.util.utils import get_proto_field_value

from xcalar.compute.coretypes.DataFormatEnums.ttypes import DfFormatTypeT

# Set up logging
logger = ctx.get_logger()


class Table:
    def __init__(self, xdb_id, columns):
        self._client = xce.Client()
        self._xdb_id = xdb_id
        self._columns = columns
        self._column_names = [c["columnName"] for c in self._columns]
        self._column_aliases = [c["headerAlias"] for c in self._columns]
        self._priv_meta = None
        self._schema = None

    @property
    def _meta(self):
        if self._priv_meta is None:
            self._priv_meta = self._client.xdb_get_meta(self._xdb_id)
        return self._priv_meta

    @property
    def schema(self):
        if self._schema is None:
            col_name_dict = {
                c["columnName"]: c["headerAlias"]
                for c in self._columns
            }
            self._schema = [{
                'name': col_name_dict[meta_col.name],
                'type': meta_col.type
            } for meta_col in self._meta.columns
                            if meta_col.name in col_name_dict]
        return self._schema

    # XXX we probably want to add more metadata here and make this return
    # some class instead of dictionaries
    def columns(self):
        """Get the field names and requested headers"""
        return self._columns

    def all_rows(self,
                 buf_size=1000,
                 format=DfFormatTypeT.DfFormatJson,
                 **kwargs):
        """Get a cursor across all rows in the entire table.

        Cursors across all rows in the table across the whole cluster. This
        will fetch data from other nodes as needed.

        kwargs are used to control format arguments and can usually be ignored.
        """
        num_nodes = ctx.get_node_count()
        for node_id in range(num_nodes):
            yield from self._node_cursor(node_id, buf_size, format, **kwargs)

    def local_node_rows(self,
                        buf_size=1000,
                        format=DfFormatTypeT.DfFormatJson,
                        **kwargs):
        """Get a cursor across the rows on the locally associated usrnode.

        Cursors across the table rows that exist on the usrnode which hosts the
        running XPU. This will not go across the network to other nodes.

        kwargs are used to control format arguments and can usually be ignored.
        """
        xpu_id = ctx.get_xpu_id()
        node_id = ctx.get_node_id(xpu_id)
        return self._node_cursor(node_id, buf_size, format, **kwargs)

    def _partitioned_row_start_count(self):
        """Get xpu start index and number of rows it gets from the index
           position. This is used to get the partitioned cusror across the
           rows of the local usrnode."""
        xpu_id = ctx.get_xpu_id()
        node_xpu_id = ctx.get_local_xpu_id(xpu_id)    # id within this node
        node_id = ctx.get_node_id(xpu_id)
        node_start = self._node_start_row(node_id)
        node_rows = self._node_num_rows(node_id)
        node_xpus = ctx.get_num_xpus_per_node()[node_id]

        # XPUs will either have (min_rows_per_xpu) or (min_rows_per_xpu + 1)
        # rows. This ensures fairness; a different algorithm might end up with
        # an XPU getting much fewer rows, which would be undesirable.
        # num_bonus_groups is how many have (... + 1) rows
        min_rows_per_xpu = node_rows // node_xpus
        num_bonus_groups = node_rows % node_xpus

        my_num_rows = min_rows_per_xpu + (1 if node_xpu_id < num_bonus_groups
                                          else 0)
        my_start_row = (node_start + min(node_xpu_id, num_bonus_groups) +
                        node_xpu_id * min_rows_per_xpu)
        return (my_start_row, my_num_rows)

    def partitioned_row_count(self):
        """Returns the partitioned row count of a xpu"""
        return self._partitioned_row_start_count()[1]

    def partitioned_rows(self,
                         buf_size=100,
                         format=DfFormatTypeT.DfFormatJson,
                         **kwargs):
        """Get a partitioned cursor across the rows of the local usrnode.

        Cursors across a continuous subset of the table rows that exist on the
        usrnode which hosts the running XPU. This continuous subset is
        specifically chosen such that all XPUs running as a part of this App
        will collectively cursor the entire table. This will not go across the
        network to other nodes.

        kwargs are used to control format arguments and can usually be ignored.
        """
        my_start_row, my_num_rows = self._partitioned_row_start_count()
        return self._local_cursor(my_start_row, my_num_rows, buf_size, format,
                                  **kwargs)

    def _node_for_row(self, row_num):
        """Get the node id for a given row; row_num must be within the table"""
        cur_sum = 0
        for node_id, num_rows in enumerate(self._meta.numRowsPerNode):
            new_sum = cur_sum + num_rows
            if cur_sum <= row_num and row_num < new_sum:
                return node_id
            cur_sum = new_sum
        raise ValueError("invalid row_num {}; table only has {} rows".format(
            row_num, cur_sum))

    def _node_start_row(self, node_id):
        """Get the row number of the first row on a node"""
        return sum(self._meta.numRowsPerNode[:node_id])

    def _node_num_rows(self, node_id):
        return self._meta.numRowsPerNode[node_id]

    def _node_cursor(self, node_id, buf_size, format, **kwargs):
        """Cursor across all the rows of a given node"""
        node_start = self._node_start_row(node_id)
        num_rows = self._node_num_rows(node_id)

        return self._local_cursor(node_start, num_rows, buf_size, format,
                                  **kwargs)

    def _local_cursor(self, start_row, num_rows, buf_size, format, **kwargs):
        """Cursor across some subset of rows within a given node"""
        if num_rows == 0:
            return

        if format not in (DfFormatTypeT.DfFormatJson,
                          DfFormatTypeT.DfFormatInternal,
                          DfFormatTypeT.DfFormatCsv):
            raise ValueError("{} is not a valid cursor format".format(
                DfFormatTypeT._VALUES_TO_NAMES.get(format, format)))

        if format == DfFormatTypeT.DfFormatJson:
            if kwargs != {}:
                raise ValueError("unrecognized arguments: {}".format(
                    kwargs.keys()))
        # currently the snapshot format is same as csv internally
        # this can be changed when we have native parquet export support
        elif format == DfFormatTypeT.DfFormatCsv or format == DfFormatTypeT.DfFormatInternal:
            expected_args = {"field_delim", "record_delim", "quote_delim"}
            if set(kwargs.keys()) != expected_args:
                raise ValueError(
                    "csv formatted cursor needs arguments: {}".format(
                        list(expected_args)))

        # We want to have tight control over when we use the network. In the
        # spirit of this, we should disallow this function from being used for
        # cursoring across multiple nodes
        node_id = self._node_for_row(start_row)
        if node_id != self._node_for_row(start_row + num_rows - 1):
            raise ValueError("cursor cannot span across nodes")

        node_start = self._node_start_row(node_id)

        rows_generated = 0
        while rows_generated < num_rows:
            this_num_rows = min(num_rows - rows_generated, buf_size)

            # The offset is given per-node, so let's take care of that here
            node_offset = start_row - node_start + rows_generated

            # We only fetch the data as a datapage if it is in CSV format.
            # In the future we might want to always fetch it as a datapage
            # and convert it differently based on format
            as_data_page = format == DfFormatTypeT.DfFormatCsv or format == DfFormatTypeT.DfFormatInternal
            logger.debug("as data page: {}, format: {}".format(
                as_data_page, format))

            out = self._client.xdb_get_local_rows(
                self._xdb_id, self._column_names, this_num_rows, node_id,
                node_offset, as_data_page)
            # Convert the return into python objects (in a stream)
            if as_data_page:
                records = xpu_host.parse_data_page_to_csv(
                    self._column_names, out.dataPage, format,
                    kwargs["field_delim"], kwargs["record_delim"],
                    kwargs["quote_delim"])
            elif format == DfFormatTypeT.DfFormatJson:
                records = [
                    _convert_row_to_dict(self._column_names, row,
                                         self._column_aliases)
                    for row in out.rows.rows
                ]
            else:
                assert False
            yield from records
            assert (len(records) > 0
                    )    # Otherwise we'll get stuck in an infinite loop
            rows_generated += len(records)


def _convert_row_to_dict(column_names, row, selected_columns):
    values = [get_proto_field_value(row.fields[col]) for col in column_names]

    return dict(zip(selected_columns, values))

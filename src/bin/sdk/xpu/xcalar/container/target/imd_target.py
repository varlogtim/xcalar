# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import os
import hashlib
import logging

import xcalar.container.target.base as target
from xcalar.container.target.sharednothingsymm import UnsharedSymmetricTarget
from xcalar.compute.util.utils import FileUtil

logger = logging.getLogger("xcalar")


@target.register(name="Imd Target")
@target.param("mountpoint", "Local path to sharded directory")
class ImdTarget(UnsharedSymmetricTarget):
    """
    Second order connector for imd snapshots/deltas.
    In the backingTargetName field, select an existing connector that
    specifies the location of the imd snapshots/deltas.
    """

    def __init__(self, name, path, **kwargs):
        super(ImdTarget, self).__init__(name, path, **kwargs)

    def open(self, path, opts):
        data_file_path = self._relative_to_abs(path)
        _, ext = os.path.splitext(data_file_path)
        if opts == 'rb' and ext != '.checksum':
            self._validate_file_checksum(data_file_path, opts)
        return self.connector.open(data_file_path, opts)

    def _validate_file_checksum(self, file_path, opts):
        # checksum file expected to be present in the same snapshot file path
        chunk_size = 4096
        base_path, ext = os.path.splitext(file_path)
        checksum_path = base_path + '.checksum'
        checksum = None
        try:
            with self.connector.open(checksum_path, opts) as fp:
                checksum = fp.read().decode("utf-8")
        except FileNotFoundError as ex:
            raise ValueError(
                "Associated checksum file of the data file {} doesn't exists".
                format(file_path))

        data_checksum = None
        data_buf = None
        m = hashlib.md5()
        with self.connector.open(file_path, opts) as base_fp:
            # expecting 0 or 1 level of packing here
            for outfile, _ in FileUtil.unpack_file(file_path, base_fp, 0):
                while True:
                    # read data in chunks
                    data_buf = outfile.read(chunk_size)
                    if not data_buf:
                        break
                    m.update(data_buf)
        data_checksum = m.hexdigest()
        if checksum != data_checksum:
            raise ValueError(
                "Invalid checksum of the file {}".format(file_path))

#!/usr/bin/env python3

# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import tarfile
import io
import os
import gzip
import json
import sys
import getpass
import base64
import argparse
import fnmatch

from uuid import uuid4
from xcalar.external.client import Client
from xcalar.compute.util.crc32c import crc32

# Unique mark to temporarily replace "--".
dash_mark = uuid4().bytes.hex()
max_line_width = 79

# This tool has been tested and verified to work only for version 1 and 2
# workbooks. For larger versioned workbooks, the tool will at least need to be
# re-tested, and in addition, possibly need to be extended for the newer
# version(s).

# This tool's use of workbook versions should match the versions enumerated in
# C++ code src/include/session/Sessions.h
workbook_version_v1 = 1
workbook_version_v2 = 2
workbook_version_v3 = 3
workbook_version_v4 = 4
workbook_version = workbook_version_v4

note = """
The tool could have life beyond Dionysus, until workbooks are robust enough, or
XD can repair workbooks (probably by calling this tool).

Fixing "--" in the column names is only for pre-Chronos workbooks. This
functionality can be deleted once there are no pre-Chronos workbooks or
dataflows left to be fixed.

For online mode, the tool must be executed only by a privileged user, since the
tool can be used to fixup any user's workbooks, and upload them into that
user's workbook list in an online cluster.

There is a pytest function included to test the script.
"""


class FixWorkbook:
    def __init__(self,
                 username=None,
                 password=None,
                 local_path=None,
                 workbook_name=None,
                 udf_path=None,
                 suffix=None,
                 module_name=None,
                 replace=None,
                 recursive=None,
                 output_path="./",
                 silent=False):
        self.username = username
        self.password = password
        self.client_secret_file = "/tmp/xcfixwb-secrets.json"
        self.local_path = local_path
        self.workbook_names = set(
            workbook_name.split(',')) if workbook_name is not None else None
        self.udf_path = udf_path
        self.suffix = suffix if suffix is not None else "_fixed"
        # This is the name of the module to be extracted, but not necessarily
        # the correct name.
        self.module_name = module_name
        self.string_to_dash = replace
        self.dash = True if replace is not None else False
        self.bdf_recursive = recursive
        self.output_path = output_path
        self.silent = silent

    def fix_workbook(self):
        # Online Mode
        if self.workbook_names is not None:
            self.session_name_set = None
            with open(self.client_secret_file, "w") as f:
                secret = {
                    "xiusername": self.username,
                    "xipassword": self.password
                }
                f.write(json.dumps(secret))
            self.client = Client(client_secrets_file=self.client_secret_file)

            # Fix the workbook.
            if self.udf_path is not None or self.dash:
                self.workbook_names = self.wild_card_list_matching(
                    self.get_session_name_set(), self.workbook_names)
                for workbook_one in self.workbook_names:
                    self.fix_online(workbook_one)
            # Extract a module.
            elif self.module_name is not None:
                if len(self.workbook_names) > 1:
                    print("Only one workbook allowed, more than one matched")
                    sys.exit(1)
                self.extract_online(self.workbook_names.pop(),
                                    self.module_name)

        # Local Mode
        elif self.local_path is not None:
            # Create a filename list.
            # We don't do wildcard matching for directories.
            # But it's OK if wildcard matching only generates one folder.
            local_file_list = None
            if len(self.local_path) == 1 and os.path.isdir(self.local_path[0]):
                local_file_list = [
                    os.path.join(self.local_path[0], f)
                    for f in os.listdir(self.local_path[0])
                    if os.path.isfile(os.path.join(self.local_path[0], f))
                ]
            else:
                local_file_list = [
                    f for f in self.local_path if os.path.isfile(f)
                ]

            # Fix the workbook.
            if self.module_name is None:
                for local_file in local_file_list:
                    if local_file[-7:] != ".tar.gz":
                        continue
                    with open(local_file, "rb") as f:
                        self.fix_local(f.read(), True, local_file)
            # Extract a module.
            else:
                for local_file in local_file_list:
                    if local_file[-7:] != ".tar.gz":
                        continue
                    with open(local_file, "rb") as f:
                        self.extract_local(f.read(), self.module_name)
                    break

    # This function is called in both online mode and local mode.
    def extract_local(self, compressed_workbook, module_name):
        print("Uncompressing compressed workbook tar.gz file")
        uncomp_bytes = gzip.GzipFile(
            fileobj=io.BytesIO(compressed_workbook), mode='rb')
        uncomp_bytes.seek(0)
        old_file = tarfile.open(fileobj=uncomp_bytes, mode="r")

        tf_list = old_file.getmembers()
        for tmemb in tf_list:
            if tmemb.name == "workbook/udfs/" + self.module_name + ".py":
                ex = old_file.extractfile(tmemb)
                ex.seek(0)
                with open("./" + self.module_name + ".py", "wb") as f:
                    f.write(ex.read())
                break

    def extract_online(self, workbook_one, module_name):
        print("Downloading workbook {}".format(workbook_one))
        downloaded_workbook = self.client.get_workbook(workbook_one).download()
        self.extract_local(downloaded_workbook, module_name)

    # This function is called in both online mode and local mode.
    def fix_local(self, compressed_workbook, is_local, local_file=None):

        # Notes of adding a new UDF.
        #
        # The addition of the new UDF module to the downloaded workbook is done
        # in 4 steps:
        # 0. first need to uncompress the downloaded tar.gz (since there's no
        #    append mode in TarFile class for :gz - i.e. no "a:gz" mode)
        # 1. append the new TarInfo object to the uncompressed tar file
        # 2. re-compress the tar file back to compressed layout
        # 3. prep the upload bytes to be sent to the upload API
        #
        # After the new UDF is appended to oldFile, the old file will be copied
        # to a new tarFile, member by member, so that the header can be modified
        # when the header memeber is encountered during the copy. A copy is done
        # since there's no support in the tarfile class to do member deletion
        # (open bug - see https://bugs.python.org/issue6818). Otherwise one
        # could delete the header and append a new header and UDF directly to
        # the downloaded tar instead of creating a new tarfile into which the
        # downloaded tar is copied, doing the mods along the way.

        compressed_tar = io.BytesIO(compressed_workbook)
        uncomp_bytes = gzip.GzipFile(fileobj=compressed_tar, mode='rb')
        uncomp_bytes.seek(0)

        # Create a bytes object for the new tarfile (which will receive the
        # modification for the new UDF)
        new_uncomp_bytes = io.BytesIO()
        new_uncomp_bytes.seek(0)

        # Uncomment to dump the compressed workbook .tar in current dir.
        # Useful for diagnosis if something goes wrong
        # with open("./download.tar.gz", "wb") as f:
        #     f.write(compressedWb)

        old_file = tarfile.open(fileobj=uncomp_bytes, mode="r")
        tf_list = old_file.getmembers()

        # workbook.tar.gz and dataflow.tar.gz have different root folder
        # structures.
        # workbook.tar.gz: /workbook/udfs
        # dataflow.tar.gz: /udfs
        # I believe it would be better if we make them consistent in the
        # backend.
        tar_prefix = self.longest_common_prefix(
            [tmemb.name for tmemb in tf_list if tmemb.isfile()])
        # If tar_prefix is "foo/bar/a", we only want "foo/"
        for i, j in enumerate(tar_prefix):
            if j == "/":
                tar_prefix = tar_prefix[0:i + 1]
                break
        # If files are "foo1.json", "foo2.json", "foo3.json"
        # Common prefix will be "foo", tar_prefix should be "" instead.
        if tar_prefix != "" and tar_prefix[len(tar_prefix) - 1] != "/":
            tar_prefix = ""

        # Open new tar in write mode since it's being created for first time.
        new_file = tarfile.open(fileobj=new_uncomp_bytes, mode="w")

        if tar_prefix != "":
            print("Adding the compressed workbook file into a new {}".format(
                tar_prefix[:-1]))
        new_mod_info = None
        # Add or replace the module.
        # When the script is only fixing dashes, self.source_path is None.
        if self.udf_path is not None:
            with open(self.udf_path, "rb") as new_mod_f_obj_bytes:
                source_name = os.path.basename(self.udf_path)
                new_mod_info = tar_prefix + "udfs/" + source_name
                self.bytes_to_tar(new_file, new_mod_info,
                                  new_mod_f_obj_bytes.read())

        # Now copy each member from old_file to new_file.
        workbook_header = None
        dataflow_info = None
        is_replacement = False
        for tmemb in tf_list:
            # Header found, save for later use.
            if tmemb.name == tar_prefix + "workbookHeader.json":
                workbook_header = old_file.extractfile(tmemb)
            elif tmemb.name == tar_prefix + "dataflowInfo.json":
                dataflow_info = old_file.extractfile(tmemb)
            # Same module found, which means it's replacement.
            elif tmemb.name == new_mod_info:
                is_replacement = True
            # Found a json file, may need to fix retina or fix dash.
            elif (tmemb.name == tar_prefix + "qgraphInfo.json"
                  or tmemb.name == tar_prefix + "kvstoreInfo.json"):

                ex = old_file.extractfile(tmemb)
                dataflow_json = json.load(ex)

                # If fix retinaBuf.
                if self.bdf_recursive:
                    self.fix_retina(dataflow_json)

                encoded = json.dumps(dataflow_json, indent=4).encode('utf-8')

                # If need to fix dash.
                if self.dash:
                    encoded = self.fix_dash(tmemb.name, encoded)

                # Add to the new tar file.
                self.bytes_to_tar(new_file, tmemb.name, encoded)
            else:
                # Add to the new tar file.
                ex = old_file.extractfile(tmemb)
                new_file.addfile(tmemb, ex)

        has_checksum = False
        # Fix header.
        # When it's fixing Retina, workbook_header is None.
        if workbook_header is not None:
            hdr_json = json.load(workbook_header)
            if (hdr_json["workbookVersion"] == 0
                    or hdr_json["workbookVersion"] > workbook_version):
                print("Invalid workbook version")
                sys.exit(1)
            if hdr_json["workbookVersion"] >= workbook_version_v2:
                has_checksum = True
            if "workBookHealthStatus" in hdr_json:
                del hdr_json["workBookHealthStatus"]
            # Mark the workbook as repaired when,
            # 1. Fixing UDFs.
            # 2. Fixing dashes.
            # 3. Checksum error.
            if (self.udf_path is not None or self.dash or
                (has_checksum and not self.verify_checksum(uncomp_bytes))):
                hdr_json['workBookHealth'] = 'Repaired'
            # If it's addition.
            # When no source path provided and the script is only fixing
            # dashes, is_replacement is False, but new_mod_info is None.
            if not is_replacement and new_mod_info is not None:
                # new_mod_hdr_list has the new UDF section with the new UDF
                # name
                new_mod_hdr_list = [{
                    'fileName': new_mod_info,
                    'moduleName': source_name[:-3],
                    'udfType': 'python'
                }]
                # Append new UDF section in header
                hdr_json['udfs'] = hdr_json['udfs'] + new_mod_hdr_list

            # Dump the json to string, and encode it, so that it's now bytes.
            encoded = json.dumps(hdr_json, indent=4).encode('utf-8')
            # Add to the new tar file.
            self.bytes_to_tar(new_file, tar_prefix + "workbookHeader.json",
                              encoded)

        if dataflow_info is not None:
            hdr_json = json.load(dataflow_info)
            if not is_replacement and new_mod_info is not None:
                new_mod_hdr_list = [{
                    'fileName': new_mod_info,
                    'moduleName': source_name[:-3],
                    'udfType': 'python'
                }]
                hdr_json['udfs'] = hdr_json['udfs'] + new_mod_hdr_list

            encoded = json.dumps(hdr_json, indent=4).encode('utf-8')

            if self.dash:
                encoded = self.fix_dash("dataflowInfo.json", encoded)

            self.bytes_to_tar(new_file, tar_prefix + "dataflowInfo.json",
                              encoded)

        # Now close both and re-open the new tar in read mode for compression
        # as prep for upload (which needs a compressed tar)
        old_file.close()
        new_file.close()

        if has_checksum:
            new_uncomp_bytes = self.fix_checksum(new_uncomp_bytes)

        new_uncomp_bytes.seek(0)
        new_file = tarfile.open(fileobj=new_uncomp_bytes, mode="r")

        # Compress the new tarfile so it's in .tar.gz format
        new_uncomp_bytes.seek(0)
        new_comp_bytes = io.BytesIO()
        new_comp_bytes.write(gzip.compress(new_uncomp_bytes.read()))
        new_comp_bytes.seek(0)
        # Convert from a streaming BytesIO object to binary data by reading the
        # entire payload from the stream, into uploadWnewUDF which will be
        # uploaded
        upload_w_new_udf = new_comp_bytes.read()

        # Local mode, save to local file.
        if is_local:
            new_comp_bytes.seek(0)
            new_local_file = os.path.basename(local_file)[:-7] + self.suffix
            self.bytes_to_local(new_local_file, ".tar.gz",
                                new_comp_bytes.read())
        # Online mode, return bytes to online function.
        else:
            return upload_w_new_udf

    def bytes_to_tar(self, tar_file_object, tar_filename, tar_file_bytes):
        tar_file_bytes_io = io.BytesIO(tar_file_bytes)
        tar_info = tarfile.TarInfo(name=tar_filename)
        tar_file_bytes_io.seek(0, io.SEEK_END)
        tar_info.size = tar_file_bytes_io.tell()
        tar_file_bytes_io.seek(0, io.SEEK_SET)
        tar_file_object.addfile(tar_info, tar_file_bytes_io)

    def bytes_to_local(self, filename, file_extension, file_bytes):
        # List local files, avoid using an existing filename.
        local_file_list = [
            f for f in os.listdir(self.output_path)
            if os.path.isfile(os.path.join(self.output_path, f))
        ]
        local_file_set = frozenset(local_file_list)
        # Try to use a different filename.
        if filename + file_extension in local_file_set:
            suffix_num = 1
            while (filename + str(suffix_num) +
                   file_extension in local_file_set):
                suffix_num += 1
            filename += str(suffix_num)
        filename = os.path.join(self.output_path, filename + file_extension)
        with open(filename, "wb") as f:
            f.write(file_bytes)

    def longest_common_prefix(self, string_list):
        if len(string_list) == 0:
            return ""

        shortest_string = min(string_list, key=len)
        shortest_len = len(shortest_string)

        for s in string_list:
            for i in range(shortest_len):
                if (s[i] != shortest_string[i]):
                    shortest_len = i
                    break

        return shortest_string[:shortest_len]

    def wild_card_list_matching(self, source_list, string_list):
        return frozenset(
            source for source in source_list for s in string_list
            if fnmatch.fnmatch(source, s))

    def fix_retina(self, dataflow_json):
        for i, query in enumerate(dataflow_json):
            # If it has retinaBuf.
            if isinstance(
                    query,
                    dict) and query["operation"] == "XcalarApiExecuteRetina":
                if not self.silent:
                    stdin = input(
                        "Do you want to fix batch dataflow {}? [y/N] ".format(
                            query["args"]["retinaName"]))
                if self.silent or stdin == "Y" or stdin == "y":
                    query_bytes = base64.b64decode(query["args"]["retinaBuf"])
                    # Recursively fix retina, both dashes and missing udf.
                    query_bytes_fixed = self.fix_local(query_bytes, False)
                    query_str = base64.b64encode(query_bytes_fixed).decode(
                        'utf-8')
                    query["args"]["retinaBuf"] = query_str
                    query["args"]["retinaBufSize"] = len(query_str)
                    dataflow_json[i] = query

    def fix_dash(self, filename, encoded):
        print("Changed lines in {}:".format(filename))
        for line in encoded.decode().splitlines():
            if self.string_to_dash in line:
                pos = line.find(self.string_to_dash)
                begin = pos - max_line_width // 2
                begin = begin if begin >= 0 else 0
                end = pos + (max_line_width + 1) // 2
                end = end if end <= len(line) else len(line)
                print(line[begin:end])
        return encoded.replace(self.string_to_dash.encode(), b"-")

    def verify_checksum(self, uncomp_bytes):
        uncomp_bytes.seek(0)
        with tarfile.open(fileobj=uncomp_bytes, mode="r") as old_tar:
            if "workbook/workbookChecksum.json" not in old_tar.getnames():
                print("Checksum file not found. Checksum will be regenerated")
                return False

            ex_checksum = old_tar.extractfile("workbook/workbookChecksum.json")
            checksum_json = json.load(ex_checksum)

            tf_list = old_tar.getmembers()
            for tmemb in tf_list:
                if tmemb.name == "workbook/workbookChecksum.json":
                    continue
                else:
                    ex = old_tar.extractfile(tmemb)
                    # Not a directory
                    if ex is not None:
                        checksum = "{:0>8}".format(hex(crc32(ex))[2:])
                        if not (tmemb.name in checksum_json
                                and checksum_json[tmemb.name] == checksum):
                            print(
                                "Checksum mismatch: {}. Checksum will be regenerated"
                                .format(tmemb.name))
                            return False

        return True

    def fix_checksum(self, uncomp_bytes):
        uncomp_bytes.seek(0)
        new_uncomp_bytes = io.BytesIO()
        with tarfile.open(fileobj=uncomp_bytes, mode="r") as old_tar:
            with tarfile.open(fileobj=new_uncomp_bytes, mode="w") as new_tar:
                checksum = {}
                tf_list = old_tar.getmembers()
                for tmemb in tf_list:
                    if tmemb.name == "workbook/workbookChecksum.json":
                        continue
                    else:
                        ex = old_tar.extractfile(tmemb)
                        # Not a directory
                        if ex is not None:
                            checksum[tmemb.name] = "{:0>8}".format(
                                hex(crc32(ex))[2:])
                            ex.seek(0)
                        new_tar.addfile(tmemb, ex)
                encoded = json.dumps(checksum, indent=4).encode()
                self.bytes_to_tar(new_tar, "workbook/workbookChecksum.json",
                                  encoded)
        return new_uncomp_bytes

    # Online mode.
    # Download the workbook, fix the workbook, and then upload the updated
    # workbook file with a new name - creating a newly uploaded workbook.
    def fix_online(self, workbook_one):
        print("Downloading workbook {}".format(workbook_one))
        downloaded_wb = self.client.get_workbook(workbook_one).download()
        upload_w_new_udf = self.fix_local(downloaded_wb, False)

        # Uncomment to save file.
        # with open("./test.tar.gz", "wb") as f:
        #     f.write(upload_w_new_udf)

        upload_name = workbook_one + self.suffix
        # Avoid using an existing workbook name.
        # Rename if necessary.
        if upload_name in self.get_session_name_set():
            suffix_num = 1
            while upload_name + str(suffix_num) in self.get_session_name_set():
                suffix_num += 1
            upload_name += str(suffix_num)
        self.session_name_set.add(upload_name)
        # Do the upload which will create a new workbook with the supplied name
        upload_workbook = self.client.upload_workbook(upload_name,
                                                      upload_w_new_udf)

        print("Uploaded workbook with new UDF has name {}\n".format(
            upload_workbook.name))

    def get_session_name_set(self):
        if self.session_name_set is None:
            session_name_list = self.client.list_workbooks()
            # Listing sessions is slow. Use a set to keep the list.
            self.session_name_set = set(session_name_list[i].name
                                        for i in range(len(session_name_list)))
        return self.session_name_set


# The command line interface is going to be deprecated.
# Use xc2 workbook repair suite instead.

description = """
This tool fixes workbooks and other tar.gz files with a similar file structure,
like dataflows, published tables. When a local path is provided, it fixes the
local tar.gz files (workbooks, dataflows, published tables) in the path. When
workbook names are provided (for workbooks only), it fixes workbooks online, by
downloading the workbooks, fixing them, and uploading the new fixed workbooks.
Wildcard matching is supported for both local paths and workbook names online.

Also, it can fix the embedded batch dataflow in workbooks, when required
(with "-r"). Use this with caution, and only if you know precisely what impact
this will have on the embedded batch dataflow. Otherwise it may break the
embedded retina's execution. It will still ask you before fixing a batch
dataflow, in case you don't want to fix all of them. And you can use this
feature get different embedded batch dataflows fixed differently by running the
tool multiple times, with different embedded batch dataflows picked each time.

Modes:
Online Mode: -u <username> -w <workbook1,workbook2,...>
Local Mode: -l <local workbook/dataflow/published table path>

Functionalities:
1. Extract a python module from a workbook online or in the local path. ("-x")
2. Fix UDFs. ("-p") When UDFs are missing, workbooks cannot be activated.
3. Fix "--" in the column names. ("-d") This is a global search and replace,
use with caution. All changed lines will be printed for verification.
4. Fix checksums for version 2 workbooks. The tool will always generate correct
checksums for version 2 workbooks. It will report checksum errors (missing,
mismatch) of the original workbook.
"""


def get_args(argv):
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-u", default=getpass.getuser(), help="username")
    parser.add_argument("-o", default="admin", help="password")
    parser.add_argument("-w", help="workbook names separated by comma")
    parser.add_argument(
        "-l",
        nargs="+",
        help="path to the local workbook, can be a file or a folder")
    parser.add_argument(
        "-p", help="path to the module to be added or replaced")
    parser.add_argument("-s", help="suffix for the fixed workbooks")
    parser.add_argument("-d", help="string to be replaced by \"-\"")
    parser.add_argument("-x", help="name of the module to be extracted")
    parser.add_argument(
        "-r",
        action='store_true',
        help="fix execute-retina nodes inside workbooks recursively")

    # Argparse always thinks "--" is an argument. Replace "--" with something
    # else before giving it to argparse.
    args = parser.parse_args(
        map(lambda arg: arg if arg != "--" else dash_mark, argv))
    # Change dash_mark back to "--"
    args.d = args.d if args.d != dash_mark else "--"

    return args


def main():
    args = get_args(sys.argv[1:])
    if (args.w is None and args.l is None
            or args.p is None and args.d is None and args.x is None):
        args.print_help()
        sys.exit(1)
    fix_tool = FixWorkbook(args.u, args.o, args.l, args.w, args.p, args.s,
                           args.x, args.d, args.r)
    fix_tool.fix_workbook()


if __name__ == "__main__":
    main()

import json
import logging
import os
import io
import subprocess
import time
import uuid

from .util import File, match

# let the $PATH handle these
XC_MAPR_BIN_NAME = "xcMapRClient"
MAPR_LOGIN_BIN_NAME = "maprlogin"
MAPR_TICKET_ENV_VAR = "MAPR_TICKETFILE_LOCATION"
MAPR_PASSABLE_ERRORS = ("Connection reset by peer(104)",
                        "Connection timed out(110)")

mapr_ticket_directory = os.environ.get("XC_MAPR_TICKET_DIR",
                                       "/tmp/xcalar/maprTickets")

logger = logging.getLogger('xcalar')


class MapRAuth():
    def __init__(self, user, pw, cluster):
        ticket_file = "mapr-ticket-{}".format(uuid.uuid4().hex)
        self.ticket_loc = os.path.join(mapr_ticket_directory, ticket_file)
        self.cluster = cluster
        self.user = user
        self.pw = pw
        self.connected = False
        os.environ[MAPR_TICKET_ENV_VAR] = self.ticket_loc

    def connect(self):
        # Try to log in if we can
        os.makedirs(os.path.dirname(self.ticket_loc), exist_ok=True)
        try:
            self.login()
            self.connected = True
        except ValueError as e:
            logger.info(
                "Failed to connect to cluster '{}' as user '{}': '{}'".format(
                    self.cluster, self.user, str(e)))
            raise

    def disconnect(self):
        if self.connected:
            try:
                self.logout()
            except ValueError as e:
                logger.info(
                    "Failed to disconnect from cluster '{}' as '{}': '{}'".
                    format(self.cluster, self.user, str(e)))
                # Consider this error nonfatal
                pass
            try:
                # the logout command will usually remove this file, let's
                # try again just to be safe in case the logout failed
                os.remove(self.ticket_loc)
            except OSError as e:
                # This will typically throw an exception, so we can ignore it
                pass

    # Log in as the service user who is capable of impersonating other users
    def login(self):
        process_args = [
            MAPR_LOGIN_BIN_NAME,
            "password",
        ]
        if self.user:
            process_args.extend([
                "-user",
                self.user,
            ])

        if self.cluster:
            process_args.extend([
                "-cluster",
                self.cluster,
            ])

        pr = subprocess.Popen(
            process_args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        out, err = pr.communicate(input="{}\n".format(self.pw).encode("utf-8"))

        if pr.returncode != 0:
            raise RuntimeError(err.decode('utf-8'))

    def logout(self):
        process_args = [MAPR_LOGIN_BIN_NAME, "end"]

        pr = subprocess.Popen(
            process_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        out, err = pr.communicate()

        if pr.returncode != 0:
            raise ValueError(err.decode('utf-8'))


class MapRConnector():
    @staticmethod
    def is_available():
        # XXX Add some checks to see if we're even set up for MapR connections
        return True

    def __init__(self, user=None, port=None, cldb_node=None):
        self.client_args = []

        # XXX for now let's assume the cluster has been configured offline;
        # setting this would allow us to access a CLDB node which has not
        # been locally configured. (If the CLDB node is configured; it will
        # be accessed implicitly by the cluster name, rather than the CLDB
        # host name)
        # The explicit CLDB node might be used often in un-secured setups.
        # self.client_args.extend(["--cldbNode", cluster])

        if port:
            self.client_args.extend(["--port", port])
        if user:
            self.client_args.extend(["--user", user])
        if cldb_node:
            self.client_args.extend(["--cldbNode", cldb_node])

    def get_file_array(self, path, recursive):
        process_args = [
            XC_MAPR_BIN_NAME,
            "ls",
            "--path",
            path,
        ]
        if recursive:
            process_args.extend(["--recursive"])
        # Add the args for connection-related info (eg. port, user)
        process_args.extend(self.client_args)
        pr = subprocess.Popen(
            process_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        out, err = pr.communicate()

        if pr.returncode != 0:
            raise ValueError(err.decode('utf-8'))

        file_list = json.loads(out)
        return file_list

    def get_files(self, path, name_pattern, recursive):
        """
        List the visible files matching name_pattern. Should return a "File"
        namedtuple.
        Results are globally visible if 'is_global()' returns true
        """
        if name_pattern == "":
            name_pattern = "*"
        file_objs = []

        file_array = self.get_file_array(path, recursive)

        for f in file_array:
            relPath = os.path.relpath(f["mName"], path)
            if relPath == '.':
                relPath = os.path.basename(f["mName"])
            file_obj = File(
                path=f["mName"],
                relPath=relPath,
                isDir=f["mKind"] == "D",
                size=f["mSize"],
                mtime=int(f["mLastMod"]))
            if match(name_pattern, recursive, file_obj):
                file_objs.append(file_obj)
        return file_objs

    def open(self, path, opts):
        if "w" in opts:
            raise ValueError("Write files not implemented for MapR")

        # In order to seek, we need to know the file's size ahead of time.
        # Let's fetch that information first.
        file_objs = self.get_files(path, "*", False)
        if len(file_objs) == 0:
            raise ValueError("File '{}' not found".format(path))
        elif len(file_objs) > 1:
            raise ValueError(
                "Path '{}' has unexpectedly resolved to {} files: {}".format(
                    path, len(file_objs), [f.path for f in file_objs]))
        file_size = file_objs[0].size

        raw_file = self.MaprFile(path, self.client_args, file_size)
        buffered_file = io.BufferedReader(raw_file, 10 * 2**20)
        return buffered_file

    class MaprFile(io.RawIOBase):
        def __init__(self, path, client_args, size):
            self._path = path
            self._client_args = client_args
            self._size = size
            self._pos = 0

        def readable(self):
            return True

        def seekable(self):
            return True

        def writeable(self):
            return False

        def tell(self):
            return self._pos

        def seek(self, offset, whence=os.SEEK_SET):
            if whence == os.SEEK_CUR:
                new_pos = self._pos + offset
            elif whence == os.SEEK_END:
                new_pos = self._size + offset
            elif whence == os.SEEK_SET:
                new_pos = offset
            else:
                raise ValueError("Whence can only be 0, 1, or 2")

            if new_pos > self._size:
                raise ValueError("Cannot seek beyond end of file")

            if new_pos < 0:
                raise ValueError("Cannot seek before start of file")

            self._pos = new_pos

            return self._pos

        def readinto(self, b):
            chunk = self._read_bytes(len(b))
            b[:len(chunk)] = chunk
            return len(chunk)

        def _read_bytes(self, req_bytes):
            num_retries = 6
            remaining = self._size - self._pos

            num_bytes = max(0, min(remaining, req_bytes))

            # We might as well special case this before running the subprocess
            if not num_bytes:
                return b''

            process_args = [
                XC_MAPR_BIN_NAME,
                "read",
                "--path",
                self._path,
                "--offset",
                str(self._pos),
                "--numBytes",
                str(num_bytes),
            ]
            # Add the args for connection-related info (eg. port, user)
            process_args.extend(self._client_args)
            for try_num in range(num_retries):
                pr = subprocess.Popen(
                    process_args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE)

                data, err = pr.communicate()

                if pr.returncode != 0:
                    # Sometimes the connector will hit an error while reading
                    # lets wait a little bit and try again.
                    err_str = err.decode('utf-8')
                    if any(x in err_str for x in MAPR_PASSABLE_ERRORS
                           ) and try_num != num_retries - 1:
                        time.sleep(
                            2**try_num)    # exp backoff before we try again
                        continue
                    raise ValueError(err_str)
                break    # break from the retry loop
            # At this point we know that the last read succeeded with results in `data`

            self._pos += len(data)
            return data

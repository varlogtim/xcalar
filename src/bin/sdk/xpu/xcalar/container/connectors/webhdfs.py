import os
import re
import itertools
import time
import random
from subprocess import Popen, PIPE

from .util import File, match

KerberosAvailable = False
try:
    from hdfs.ext.kerberos import KerberosClient
    KerberosAvailable = True
except ImportError:
    pass

HdfsCliAvailable = False
try:
    from hdfs.client import Client
    HdfsCliAvailable = True
except ImportError:
    pass

# To create a keytab, refer to https://kb.iu.edu/d/aumh
# MIT Kerberos
#  > ktutil
#  ktutil:  addent -password -p username@ADS.IU.EDU -k 1 -e rc4-hmac
#  Password for username@ADS.IU.EDU: [enter your password]
#  ktutil:  addent -password -p username@ADS.IU.EDU -k 1 -e aes256-cts
#  Password for username@ADS.IU.EDU: [enter your password]
#  ktutil:  wkt username.keytab
#  ktutil:  quit
# Heimdal Kerberos
#  > ktutil -k username.keytab add -p username@ADS.IU.EDU -e arcfour-hmac-md5 -V 1
# Once the keytab has been created, add it to your config file
# WebHdfs.keytab=<user@realm>|<path-to-keytab-file>


class WebHDFSConnector():
    @staticmethod
    def is_available(kerberos=True):
        return HdfsCliAvailable and ((not kerberos) or KerberosAvailable)

    def __init__(
            self,
            nn,
            kerberos=False,
            keytab=None,
            target_name=None,    # this should never default
            config_path=None):
        if not HdfsCliAvailable:
            raise ImportError("No module named hdfs.client")

        if keytab is not None:
            num_retries = 6

            keytab_regex = r'^(.*@.*)\|(.*)$'
            match = re.match(keytab_regex, keytab)
            if match is None:
                raise ValueError("keytab must be in format: "
                                 "<user>@domain|/path/to/keytab")

            user_domain = "'{}'".format(match.group(1).replace("'", "'\\''"))
            keytab_file = "'{}'".format(match.group(2).replace("'", "'\\''"))

            klist_cmd = "klist -s"
            kinit_cmd = "kinit {} -k -t {}".format(user_domain, keytab_file)

            if config_path is not None:
                os.environ['KRB5_CONFIG'] = config_path

            # Set cache path for each target+user+domain combination
            (user, domain) = user_domain.split('@')
            cache_path = '/tmp/xcalar.kerberos.{}.{}.{}.cache'.format(
                target_name.replace(' ', '_'), user, domain)
            os.environ['KRB5CCNAME'] = cache_path

            # check whether we need to run kinit or not using klist.
            # Also retry just in case the errors from kinit are transient.
            for try_num in range(num_retries):
                ret = os.system(klist_cmd)
                if (ret != 0):
                    pr = Popen(kinit_cmd, stdout=PIPE, stderr=PIPE, shell=True)
                    out, err = pr.communicate()
                    ret = pr.returncode
                    if (ret != 0):
                        if try_num != num_retries - 1:
                            # sleep random amount of time so that all XPU's on a node
                            # dont end up trying kinit at the same time
                            time.sleep(random.random()**try_num)
                            continue
                        raise RuntimeError("kinit: [{}]: {}".format(
                            ret, err.decode('utf-8')))
                break    # break from the retry loop

        if kerberos and not KerberosAvailable:
            raise ImportError("No module named hdfs.ext.kerberos")

        if kerberos:
            self.client = KerberosClient(url=nn, root="/")
        else:
            self.client = Client(url=nn, root="/")

    def get_files(self, url_path, name_pattern, recursive):
        """
        List the visible files matching name_pattern. Should return a "File"
        namedtuple.
        """
        if name_pattern == "":
            name_pattern = "*"

        file_objs = []

        single_file = True
        depth = 0 if recursive else 1
        for path, dirs, files in self.client.walk(
                url_path, depth=depth, status=True):
            single_file = False

            for file_or_dir in itertools.chain(dirs, files):
                name, stats = file_or_dir
                fpath = path[0] + '/' + name

                file_obj = File(
                    path=fpath,
                    relPath=os.path.relpath(fpath, url_path),
                    isDir=stats["type"] == "DIRECTORY",
                    size=stats["length"],
                    mtime=int(stats["modificationTime"] / 1000))

                if match(name_pattern, recursive, file_obj):
                    file_objs.append(file_obj)
        if single_file:
            file_obj = File(
                path=url_path,
                relPath=os.path.basename(url_path),
                isDir=False,
                size=1,
                mtime=0)
            if match(name_pattern, recursive, file_obj):
                file_objs.append(file_obj)

        return file_objs

    def open(self, path, opts):
        if "w" in opts:
            raise ValueError("Write files not implemented for WebHDFS")
        only_file = self.client.read(path)

        return only_file

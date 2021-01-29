import urllib.parse

from xcalar.container.connectors.webhdfs import WebHDFSConnector
import xcalar.container.target.base as target


@target.register(
    name="WebHDFS", is_available=WebHDFSConnector.is_available(kerberos=False))
@target.param("namenodes", "Semi-colon delimited list of namenodes")
class WebHdfsNoKerberosTarget(target.BaseTarget):
    """
    Connect to a HDFS cluster via the WebHDFS protocol.

    Namenode to be specified in the form
    <FQDN of namenode>:<port number>. <port number> if omitted, defaults to 50070.

    Multiple name-nodes can be specified with semi-colon delimiter. E.g.

    http://namenode1.example.com;http://namenode2.example.com:50071;http://namenode3.example.com;...

    You may use https instead of http as well. The default is http if
    protocol is not provided.
    """

    def __init__(self, name, path, namenodes, **kwargs):
        super(WebHdfsNoKerberosTarget, self).__init__(name)

        nodes = []
        for node in namenodes.split(";"):
            parsed = urllib.parse.urlparse(node)
            protocol = "http" if parsed.scheme == "" else parsed.scheme
            hostname = parsed.netloc if parsed.netloc != "" else parsed.path
            if hostname == "":
                continue
            tmp = hostname.split(":")
            if (len(tmp) > 1):
                fqdn = tmp[0]
                port = int(tmp[1])
            else:
                fqdn = hostname
                port = 50070

            nodes.append("%s://%s:%d" % (protocol, fqdn, port))

        nn = ";".join(nodes)
        self.connector = WebHDFSConnector(nn, kerberos=False, keytab=None)

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, **user_args):
        return self.connector.get_files(path, name_pattern, recursive)

    def open(self, path, opts):
        return self.connector.open(path, opts)
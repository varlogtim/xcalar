import requests
from urllib.parse import urlparse, urlunparse
from urllib3.exceptions import ConnectTimeoutError, ConnectionError, \
    InvalidHeader, MaxRetryError, NewConnectionError, TimeoutError, \
    ReadTimeoutError, InsecureRequestWarning
from urllib3 import disable_warnings
from socket import timeout as socketTimeout, gaierror
disable_warnings(InsecureRequestWarning)


class Service():
    """
    The Xcalar Api service object is a collection of utilities for detecting the state of
    Xcalar software services.

    To obtain a Service instance:
        >>> from xcalar.external.service import Service
        >>> service = Service("base_url"="https://cluster_node4.domain/")

    **Initializer Arguments**

    The Xcalar Api Service must be called with the following arguments:

    * **base_url** (:class:`str`) - Url for a cluster node where Xcalar is running

    **Atttributes**

    None.

    **Methods**

    These are the available methods:

    * :meth:`node_status`

    """

    def __init__(self, base_url=None):
        """
        Object to evaluate Xcalar service status

        :param base_url: Url to a Xcalar node, like "https://my-xalar-node"

        :raises ValueError: if no base_url is provided
        """
        self._base_url = base_url
        if (self._base_url is None):
            raise ValueError("A base node url must be provided!")

    def node_status(self, time_out=5):
        """
        This method returns a dictionary showing the list of Xcalar services that
        respond to requests on a cluster node and an overall status.

        Example:
            >>> from xcalar.external.service import Service
            >>> node = Service('https://my-node:8443')
            >>> status = node.node_status()
            >>> status
            { 'total_services': 5, 'services': { 'caddy': True, 'expServer': True,
            'usrnode': True, 'mgmtd': True, 'sqldf': True }, 'status': "up" }

        :param time_out: the number of seconds a REST http request will wait
                         before timing out

        :returns: a structure describing the state of cluster services
        :return_type: :obj:`dict`

        * :Response Stucture:
            * (*dict*)
                * **total_services** (*int*) - total number of Xcalar node services
                * **services** (*dict*) - A dictionary of node services and a boolean \
                                          describing their current availability: True is up \
                                          and False is down.  The number of keys should \
                                          equal total_services.
                * **status** (*str*) - One of three stings: 'up' means all services responded, \
                                       'down' means that no services responded, 'partial' means \
                                       that n services responded where 0 < n < total_services
        """

        def poll_service(parsed_url, service, service_paths, check_struct,
                         service_result):
            service_url = parsed_url
            service_url = service_url._replace(path=service_paths[service])
            is_running = 0

            try:
                service_resp = \
                    requests.get(urlunparse(service_url),
                                 verify=False, timeout=time_out)
            except (ConnectTimeoutError, ConnectionError, InvalidHeader,
                    MaxRetryError, NewConnectionError, TimeoutError,
                    ConnectionRefusedError, ConnectionResetError,
                    ConnectionAbortedError, socketTimeout, gaierror,
                    ReadTimeoutError, requests.ReadTimeout,
                    requests.ConnectionError) as e:
                return is_running

            if not check_struct and service_resp.status_code == 200:
                service_result['services'][service] = True
                is_running = 1

            if check_struct and service_resp.status_code == 200 and \
               service_resp.json()['status'] == "up":
                service_result['services'][service] = True
                is_running = 1

            return is_running

        result = {
            'services': {
                'caddy': False,
                'expServer': False,
                'usrnode': False,
                'mgmtd': False,
                'sqldf': False
            },
            'status': 'down'
        }

        paths_by_service = {
            'caddy': '/healthCheck',
            'expServer': '/app/service/healthCheck',
            'usrnode': '/app/service/healthCheckUsrnode',
            'mgmtd': '/app/service/healthCheckMgmtd',
            'sqldf': '/app/service/healthCheckSqldf'
        }

        self.parsed_url = urlparse(self._base_url)
        total_services = len(result['services'])
        running_services = 0

        running_services += \
            poll_service(self.parsed_url, 'caddy',
                         paths_by_service, False, result)

        if result['services']['caddy']:
            running_services += \
                    poll_service(self.parsed_url, 'expServer',
                                 paths_by_service, True, result)

        if result['services']['expServer']:
            for service in ['usrnode', 'mgmtd', 'sqldf']:
                running_services += \
                        poll_service(self.parsed_url, service,
                                     paths_by_service, True, result)

        if running_services == total_services:
            result['status'] = 'up'
        elif running_services > 0 and \
        running_services < total_services:
            result['status'] = 'partial'

        return result

import argparse
import json
import collections.abc
import os
import traceback
from pathlib import Path
from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from colorama import Style, Fore
from xcalar.external.app import App
from xcalar.solutions.singleton import Singleton_Controller
from xcalar.solutions.orchestrator import Orchestrator
from xcalar.solutions.uber_dispatcher import UberDispatcher
from xcalar.solutions.analyzer import Analyzer
from xcalar.solutions.state_persister import StatePersister
import logging
import logging.config
import structlog
from structlog import get_logger
from structlog.contextvars import bind_contextvars, clear_contextvars, merge_contextvars
from xcalar.solutions.app.app_runner import ControllerAppClient
from xcalar.solutions.logging_config import LOGGING_CONFIG

logger = get_logger(__name__)


class XShell_Connector(object):
    def __init__(self):
        self.is_setup = False

    def setup(self):
        if self.is_setup:
            return

        os.environ['XCALAR_SHELL_UNIVERSE_ID'] = xshell_conf.get(
            'controller').get('universe_id')
        if xshell_conf.get('controller').get('session_name'):
            os.environ['XCALAR_SHELL_SESSION_NAME'] = xshell_conf.get(
                'controller').get('session_name')
        else:
            os.environ['XCALAR_SHELL_SESSION_NAME'] = xshell_conf.get(
                'controller').get('universe_id')
        # register the controller app
        # propertiesAdapter = PropertiesAdapter(
        #     os.environ['XCALAR_SHELL_PROPERTIES_FILE']
        # )
        # xcalarProperties = propertiesAdapter.getXcalarProperties()
        # controllerAppProps = propertiesAdapter.getControllerAppProps()
        # client = get_client(
        #     xcalarProperties.url, xcalarProperties.username, xcalarProperties.password
        # )

        # Todo: What are xcalarApi, appMagr
        client = XShell_Connector.get_client()
        self.is_setup = True

    def readArgs(self):
        parser = argparse.ArgumentParser(description='Xcalar Shell')
        parser.add_argument(
            '--properties',
            type=str,
            required=True,
            help='Properties file for the shell',
        )
        parser.add_argument(
            '--universe_id',
            type=str,
            required=True,
            help='Universe id to use')

        parser.add_argument(
            '--session',
            type=str,
            required=False,
            help=    # NOQA
            'Session name to use. Will be read from properties if unspecified.',    # NOQA
        )

        return parser.parse_args()

    @staticmethod
    def release_connect_cache():
        empty_objs = {
            'client': {
                'url': None,
                'username': None,
                'password': None,
                'client_obj': None,
                'xcalarapi_obj': None,
                'appClient_obj': None,
            },
        }
        XShell_Connector.update(xshell_conf, empty_objs)

    @staticmethod
    def release_use_cache():
        empty_objs = {
            'client': {
                'appClient_obj': None,
            },
            'controller': {
                'universe_id': None,
                'session_obj': None,
                'properties_file_path': None,
                'controller_obj': None,
                'uber_dispatcher_obj': None,
                'analyzer_obj': None,
                'universe_obj': None,
                'orchestrator_obj': None,
            },
            'controllerApp': {
                'stateChange': {
                    'numTries': None,
                    'sleepIntervalInSec': None
                },
                'appFilePath': None
            },
            'git': {
                'url': None,
                'projectId': None,
                'accessToken': None,
            },
            'callbacksPluginName': None,
            'universeAdapterPluginName': None,
            'xcautoconfirm': 'yes'
        }
        XShell_Connector.update(xshell_conf, empty_objs)

    @staticmethod
    def release_appClient():
        empty_objs = {'client': {'appClient_obj': None, }}
        XShell_Connector.update(xshell_conf, empty_objs)

    @staticmethod
    def initializeLogger(universe_id, properties_file_path):
        # =================================
        # 0. Initial Logging
        # =================================
        try:
            logging.config.dictConfig(LOGGING_CONFIG)
            structlog.configure(
                processors=[
                    merge_contextvars,
                    structlog.processors.KeyValueRenderer()
                ],
                context_class=structlog.threadlocal.wrap_dict(dict),
                logger_factory=structlog.stdlib.LoggerFactory(),
                wrapper_class=structlog.stdlib.BoundLogger,
                cache_logger_on_first_use=True,
            )
            clear_contextvars()
            bind_contextvars(universe_id=universe_id)
        except Exception as e:
            print(f'{Style.DIM}Error: {Style.RESET_ALL}controller {e}')

    @staticmethod
    def update(xmap, upd):
        for k, v in upd.items():
            if isinstance(v, collections.abc.Mapping):
                xmap[k] = XShell_Connector.update(xmap.get(k, {}), v)
            else:
                xmap[k] = v
        return xmap

    def update_xshell_client(self, url, username, password):
        # clear cache
        XShell_Connector.release_connect_cache()
        XShell_Connector.release_use_cache()

        is_client_success = False
        is_xcalarapi_success = False
        # =========================================
        # 1. client
        # =========================================
        try:
            client = Client(
                url=url,
                client_secrets={
                    'xiusername': username,
                    'xipassword': password
                },
                session_type='xshell',
                bypass_proxy=False,
            )

            client_version = client.get_version()
            is_client_success = False if not client_version else True
            print(
                f'{Style.DIM}INFO: {Style.RESET_ALL}client version: {client_version}'
            )
        except Exception as e:
            print(f'{Style.DIM}Error: {Style.RESET_ALL}client {e}')
            pass
        # =========================================
        # 2. xcalarapi
        # =========================================
        try:
            xcalarapi = XcalarApi(
                url=url,
                client_secrets={
                    'xiusername': username,
                    'xipassword': password
                },
                session_type='xshell',
                bypass_proxy=False,
            )
            is_xcalarapi_success = True
        except Exception as e:
            print(f'{Style.DIM}Error: {Style.RESET_ALL}xcalarapi {e}')
            pass

        # =========================================
        # final: update to xshell_conf
        # =========================================
        try:
            if is_client_success and is_xcalarapi_success:
                print(
                    f'{Style.DIM}INFO: {Style.RESET_ALL}{url} connection succeed!'
                )
                upd_objs = {
                    'client': {
                        'url': url,
                        'username': username,
                        'password': password,
                        'client_obj': client,
                        'xcalarapi_obj': xcalarapi,
                    }
                }
                XShell_Connector.update(xshell_conf, upd_objs)

            else:
                print(
                    f'{Style.DIM}INFO: {Style.RESET_ALL}{url} connection failed!'
                )
                upd_objs = {
                    'client': {
                        'url': url,
                        'username': None,
                        'password': None,
                        'client_obj': None,
                        'xcalarapi_obj': None,
                    }
                }
                XShell_Connector.update(xshell_conf, upd_objs)

        except Exception as e:
            print(f'{Style.DIM}Error: {Style.RESET_ALL}xcalarapi {e}')

        return

    def update_xshell_controller(self,
                                 universe_id,
                                 session_name=None,
                                 properties_file_path=None):
        # reset xshell_conf
        XShell_Connector.release_use_cache()
        try:
            properties_dict={
                "controllerApp": {
                    "stateChange": {
                            "numTries": 40,
                            "sleepIntervalInSec": 10
                    },
                    "appFilePath": "/tmp"
                }
            }

            client = XShell_Connector.get_client()
            xcalarapi = XShell_Connector.get_xcalarapi()
            if session_name:
                session_obj = XShell_Connector.create_session_obj(
                    client, session_name)
            else:
                session_obj = XShell_Connector.create_session_obj(
                    client, universe_id)
            xcalarapi.setSession(session_obj)
            XShell_Connector.update_xshell_universe_id(universe_id)

        except Exception as e:
            print(f'{Style.DIM}INFO: {Style.RESET_ALL}{e}')
            traceback.print_tb(e.__traceback__)

        # Todo: Not sure if others are still using environment variables,
        #       so I add environment variable (Francis)
        # args = readArgs()
        if universe_id:
            os.environ['XCALAR_SHELL_UNIVERSE_ID'] = universe_id

        if session_name:
            os.environ['XCALAR_SHELL_SESSION_NAME'] = session_name
        else:
            os.environ['XCALAR_SHELL_SESSION_NAME'] = universe_id

        if properties_file_path:
            os.environ['XCALAR_SHELL_PROPERTIES_FILE'] = properties_file_path

        # =================================
        # 0. read config file
        # =================================
        try:
            if properties_file_path is None:
                # No appFilePath and appPropertiesFilePath
                print(f'{Style.DIM}Warning: {Style.RESET_ALL}argument properties not assigned. Use default setttings without app configuration.')
            else:
                with open(properties_file_path, 'r') as f:
                    properties_dict = json.load(f)

        except Exception as e:
            print(f'{Style.DIM}Error: {Style.RESET_ALL}controller {e}')
        # =================================
        # 1. controller = Controller()
        # =================================
        # try:
        #     singleton_controller = Singleton_Controller.getInstance(
        #         properties_file_path, universe_id
        #     )
        #     controller = singleton_controller.get_controller()
        #     # controller.initialize(properties_file_path, universe_id, universe_name)
        #
        # except Exception as e:
        #     print(f'{Style.DIM}Error: {Style.RESET_ALL}controller {e}')

        # =========================================
        # 2. uber_dispatcher
        # =========================================

        try:
            uber_dispatcher = UberDispatcher(
                client,
                session_obj,
                xcalarapi,
            )
            XShell_Connector.update_xshell_uber_dispatcher(uber_dispatcher)

            # version = client.get_version()
            # print(f'{Style.DIM}INFO: {Style.RESET_ALL}uber_dispatcher version: {version}')
        except Exception as e:
            print(f'{Style.DIM}Error: {Style.RESET_ALL}uber_dispatcher {e}')
        # =========================================
        # 3. set analyzer
        # =========================================
        try:
            analyzer = Analyzer(uber_dispatcher)
            XShell_Connector.update_xshell_analyzer(analyzer)

            # version = client.get_version()
            # print(f'{Style.DIM}INFO: {Style.RESET_ALL}analyzer version: {version}')
        except Exception as e:
            print(f'{Style.DIM}Error: {Style.RESET_ALL}analyzer {e}')

        # =========================================
        # 4. set orchestrator
        # =========================================
        # Notes:
        # restoreFromCheckpoint(key, client, session, xcalarApi, universe):
        # key = universe_id  (if no universe_id exists then fail)
        # unniverse = None

        try:
            orchestrator = XShell_Connector.get_orchestrator(universe_id)
            # version = client.get_version()
            # print(f'{Style.DIM}INFO: {Style.RESET_ALL}orchestrator_obj version: {version}')
        except Exception as e:
            print(
                f'{Style.DIM}INFO: {Style.RESET_ALL}create new orchestrator {e}'
            )
            # if not, create a new one
            orchestrator = Orchestrator.restoreFromCheckpoint(
                universe_id, client, session_obj, xcalarapi, None)
            XShell_Connector.update_xshell_orchestrator(orchestrator)

        # =========================================
        # 5. final : update instances
        # =========================================

        try:
            # session_obj = get_or_create_session(client, universe_id)
            # xcalarapi.setSession(session_obj)

            upd_objs = {
                'controller': {
                    'universe_id': universe_id,
                    'session_name': session_name,
                    'session_obj': session_obj,
                    'properties_file_path': properties_file_path,
            # 'controller_obj': None,
                    'uber_dispatcher_obj': uber_dispatcher,
                    'analyzer_obj': analyzer,
                    'orchestrator_obj': orchestrator,
                }
            }

            XShell_Connector.update(xshell_conf, upd_objs)
            # -------------------------------------
            # update controllerApp, git to properties_dict
            # -------------------------------------

            # update controllerApp
            # properties_dict
            numTries = properties_dict['controllerApp']['stateChange'][
                'numTries']
            sleepIntervalInSec = properties_dict['controllerApp'][
                'stateChange']['sleepIntervalInSec']
            appFilePath = properties_dict['controllerApp']['appFilePath']

            upd_controllerApp_objs = {
                'controllerApp': {
                    'stateChange': {
                        'numTries': numTries,
                        'sleepIntervalInSec': sleepIntervalInSec
                    },
                    'appFilePath': appFilePath
                }
            }
            XShell_Connector.update(xshell_conf, upd_controllerApp_objs)

            # ------------------------------------------------------------------------------------------------
            # git, callbacksPluginName, universeAdapterPluginName, loggingPropertiesFilepath, 'xcautoconfirm'
            # ------------------------------------------------------------------------------------------------
            xcautoconfirm = properties_dict.get('xcautoconfirm', 'yes')
            if 'git' not in properties_dict:
                raise Exception('Please provide <git> information in property file')
            else:
                url = properties_dict['git']['url']
                projectId = properties_dict['git']['projectId']
                accessToken = properties_dict['git']['accessToken']
            if 'callbacksPluginName' not in properties_dict:
                raise Exception('Please provide <callbacksPluginName> in property file')
            else:
                callbacksPluginName = properties_dict['callbacksPluginName']
            if 'universeAdapterPluginName' not in properties_dict:
                raise Exception('Please provide <universeAdapterPluginName> in property file')
            else:
                universeAdapterPluginName = properties_dict[
                        'universeAdapterPluginName']
            upd_other_objs = {
                'git': {
                    'url': url,
                    'projectId': projectId,
                    'accessToken': accessToken,
                },
                'callbacksPluginName': callbacksPluginName,
                'universeAdapterPluginName': universeAdapterPluginName,
                'xcautoconfirm': xcautoconfirm
            }
            XShell_Connector.update(xshell_conf, upd_other_objs)
                # logger.info('update git, callbacksPluginName, universeAdapterPluginName and xcautoconfirm to xshell_conf')
            # -------------------------------------------
            # nodes
            # -------------------------------------------
            key = 'xcalar'
            if key in properties_dict:
                nodes = properties_dict['xcalar']['nodes']
                upd_nodes = {'cluster': {'nodes': nodes, }}
                XShell_Connector.update(xshell_conf, upd_nodes)

            # -------------------------------------------
            # logging
            # -------------------------------------------
            os.environ['XCAUTOCONFIRM'] = xcautoconfirm
            upd_other_objs = {'xcautoconfirm': xcautoconfirm}
            XShell_Connector.update(xshell_conf, upd_other_objs)

        except Exception as e:
            print(f'{Style.DIM}Error: {Style.RESET_ALL}update objects {e}')

        return

    def update_xshell_appClient(self, client, universeId, xcalarApi):
        controllerAppProp = xshell_conf.get('controllerApp')
        controllerAppProp['git'] = xshell_conf.get('git')
        controllerAppProp['callbacksPluginName'] = xshell_conf.get('callbacksPluginName')
        controllerAppProp['universeAdapterPluginName'] = xshell_conf.get('universeAdapterPluginName')
        appClient = ControllerAppClient(client, universeId, xcalarApi,
                                        controllerAppProp
                                        )
        upd_objs = {'client': {'appClient_obj': appClient}}
        XShell_Connector.update(xshell_conf, upd_objs)

    @staticmethod
    def update_xshell_universe_id(universe_id):
        upd_objs = {'controller': {'universe_id': universe_id}}
        XShell_Connector.update(xshell_conf, upd_objs)

    @staticmethod
    def update_xshell_session_name(session_name):
        upd_objs = {'controller': {'session_name': session_name}}
        XShell_Connector.update(xshell_conf, upd_objs)

    @staticmethod
    def update_xshell_properties_file_path(properties_file_path):
        upd_objs = {
            'controller': {
                'properties_file_path': properties_file_path
            }
        }
        XShell_Connector.update(xshell_conf, upd_objs)

    @staticmethod
    def update_xshell_analyzer(analyzer):
        upd_objs = {'controller': {'analyzer_obj': analyzer}}
        XShell_Connector.update(xshell_conf, upd_objs)

    @staticmethod
    def update_xshell_uber_dispatcher(uber_dispatcher):
        upd_objs = {'controller': {'uber_dispatcher_obj': uber_dispatcher}}
        XShell_Connector.update(xshell_conf, upd_objs)

    @staticmethod
    def update_xshell_orchestrator(orchestrator):
        upd_objs = {'controller': {'orchestrator_obj': orchestrator}}
        XShell_Connector.update(xshell_conf, upd_objs)

    @staticmethod
    def create_session_obj(client, universe_id):
        try:
            return client.get_session(universe_id)
        except Exception:
            return client.create_session(universe_id)

    @staticmethod
    def get_universe_id():
        if XShell_Connector.self_check_controller():
            try:
                universe_id = xshell_conf.get('controller').get('universe_id')
                if universe_id is None:
                    raise ValueError('universe_id is not defined.')
                else:
                    return universe_id
            except Exception as e:
                raise ValueError(
                    f'{e} Please run "{Style.DIM}connect {Style.RESET_ALL}" command first. '
                )

    @staticmethod
    def get_cluster_nodes():
        if XShell_Connector.self_check_controller():
            try:
                nodes = xshell_conf.get('cluster').get('nodes')
                if nodes is None:
                    raise ValueError('nodes is not defined.')
                else:
                    return nodes
            except Exception as e:
                raise ValueError(
                    f'{e} Please run "{Style.DIM}connect {Style.RESET_ALL}" command first. '
                )

    @staticmethod
    def get_session_obj():
        if XShell_Connector.self_check_controller():
            try:
                session_obj = xshell_conf.get('controller').get('session_obj')
                if session_obj is None:
                    raise ValueError('universe_id is not defined.')
                else:
                    return session_obj
            except Exception as e:
                raise ValueError(
                    f'{e} Please run "{Style.DIM}connect {Style.RESET_ALL}" command first. '
                )

    @staticmethod
    def get_properties():
        if XShell_Connector.self_check_controller():
            try:
                properties = xshell_conf.get('controller').get(
                    'properties_file_path')
                if properties is None:
                    raise ValueError('properties_file_path is not defined.')
                else:
                    return properties
            except Exception as e:
                raise ValueError(
                    f'{e} Please run "{Style.DIM}connect {Style.RESET_ALL}" command first. '
                )

    @staticmethod
    def get_controller():
        if XShell_Connector.self_check_controller():
            try:
                controller = xshell_conf.get('controller').get(
                    'controller_obj')
                if controller is None:
                    try:
                        logger.info(
                            'First time launch controller, initialize controller...'
                        )
                        # print(f'{Style.BRIGHT}First time launch controller, initialize controller...{Style.RESET_ALL}')
                        singleton_controller = Singleton_Controller.getInstance(
                        )
                        controller = singleton_controller.get_controller()
                        # controller.orchestrator = Orchestrator.updateState(controller.orchestrator, XShell_Connector.get_client(), XShell_Connector.get_session_obj(), XShell_Connector.get_xcalarapi(), None)
                        # controller.initialize(
                        #     XShell_Connector.get_properties,
                        #     XShell_Connector.get_universe_id,
                        #     XShell_Connector.get_universe_id
                        # )

                        upd_objs = {
                            'controller': {
                                'controller_obj': controller
                            }
                        }

                        XShell_Connector.update(xshell_conf, upd_objs)
                        logger.info('controller_obj updated')
                        logger.info('controller initialized')
                        # print(f'{Style.BRIGHT}controller initialized.{Style.RESET_ALL}')
                        return controller
                    except Exception as e:
                        print(
                            f'{Style.DIM}Error: {Style.RESET_ALL}controller {e}'
                        )

                else:
                    return controller
            except Exception as e:
                raise ValueError(
                    f'{e} Please run "{Style.DIM}connect {Style.RESET_ALL}" command to get controller instance. '
                )

    @staticmethod
    def get_xcautoconfirm():
        if XShell_Connector.self_check_controller():
            try:
                xcautoconfirm = xshell_conf.get('xcautoconfirm')
                if xcautoconfirm is None:
                    raise ValueError('xcautoconfirm is not defined.')
                else:
                    return xcautoconfirm
            except Exception as e:
                raise ValueError(
                    f'{e} Please run "{Style.DIM}use{Style.RESET_ALL}" command to select a universe. '
                )

    @staticmethod
    def get_client():
        if XShell_Connector.self_check_client():
            try:
                client_obj = xshell_conf.get('client').get('client_obj')
                if client_obj is None:
                    raise ValueError('client is not defined.')
                else:
                    return client_obj
            except Exception as e:
                raise ValueError(
                    f'{e} Please run "{Style.DIM}connect{Style.RESET_ALL}" command to connect a cluster. '
                )

    @staticmethod
    def get_xcalarapi():
        if XShell_Connector.self_check_client():
            try:
                xcalarapi_obj = xshell_conf.get('client').get('xcalarapi_obj')
                if xcalarapi_obj is None:
                    raise ValueError('xcalarapi is not defined.')
                else:
                    return xcalarapi_obj
            except Exception as e:
                raise ValueError(
                    f'{e} Please run "{Style.DIM}connect {Style.RESET_ALL}" command to connect a cluster. '
                )

    @staticmethod
    def get_appClient():
        if XShell_Connector.self_check_client():
            appClient_obj = xshell_conf.get('client').get('appClient_obj')
            if appClient_obj is None:
                raise ValueError(
                    f' Please run "{Style.DIM}controller_app -init {Style.RESET_ALL}" command. '
                )
            else:
                return appClient_obj

    @staticmethod
    def get_uber_dispatcher():
        if XShell_Connector.self_check_controller():
            try:
                uber_dispatcher_obj = xshell_conf.get('controller').get(
                    'uber_dispatcher_obj')
                if uber_dispatcher_obj is None:
                    raise ValueError('uber_dispatcher_obj is not defined.')
                else:
                    return uber_dispatcher_obj
            except Exception as e:
                raise ValueError(
                    f'{e} Please run "{Style.DIM}use {Style.RESET_ALL}" command to select a universe. '
                )

    @staticmethod
    def get_analyzer():
        if XShell_Connector.self_check_controller():
            try:
                analyzer_obj = xshell_conf.get('controller').get(
                    'analyzer_obj')
                if analyzer_obj is None:
                    raise ValueError('analyzer_obj is not defined.')
                else:
                    return analyzer_obj
            except Exception as e:
                raise ValueError(
                    f'{e} Please run "{Style.DIM}use {Style.RESET_ALL}" command to select a universe. '
                )

    @staticmethod
    def get_watermark():
        if XShell_Connector.self_check_controller():

            # -----------------------------------------------------------
            # get watermark ... Not done yet!!!
            # -----------------------------------------------------------
            # universe_id = XShell_Connector.get_universe_id()
            # key = f'universe_{universe_id}'
            # kvstore = XShell_Connector.get_client()
            # watermarkPersister = StatePersister(key, kvstore)
            # watermark = watermarkPersister.restore_state()
            # return watermark

            # -----------------------------------------------------------
            # get orchestrator from kvstore => key = 'universe_' + universeId
            # -----------------------------------------------------------
            universe_id = XShell_Connector.get_universe_id()
            client = XShell_Connector.get_client()
            kvstore = client.global_kvstore()
            orchestratorPersister = StatePersister(universe_id, kvstore)
            try:
                orchestrator = orchestratorPersister.restore_state()
                return orchestrator.watermark
            except Exception as e:
                raise ValueError(
                    f'{e} Please run "{Style.DIM}use {Style.RESET_ALL}" command to select a universe. '
                )

    @staticmethod
    def get_flat_universe():
        if XShell_Connector.self_check_controller():

            # -----------------------------------------------------------
            # get universe from kvstore => key = 'universe_' + universeId
            # -----------------------------------------------------------
            # universe_id = XShell_Connector.get_universe_id()
            # key = f'universe_{universe_id}'
            # kvstore = XShell_Connector.get_client()
            # universePersister = StatePersister(key, kvstore)
            # try:
            #     universe = universePersister.restore_state()
            #     print(type(universe))
            #     return universe
            # except Exception as e:
            #     raise ValueError(
            #         f'{e} Please run "{Style.DIM}use {Style.RESET_ALL}" command to select a universe. '
            #     )

            universe_id = XShell_Connector.get_universe_id()
            client = XShell_Connector.get_client()
            kvstore = client.global_kvstore()
            orchestratorPersister = StatePersister(universe_id, kvstore)
            try:
                orchestrator = orchestratorPersister.restore_state()
                return orchestrator.flat_universe['tables']
            except Exception as e:
                raise ValueError(
                    f'{e} Please run "{Style.DIM}use {Style.RESET_ALL}" command to select a universe. '
                )

    @staticmethod
    def get_orchestrator(universe_id):
        if XShell_Connector.self_check_controller():
            # -----------------------------------------------------------
            # get orchestrator from kvstore => key = 'universe_' + universeId
            # -----------------------------------------------------------
            # universe_id = XShell_Connector.get_universe_id()
            client = XShell_Connector.get_client()
            kvstore = client.global_kvstore()
            orchestratorPersister = StatePersister(universe_id, kvstore)

            orchestrator = orchestratorPersister.restore_state()
            if orchestrator is None:
                print()
                    #f'Please run "{Style.DIM}controller_app -init {Style.RESET_ALL}" first. '
                return

            try:
                orchestrator = orchestrator.updateStateClient(
                    orchestrator, XShell_Connector.get_client(),
                    XShell_Connector.get_session_obj(),
                    XShell_Connector.get_xcalarapi())
                return orchestrator
            except Exception as e:
                raise ValueError(
                    f'{e} Please run "{Style.DIM}use {Style.RESET_ALL}" command to select a universe. '
                )

    @staticmethod
    def get_universe():
        if XShell_Connector.self_check_controller():

            # -----------------------------------------------------------
            # get universe from kvstore => key = 'universe_' + universeId
            # -----------------------------------------------------------
            universe_id = XShell_Connector.get_universe_id()
            key = f'universe_{universe_id}'
            kvstore = XShell_Connector.get_client()
            universePersister = StatePersister(key, kvstore)
            try:
                universe = universePersister.restore_state()
                print(type(universe))
                return universe
            except Exception as e:
                raise ValueError(
                    f'{e} Please run "{Style.DIM}use {Style.RESET_ALL}" command to select a universe. '
                )

    @staticmethod
    def status():
        if xshell_conf.get('client').get('client_obj') is None:
            print(
                f'Please run "{Style.DIM}connect {Style.RESET_ALL}" command to connect a cluster.'
            )
            return

        if xshell_conf.get('client').get('client_obj') is None:
            print(
                f'Please run "{Style.DIM}connect {Style.RESET_ALL}" command to connect a cluster.'
            )
            return

        if xshell_conf.get('controller').get('client_obj') is None:
            print(
                f'Please run "{Style.DIM}use {Style.RESET_ALL}" command to select a universe.'
            )
            return

        if xshell_conf.get('controller').get('client_obj') is None:
            print(
                f'Please run "{Style.DIM}use {Style.RESET_ALL}" command to select a universe.'
            )
            return

    @staticmethod
    def self_check_client():
        client = xshell_conf.get('client').get('client_obj')
        xcalarapi = xshell_conf.get('client').get('xcalarapi_obj')
        if client is None or xcalarapi is None:
            print(
                f'No cluster connected. \nPlease run "{Fore.YELLOW}connect {Style.RESET_ALL}" to connect a cluster first.'
            )
            return False
        return True

    @staticmethod
    def self_check_controller():
        analyzer = xshell_conf.get('controller').get('analyzer_obj')
        uber_dispatcher = xshell_conf.get('controller').get(
            'uber_dispatcher_obj')
        if analyzer is None or uber_dispatcher is None:
            print(
                f'No universe is assigned. \nPlease run "{Fore.YELLOW}use {Style.RESET_ALL}" to assigne a universe first. '
            )
            return False

        return True


# =========================================================
# global (Single Cluster & single user & Single Universe)
# =========================================================
# cluster1 -> user A  -> universe X

xshell_conf = {
    'client': {
        'url': None,
        'username': None,
        'password': None,
        'client_obj': None,
        'xcalarapi_obj': None,
        'appClient_obj': None,
    },
    'controller': {
        'universe_id': None,
        'session_obj': None,
        'properties_file_path': None,
        'controller_obj': None,
        'uber_dispatcher_obj': None,
        'analyzer_obj': None,
        'universe_obj': None,
        'orchestrator_obj': None,
    },
    'controllerApp': {
        'stateChange': {
            'numTries': None,
            'sleepIntervalInSec': None
        },
        'appFilePath': None
    },
    'git': {
        'url': None,
        'projectId': None,
        'accessToken': None,
    },
    'callbacksPluginName': None,
    'universeAdapterPluginName': None,
    'xcautoconfirm': 'yes'
}
# TODO: talk to josh , read git info from cluster.
# =============================================
# Multi Cluster & multi user & multi universe ( coming soon)
# ==============================================
# cluster1 -> user A
#         |          -> universe U1
#         |          -> universe U2
#         |
#          -> user B
#         |          -> universe C1
#         |          -> universe C2
#
# cluster2 -> user X
#         |          -> universe F1
#         |          -> universe F2
#          -> user Y
#                    -> universe Y1
#
# --------------------------

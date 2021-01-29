from xcalar.solutions.xcalar_client_utils import (
    get_or_create_session,
    get_client,
    get_xcalarapi,
)
from xcalar.solutions.uber_dispatcher import UberDispatcher
from xcalar.solutions.uber_connector import Connector
from xcalar.solutions.properties_adapter import PropertiesAdapter
from xcalar.solutions.orchestrator import Orchestrator
from xcalar.solutions.universe import Universe
from xcalar.solutions.git.git_api import GitApi
from xcalar.solutions.sdlc import SdlcManager
from xcalar.solutions.state_persister import StatePersister
from transitions import Machine
import xcalar.container.context as ctx

# Use this to generate graphs
# from transitions.extensions import GraphMachine as Machine
# from xcalar.external.LegacyApi.App import App
from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.solutions.controller_state import ControllerState
from xcalar.solutions.logging_config import LOGGING_CONFIG
import sys
import jsonpickle
import argparse
import importlib
import logging
import logging.config
import structlog
from structlog import get_logger
from datetime import datetime
import time
import atexit
import math
import traceback
import multiprocessing

from structlog.contextvars import bind_contextvars, clear_contextvars, merge_contextvars

logger = get_logger(__name__)


class Controller:
    states = [
        'none',
        'initialized',
        'universe_loading',
        'universe_loaded',
        'materializing',
        'materialized',
        'streaming',
        'snapshotting',
        'recovering',
        'adjusting',
        'failed',
        'destroyed',
    ]
    transitions = [
    # action, from, to
    # none
        ['_sm_init', 'none', 'initialized'],
    # initialized
        ['_sm_start_streaming', 'initialized', 'streaming'],
        ['_sm_start_recovery', 'initialized', 'recovering'],
        ['_sm_start_refresh', 'initialized', 'universe_loading'],
        ['_sm_start_materializing', 'initialized', 'materializing'],
        ['_sm_snapshot', 'initialized', 'snapshotting'],
        ['_sm_start_adjust', 'initialized', 'adjusting'],
        ['_sm_start_adjust', 'materialized', 'adjusting'],
    # universe_loading
        ['_sm_loaded', 'universe_loading', 'universe_loaded'],
    # universe_loaded
        ['_sm_init', 'universe_loaded', 'initialized'],
    # materializing
        ['_sm_snapshot', 'materializing', 'snapshotting'],
    # snapshotting
        ['_sm_continue_materializing', 'snapshotting', 'materializing'],
    # materialized
        ['_sm_init', 'materialized', 'initialized'],
    # streaming
        ['_sm_snapshot', 'streaming', 'snapshotting'],
        ['_sm_pause', 'streaming', ' '],
        ['_sm_continue_streaming', 'streaming', 'streaming'],
    # snapshotting
        ['_sm_materialized', 'snapshotting', 'materialized'],
        ['_sm_continue_streaming', 'snapshotting', 'streaming'],
        ['_sm_init', 'snapshotting', 'initialized'],
    # recovering
        ['_sm_recovered', 'recovering', 'initialized'],
    # adjusting
        ['_sm_adjusted', 'adjusting', 'initialized'],
    # destroyed
        ['_sm_init', 'destroyed', 'initialized'],
    # fail
        ['_sm_fail', '*', 'failed'],
        ['_sm_destroy', '*', 'destroyed'],
    ]

    def initialize(self,
                   universeId,
                   propertyFile = None,
                   properties = None,
                   sessionName=None,
                   runAsApp=False,
                   username=None):
        self.universeId = universeId
        # Read properties file
        gitInfo = {}
        if not runAsApp:
            self.propertiesAdapter = PropertiesAdapter(propertyFile)
            git = self.propertiesAdapter.getGitProperties()
            gitInfo['url'] = git.url
            gitInfo['projectId'] = git.projectId
            gitInfo['accessToken'] = git.accessToken
            callbacksModule = self.propertiesAdapter.getCallbacksPluginName()
            universeAdapterModule = self.propertiesAdapter.getUniverseAdapterPluginName()
        else:
            logger.info(f'{properties}')
            gitInfo = properties['git']
            callbacksModule = properties['callbacksPluginName']
            universeAdapterModule = properties['universeAdapterPluginName']

        self.initializeLogger()
        # self.loadProperties()

        logger.debug('Initializing the controller')
        try:
            self.runAsApp = runAsApp
            # Initialize Xcalar session
            self.initializeXcalarClient(
                sessionName=sessionName, username=username)

            self.initializeStateMachine()

            # These are order dependent
            self.initializeDispatcher()

            self.initializeSdlcManager(gitInfo)

            self.initializeUniverse(callbacksModule, universeAdapterModule)

            self.initializeConnector()

            if self.state != 'initialized':
                self.to_initialized()
        except Exception as e:
            logger.fatal(f'Failed to initialize controller {str(e)}')
            exc_type, exc_value, exc_traceback = sys.exc_info()
            message = traceback.format_exception(exc_type, exc_value,
                                                 exc_traceback)
            logger.fatal(message)
            if hasattr(self, '_sm_fail'):
                self._sm_fail()
            raise e

    def initializeStateMachine(self):
        if self.runAsApp:
            KEY_NEXT_PREFIX = 'app_{}_next'
            KEY_CURRENT_PREFIX = 'app_{}_current'
            KEY_TRANSIT_PREFIX = 'app_{}_transit'

            self.transitStatePersister = StatePersister(
                KEY_TRANSIT_PREFIX.format(self.universeId),
                self.client.global_kvstore())
        else:
            KEY_NEXT_PREFIX = '{}_next'
            KEY_CURRENT_PREFIX = '{}_current'
        self.statePersister = StatePersister(
            KEY_NEXT_PREFIX.format(self.universeId),
            self.client.global_kvstore())
        self.currentStatePersister = StatePersister(
            KEY_CURRENT_PREFIX.format(self.universeId),
            self.client.global_kvstore())
        initial_state = self.currentStatePersister.restore_state()
        if initial_state is None:
            logger.info(f'State: No state found. Setting to initialized')
            initial_state = 'none'
        if not hasattr(self, 'machine') or not self.machine:
            self.machine = Machine(
                model=self,
                queued=True,
                ignore_invalid_triggers=True,
                states=Controller.states,
                initial=initial_state,
                transitions=Controller.transitions,
                before_state_change='recordPreviousState',
                after_state_change='persistCurrentState',
            )

    def recordPreviousState(self, **kwargs):
        logger.info(f'State: Previous state: {self.state}')
        self.previousState = self.state

    def persistCurrentState(self, **kwargs):
        logger.info(f'State: Persisting current state {self.state}')
        self.currentStatePersister.store_state(self.state)
        logger.info(f'State: Persisted current state {self.state}')
        if self.runAsApp:
            self.transitStatePersister.delete_state()

    def persistState(self, new_state: str, params={}):
        serializedState = jsonpickle.encode(ControllerState(new_state, params))
        logger.info(f'State: Persisting next state {serializedState}')
        self.statePersister.store_state(serializedState)
        logger.info(f'State: Persisted next state {serializedState}')

    def persistStateAfterSnapshot(self, new_state: str, params={}):
        serializedState = jsonpickle.encode(ControllerState(new_state, params))
        logger.info(f'Persisting next state {serializedState}')
        nextState = self.statePersister.restore_state()
        if nextState:
            logger.info(f'Persisted next state {nextState}')
        else:
            self.statePersister.store_state(serializedState)
            logger.info(f'Persisted next state {serializedState}')

    def nextState(self):
        nextState = self.statePersister.restore_state()
        logger.info(f'State: Next state {nextState}')
        if nextState:
            self.statePersister.delete_state()
            return jsonpickle.decode(nextState)
        return None

    def initializeUniverse(self, callbacksName, universeAdapterName):
        self.universeAdapter = self.getUniverseAdapter(universeAdapterName)
        self.universe = Universe()
        callback_module_name = callbacksName
        if not hasattr(self, 'callbacks') or self.callbacks is None:
            self.callbacks = self.dynaload(callback_module_name)

        self.universe.initialize(self.dispatcher, self.universeAdapter,
                                 self.universeId, self.callbacks,
                                 self.sdlcManager)

    def initializeSdlcManager(self, gitInfo):
        logger.debug('Initializing SDLC Manager')
        gitApi = GitApi(gitInfo['url'], gitInfo['projectId'],
                        gitInfo['accessToken'])
        self.sdlcManager = SdlcManager(self.dispatcher, gitApi)

    def initializeLogger(self):
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
        bind_contextvars(universe_id=self.universeId)

    def dynaload(self, name):
        logger.debug(f'Searching for {name} module')
        spec = importlib.util.find_spec(name)
        if spec is None:
            raise Exception(f'No {name} plugin found.')
        return spec.loader.load_module()

    def getSnapshotDataTargetName(self):
        name = self.universe.getSnapshotDataTargetName()
        if name is not None:
            return name
        else:
            return 'snapshot'

    def getSnapshotMaxRetentionNumber(self):
        max_retention_number = self.universe.getSnapshotMaxRetentionNumber()
        if max_retention_number is not None:
            return max_retention_number
        else:
            return 5

    def getSnapshotMaxRetentionPeriodSeconds(self):
        max_retention_period_seconds = (
            self.universe.getSnapshotMaxRetentionPeriodSeconds())
        if max_retention_period_seconds is not None:
            return max_retention_period_seconds
        else:
            return 604800    # one week in seconds

    def getControllerPausePollFreqSecs(self):
        freq = self.propertiesAdapter.getControllerPausePollFreqSecs()
        if freq is not None:
            return freq
        else:
            return 30

    def getUniverseAdapter(self, pluginName):
        m = self.dynaload(pluginName)
        return m.XcalarUniverseAdapter()

    def initializeDispatcher(self):
        # xcalarProperties = self.propertiesAdapter.getXcalarProperties()

        logger.debug('Initializing dispatcher')
        self.dispatcher = UberDispatcher(
            self.client,
            session=self.session,
            xcalarApi=self.xcalarApi,
            isDebug=True,
        )

    def initializeConnector(self):
        # logger.debug('Initializing connector')
        # universeDef = self.universe.universeDef
        # sourceDefs = universeDef['source_defs']
        # kafka_connector = [
        #     sourceDefs[u]['parserTemplateName']
        #     for u in sourceDefs
        #     if sourceDefs[u].get('targetName', None) == 'kafka-base'
        # ]
        # # fixme. There should be multiple connectors generated below.
        # # currently, the old connector would be replaced by the new connector.
        # for conn in kafka_connector:
        #     try:
        #         import_udf_fname = conn
        #         udf = self.client.get_udf_module(import_udf_fname)
        #         udf_source = udf._get()
        #     except Exception as e:
        #         logger.warning(e)
        #         udf_file = (
        #             os.path.dirname(Path(__file__)) + '/udf/kafka_import_confluent.py'
        #         )
        #         with open(udf_file) as f_udf:
        #             udf_source = f_udf.read()
        #     self.connector = Connector(import_udf_template=udf_source)
        self.dispatcher.check_udf('kafka_import_confluent',
                                  '/udf/kafka_import_confluent.py')
        self.connector = Connector(import_udf_name='kafka_import_confluent')

    def initializeXcalarClient(self, sessionName=None, username=None):
        if self.runAsApp:
            logger.info(f'Connecting to Xcalar cluster in app')
            if username is None:
                raise Exception('need to provide username to run the app')
            self.client = Client(bypass_proxy=True, user_name=username)
            self.xcalarApi = XcalarApi(bypass_proxy=True)
        else:
            xcalarProperties = self.propertiesAdapter.getXcalarProperties()
            logger.info(
                f'Connecting to Xcalar cluster to {xcalarProperties.url} as {xcalarProperties.username}'
            )
            self.client = get_client(
                xcalarProperties.url,
                xcalarProperties.username,
                xcalarProperties.password,
            )

            self.xcalarApi = get_xcalarapi(
                xcalarProperties.url,
                xcalarProperties.username,
                xcalarProperties.password,
            )

        if sessionName is None:
            sessionName = xcalarProperties.sessionName
        self.session = get_or_create_session(self.client, sessionName)
        self.xcalarApi.setSession(self.session)
        # self.appMgr = App(self.xcalarApi)
        logger.info('State: Connected to Xcalar cluster')

    def materialize(self, dirty=False, from_universe_id=None):
        logger.info('State: Materializing data into the cluster')
        self.orchestrator = Orchestrator()
        if self.runAsApp:
            # Assumption: all nodes have same number of cpu
            node_cpus = multiprocessing.cpu_count()
            num_nodes = ctx.get_node_count()
            xpuCount = node_cpus * num_nodes
        else:
            xpuCount = self.propertiesAdapter.getXcalarProperties().xpuCount
        self.orchestrator.initialize(
            self.dispatcher,
            self.universe,
            self.getSnapshotDataTargetName(),
            self.getSnapshotMaxRetentionNumber(),
            self.getSnapshotMaxRetentionPeriodSeconds(),
            getattr(self.callbacks, 'isTimetoPurge'),
            xpuCount,
            batch_retention_seconds=self.universe.getBatchRetentionSeconds(),
        )
        if from_universe_id is not None:
            logger.info(
                f'State: Starting restoring tables from latest snapshot in universe: {from_universe_id}. If failure occurs during this process, you must rerun this command.'
            )
            self.dispatcher.unpinDropTables(tnames_to_keep=[], prefix='*')
            self.orchestrator = Orchestrator.restoreFromLatestSnapshot(
                from_universe_id,
                self.client,
                self.session,
                self.xcalarApi,
                self.universe,
                self.getSnapshotDataTargetName(),
                self.getSnapshotMaxRetentionNumber(),
                self.getSnapshotMaxRetentionPeriodSeconds(),
            )
            logger.info(
                f'Finished restoring tables from latest snapshot in universe: {from_universe_id}. If failure occurs henceforth, you can continue with materialize_continue.'
            )
        if not dirty:
            self.orchestrator.cleanLastBatch()

        logger.info('State: Start hydration')
        self.orchestrator.hydrateUniverseFromSource(reuse_dirty=dirty)
        logger.info(
            'State: Hydration finished successfully, now checkpointing')
        self.orchestrator.checkpoint(self.universeId)
        logger.info('State: Checkpoint finished successfully')
        if from_universe_id is None and not self.universeId.endswith('no_state'):
            logger.info('State: Now snapshotting')
            self._sm_snapshot()
            logger.info('State: Snapshot finished successfully')
        else:
            logger.info('State: No state required, not snapshotting')
        self.to_materialized()
        self.logInfo()

    def logInfo(self):
        logger.info('**** Universe Info ****')
        # logger.info("Watermark: {}".format(self.orchestrator.watermark))
        logger.info('Batch info: {}'.format(
            self.orchestrator.watermark.batchInfo))
        # logger.info('All objects: {}'.format(self.orchestrator.watermark.mapObjects()))

    def logOffsets(self):
        a = self.orchestrator.getAnalyzer()
        logger.info(a.toPandasDf('$info').head(1000))
        logger.info(a.toPandasDf('$offsets').head(1000))

    def restoreOrchestrator(self, skip_props_refresh=False, getMaterializationTime=True):
        self.orchestrator = Orchestrator.restoreFromCheckpoint(
            self.universeId, self.client, self.session, self.xcalarApi,
            self.universe)
        if skip_props_refresh:
            logger.info('State: Skipping properties refresh')
            return
        logger.info('State: Updating orchestrator state')
        self.orchestrator.update(
            self.dispatcher,
            self.universe,
            self.getSnapshotDataTargetName(),
            self.getSnapshotMaxRetentionNumber(),
            self.getSnapshotMaxRetentionPeriodSeconds(),
            getattr(self.callbacks, 'isTimetoPurge'),
            self.orchestrator.xpu_count,
            batch_retention_seconds=self.universe.getBatchRetentionSeconds(),
        )
        xpuPartitionMap = self.callbacks.getXpuPartitionMap(
            self.orchestrator.xpu_count,
            self.universe.getDistributerProps(),
            self.universe.universe_id,
            self.orchestrator.getMaterializationTime(getMaterializationTime),
        )
        self.orchestrator.registerConnector(self.connector, xpuPartitionMap)

    def pullDeltas(self, num_runs=math.inf, sleep_between_runs=True):
        if num_runs <= 0:
            return
        run = 0
        while run < num_runs:
            # stay in streaming state
            if  self.pause_time is None or self.pause_time <= 0:
                logger.info(
                    'State: Starting to pull deltas, restoring orchestrator state')
                self.restoreOrchestrator(getMaterializationTime=False)
                logger.info('State: Pulling deltas now')
                start_dt = datetime.now()
                self.orchestrator.pullDeltas()
                #sleep
                logger.info('State: Pulled deltas successfully, checkpointing now')
                self.orchestrator.checkpoint(self.universeId)
                logger.info('State: Checkpointed orchestrator state successfully')
                isTimeToSnapshot = getattr(self.callbacks, 'isTimeToSnapshot')
                if isTimeToSnapshot(self.orchestrator.watermark,
                                    self.orchestrator.getAnalyzer()):
                    logger.info('State: Snapshotting orchestrator')
                    self.state = 'streaming'
                    self._sm_snapshot()
                    self.machine.set_state('streaming')
                    self.persistStateAfterSnapshot('streaming')
                    logger.info(f'State: Current state: {self.state}')
                if sleep_between_runs:
                    self.pause_time = self.universe.getDeltaIntervalInSeconds()
                logger.info(f'State: Now set up a sleep time {self.pause_time}...')
            else:
                logger.info(f'State: Will sleep {self.universe.getMinimumSleepIntervalInSeconds()}')
                self.pause_time -= self.universe.getMinimumSleepIntervalInSeconds()
                time.sleep(self.universe.getMinimumSleepIntervalInSeconds())
            run += 1
            # # end_dt = datetime.now()
            # # if sleep_between_runs:
            # #     self.sleep(start_dt, end_dt)
            # # run += 1
            # return run

    def sleep(self, start_dt, end_dt):
        time_to_sleep = (self.universe.getMinimumSleepIntervalInSeconds() -
                         (end_dt - start_dt).seconds)
        if time_to_sleep < self.universe.getMinimumSleepIntervalInSeconds():
            time_to_sleep = self.universe.getMinimumSleepIntervalInSeconds()
        logger.info(f'State: Sleeping for {time_to_sleep} seconds.')
        time.sleep(time_to_sleep)

    def recoverFromLatestSnapshot(self):
        for t in self.session.list_tables('*'):
            logger.info('State: Dropping ... {}'.format(t.name))
            try:
                if t.is_pinned():
                    t.unpin()
                t.drop(delete_completely=True)
            except Exception as e:
                logger.debug('***: Drop failed: {}'.format(str(e)))
        self.orchestrator = Orchestrator.restoreFromLatestSnapshot(
            self.universeId,
            self.client,
            self.session,
            self.xcalarApi,
            self.universe,
            self.getSnapshotDataTargetName(),
            self.getSnapshotMaxRetentionNumber(),
            self.getSnapshotMaxRetentionPeriodSeconds(),
        )
        logger.info('State: Recovered orchestrator from snapshot')
        self.orchestrator.checkpoint(self.universeId)
        logger.info('State: Checkpointed orchestrator')

    def recoverFromSpecificSnapshot(self, timestamp):
        for t in self.session.list_tables('*'):
            logger.info('State: Dropping ... {}'.format(t.name))
            try:
                t.unpin()
            except Exception:
                pass
            try:
                t.drop(delete_completely=True)
            except Exception as e:
                logger.debug('***: Drop failed: {}'.format(str(e)))
        self.orchestrator = Orchestrator.restoreFromSpecificSnapshot(
            self.universeId,
            self.client,
            self.session,
            self.xcalarApi,
            self.universe,
            self.getSnapshotDataTargetName(),
            self.getSnapshotMaxRetentionNumber(),
            self.getSnapshotMaxRetentionPeriodSeconds(),
            timestamp,
        )
        logger.info('State: Recovered orchestrator from snapshot')
        self.orchestrator.checkpoint(self.universeId)
        logger.info('State: Checkpointed orchestrator')

    def adjustOffsetTable(self, topic: str, partitions: str,
                          new_offset: int) -> None:
        self.restoreOrchestrator()
        if topic and partitions:
            self.orchestrator.adjustOffsetTable(topic, partitions, new_offset)
            logger.info(
                f'State: adjustment for topic: {topic} and partitions: {partitions} finished successfully, now checkpointing'
            )
            self.orchestrator.checkpoint(self.universeId)

    def adjustTable(self, table_names: str, apply: str) -> None:
        self.restoreOrchestrator()
        if table_names and apply in ('drop', 'add', 'replace'):
            self.orchestrator.adjustUniverse(table_names, apply)
            logger.info(
                f'State: adjustment {apply} for table {table_names} finished successfully, now checkpointing'
            )
            self.orchestrator.checkpoint(self.universeId)
        else:
            logger.error(
                f'Unable to adjust table {table_names} with method {apply}.')

    # TODO: This should be on_enter_snapshotting
    def snapshot(self, restore_orchestrator=True):
        if restore_orchestrator:
            self.restoreOrchestrator()
        self.orchestrator.snapshot(self.universeId)
        logger.info('State: Snapshot finished successfully')

    def recover(self):
        # check if previous orchestrator's state was in (streaming, materialized)
        orchestrator = Orchestrator.restoreFromCheckpoint(
            self.universeId, self.client, self.session, self.xcalarApi,
            self.universe)
        if orchestrator is not None:
            self.restoreOrchestrator(skip_props_refresh=True)
        logger.info(
            'State: Current state is none, moving to a initialized state')
        self.to_initialized()
        self.isRecovered = True

    def on_enter_materializing(self, event_data=None):
        if ((event_data is not None) and ('dirty' in event_data)
                and ('universe_id' in event_data)):
            logger.info(
                f'State: Materializing using dirty={event_data["dirty"]}, latest snapshot from universe_id={event_data["universe_id"]}'
            )
            self.materialize(
                dirty=event_data['dirty'],
                from_universe_id=event_data['universe_id'])
        elif event_data is not None and 'dirty' in event_data:
            logger.info(
                f'State: Materializing using dirty={event_data["dirty"]}')
            self.materialize(dirty=event_data['dirty'])
        else:
            logger.info(f'State: Materializing using dirty=False')
            self.materialize(dirty=False)

    def on_enter_streaming(self, event_data=None):
        num_runs = 1
        sleep_between_runs = True
        if event_data is not None and 'num_runs' in event_data:
            num_runs = event_data['num_runs']
        if event_data is not None and 'sleep_between_runs' in event_data:
            sleep_between_runs = event_data['sleep_between_runs']
        logger.info(
            f'State: Starting streaming with num_runs:{num_runs}, sleep_between_runs:{sleep_between_runs}'
        )
        self.pullDeltas(
            num_runs=num_runs, sleep_between_runs=sleep_between_runs)
        if event_data is not None and 'next_state' in event_data:
            self.persistState(event_data['next_state'])

    def on_enter_initialized(self, event_data=None):
        self.pause_time = None
        logger.info('State: Controller is initialized')

    def on_enter_failed(self, event_data=None):
        logger.fatal(
            '***: Controller is in failed state. Waiting for operator to clear it.'
        )

    def on_enter_none(self, event_data=None):
        self._sm_init()

    def on_enter_destroyed(self, event_data=None):
        if hasattr(self, 'orchestrator') and self.orchestrator is not None:
            self.orchestrator.reset_state()
            self.orchestrator.cleanLastBatch()
        self.persistState('initialized')
        self._sm_init()

    def on_enter_snapshotting(self, event_data=None):
        self.snapshot(restore_orchestrator=False)

    def on_enter_recovering(self, event_data=None):
        if event_data is not None and 'timestamp' in event_data:
            logger.info(
                f"State: Recovering using timestamp:{event_data['timestamp']}")
            self.recoverFromSpecificSnapshot(event_data['timestamp'])
        else:
            self.recoverFromLatestSnapshot()
        self._sm_recovered()

    def on_enter_adjusting(self, event_data):
        if 'tables' in event_data:
            logger.info('State: adjusting table')
            self.adjustTable(event_data['tables'], event_data['action'])
        else:
            self.adjustOffsetTable(event_data['topic'],
                                   event_data['partitions'],
                                   event_data['new_offset'])
        self._sm_adjusted()

    def eventloop(self, num_runs=math.inf):
        try:
            if not hasattr(self, 'isRecovered') or not self.isRecovered:
                atexit.register(exitHandler, controller=self)
                self.recover()
        except Exception as e:
            logger.fatal(f'Controller failed. {str(e)}')
            exc_type, exc_value, exc_traceback = sys.exc_info()
            message = traceback.format_exception(exc_type, exc_value,
                                                 exc_traceback)
            logger.fatal(message)
            self._sm_fail()

        run = 0
        while run < num_runs:
            try:
                run += 1
                start_dt = datetime.now()
                next_state = self.nextState()
                logger.info(
                    f'State: Current State: {self.state}, Next state: {next_state}'
                )

                # Handle special streaming case when no next_state is specified
                if self.state == 'streaming' and next_state is None:
                    self._sm_continue_streaming()
                    continue

                if next_state is None:
                    self.sleep(start_dt, datetime.now())
                    continue

                name = next_state.name
                params = next_state.params
                if name == 'adjusting':
                    self._sm_start_adjust(event_data=params)
                elif name == 'streaming' or name != self.state:
                    handler = getattr(self, f'to_{name}',
                                      lambda: 'Unknown state!')
                    handler(event_data=params)
                end_dt = datetime.now()
                self.sleep(start_dt, end_dt)
            except Exception as e:
                logger.fatal(f'Controller failed. {str(e)}')
                exc_type, exc_value, exc_traceback = sys.exc_info()
                message = traceback.format_exception(exc_type, exc_value,
                                                     exc_traceback)
                logger.fatal(message)
                self._sm_fail()


def readArgs():
    parser = argparse.ArgumentParser(description='Xcalar Controller')
    parser.add_argument(
        'mode',
        choices=[
            'materialize',
            'materialize_continue',
            'recover',
            'snapshot',
            'incremental',
            'adjustment',
        ],
    )
    parser.add_argument(
        '--properties',
        type=str,
        required=True,
        help='Properties file for the controller',
    )
    parser.add_argument(
        '--universe_id', type=str, required=True, help='Universe id to use')

    parser.add_argument(
        '--session',
        type=str,
        required=False,
        default=None,
        help=    # NOQA
        'Session name to use. Will be read from properties if unspecified.',    # NOQA
    )

    materialize_group = parser.add_argument_group('materialize')
    materialize_group.add_argument(
        '--using_latest_snaphot_from_universe_id',
        type=str,
        help='universe id to take latest snapshot from',
    )

    incremental_group = parser.add_argument_group('incremental')
    incremental_group.add_argument(
        '--num_runs', type=int, help='Number of times to run incremental')

    recover_group = parser.add_argument_group('recover')
    recover_group.add_argument(
        '--from_timestamp',
        type=str,
        help='Snapshot timestamp to recover Xcalar state from',
    )

    adjustment_group = parser.add_argument_group('adjustment')
    adjustment_group.add_argument(
        '--table', type=str, help='table name to adjust')
    adjustment_group.add_argument(
        '--action', type=str, help='method to apply [add, drop, replace]')
    adjustment_group.add_argument('--offset_table', action='store_true')
    adjustment_group.add_argument(
        '--topic', type=str, help='the topic to apply the offset to')
    adjustment_group.add_argument(
        '--partitions',
        type=str,
        help='list of partitions seperated by comma i.e. 1,3,5,6',
    )
    adjustment_group.add_argument(
        '--new_offset', type=str, help='the new offset to set')

    return parser.parse_args()


def exitHandler(controller):
    logger.info('State: Exiting')
    try:
        logger.info(
            f'***: Undeleted datasets: {controller.orchestrator.dispatcher.datasetCounter}'
        )
        controller.logInfo()
        controller.logOffsets()
    except Exception:
        pass


# use this function to generate a state diagram
def generateStateMachineDiagram(controller):
    # when you want to use graphviz explicitely
    # machine = Machine(model=m, use_pygraphviz=False, ...)
    # in cases where auto transitions should be visible
    # machine = Machine(model=m, show_auto_transitions=True, ...)
    # draw the whole graph ...
    controller.get_graph().draw('/tmp/state_diagram.png', prog='dot')


if __name__ == '__main__':
    args = readArgs()
    c = Controller()
    c.initialize(args.properties, args.universe_id, args.session)
    c.to_initialized()
    atexit.register(exitHandler, controller=c)

    if args.mode == 'materialize' and args.using_latest_snaphot_from_universe_id:
        c.to_materializing(
            event_data={
                'dirty': True,
                'universe_id': args.using_latest_snaphot_from_universe_id,
            })
    elif args.mode == 'materialize':
        c.to_materializing(event_data={'dirty': False})
    elif args.mode == 'materialize_continue':
        c.to_materializing(event_data={'dirty': True})
    elif args.mode == 'incremental' and args.num_runs:
        c.to_streaming(event_data={'num_runs': args.num_runs})
    elif args.mode == 'incremental':
        c.to_streaming(event_data={'num_runs': math.inf})
    elif args.mode == 'recover' and args.from_timestamp:
        c.recoverFromSpecificSnapshot(args.from_timestamp)
    elif args.mode == 'recover':
        c.recoverFromLatestSnapshot()
    elif args.mode == 'snapshot':
        c.snapshot()
    elif args.mode == 'adjustment' and args.offset_table:
        c.adjustOffsetTable(args.topic, args.partitions, args.new_offset)
    elif args.mode == 'adjustment' and args.tables:
        c.adjustTable(args.tables, args.action)
    else:
        raise Exception(f'Unsupported mode: {args.mode}')

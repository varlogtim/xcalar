from xcalar.external.app import App
import json
from xcalar.solutions.state_persister import StatePersister
import time
import jsonpickle
from xcalar.solutions.controller_state import ControllerState
from xcalar.solutions.snapshot import SnapshotManagement
import math
from xcalar.solutions.xcalar_client_utils import get_or_create_session

APP_STR_KEY = '{}_038367a9-a764-4f36-b8d2-6fa9580fcf5f'
APP_NAME = 'ControllerApp'


# one session only have one app with same name #
class ControllerAppClient:
    __instance = {}

    @staticmethod
    def getInstance(universe_id):
        """ Static access method. """
        if universe_id not in ControllerAppClient.__instance.keys():
            raise Exception('There it no ControllerAppClient')
        return ControllerAppClient.__instance[universe_id]

    def __init__(self, client, universe_id, xcalarApi, controllerAppProps):
        self.client = client
        self.xcalarApi = xcalarApi
        self.universeId = universe_id
        self.session = get_or_create_session(self.client, self.universeId)
        self.appMgr = App(client)
        self.statePersister = StatePersister(f'app_{self.universeId}_next',
                                             self.client.global_kvstore())
        self.currentStatePersister = StatePersister(
            f'app_{self.universeId}_current', self.client.global_kvstore())
        self.transitStatePersister = StatePersister(
            f'app_{self.universeId}_transit', self.client.global_kvstore())
        self.controllerAppProps = controllerAppProps
        # ControllerAppClient.__instance[self.universeId] = self

    def materialize(self):
        new_state = 'materializing'
        self.set_state(new_state)
        self.waitForStateChange('materialized')

    def materialize_continue(self):
        new_state = 'materializing'
        self.set_state(new_state, params={'dirty': True})
        self.waitForStateChange('materialized')

    def materialize_from_universe_id(self, universeId):
        new_state = 'materializing'
        self.set_state(
            new_state, params={
                'dirty': True,
                'universe_id': universeId
            })
        self.waitForStateChange('materialized')

    def incremental(self):
        self.set_state('streaming')
        self.waitForStateChange('streaming')

    def incremental_num_runs(self, num):
        self.set_state(
            'streaming', params={
                'num_runs': num,
                'next_state': 'initialized',
                'sleep_between_runs': False
            })
        self.waitForStateChange('initialized')

    def pause(self):
        if self.get_state() != 'streaming':
            raise Exception('App is not in incremental mode! Cannot pause.')
        self.set_state('initialized')
        self.waitForStateChange('initialized')

    def snapshot(self):
        previous_state = self.get_state()
        self.set_state('snapshotting')
        self.waitForStateChange('snapshotting')
        self.set_state(previous_state)
        self.waitForStateChange(previous_state)

    def reset(self):
        self.set_state('destroyed')
        self.waitForStateChange('initialized')

    def recoverFromSpecificSnapshot(self, timestamp):
        self.set_state('recovering', params={'timestamp': timestamp})
        self.waitForStateChange('initialized')

    def recoverFromLatestSnapshot(self):
        self.set_state('recovering')
        self.waitForStateChange('initialized')

    def adjust_offset_table(self, topic_name, partition_list, new_offset):
        self.set_state(
            'adjusting',
            params={
                'topic': topic_name,
                'partitions': partition_list,
                'new_offset': new_offset,
            },
        )
        self.waitForStateChange('initialized')

    def adjust_table(self, table_names, action):
        self.set_state(
            'adjusting', params={
                'tables': table_names,
                'action': action
            })
        self.waitForStateChange('initialized')

    def listSnapshots(self):
        sm = SnapshotManagement(self.universeId, self.client, self.session,
                                self.xcalarApi, None, math.inf, math.inf, None,
                                True)
        return sm.get_snapshots()

    def store_group_id(self, group_id):
        name = 'app_{}_id'.format(self.universeId)
        self.client.global_kvstore().add_or_replace(
            APP_STR_KEY.format(name), group_id, True)

    def check_group_id(self):
        name = 'app_{}_id'.format(self.universeId)
        try:
            result = self.client.global_kvstore().lookup(
                APP_STR_KEY.format(name))
        except Exception:
            result = None
        return result

    def stop(self):
        name = 'app_{}_id'.format(self.universeId)
        group_id = self.client.global_kvstore().lookup(
            APP_STR_KEY.format(name))
        self.appMgr.cancel(group_id)
        self.client.global_kvstore().delete(APP_STR_KEY.format(name))
        self.currentStatePersister.delete_state()
        self.statePersister.delete_state()
        self.transitStatePersister.delete_state()
        # ControllerAppClient.__instance.pop(self.universeId, None)

    def set_state(self, state, params={}):
        current = self.get_state()
        self.currentStatePersister.delete_state()
        serializedState = jsonpickle.encode(ControllerState(state, params))
        self.statePersister.store_state(serializedState)
        serializedTransitState = jsonpickle.encode({
            'previousState': current,
            'nextState': state
        })
        self.transitStatePersister.store_state(serializedTransitState)

    def check_alive(self):
        group_id = self.check_group_id()
        if group_id is None:
            return False
        else:
            return self.appMgr.is_app_alive(group_id)

    def get_state(self):
        return self.currentStatePersister.restore_state()

    def get_transit_state(self):
        states = self.transitStatePersister.restore_state()
        if states:
            return jsonpickle.decode(states)
        else:
            return None

    def waitForStateChange(self, finalState):
        num = 0
        while num < self.controllerAppProps.get('stateChange').get('numTries'):
            state = self.get_state()
            if state and state in (finalState, 'failed'):
                break
            num += 1
            print(f'In progess... Waiting for {finalState}...')
            time.sleep(
                self.controllerAppProps.get('stateChange').get(
                    'sleepIntervalInSec'))
        print(f'Current controller state is {state}')
        if not state or state != finalState:
            if state != 'failed':
                print(
                    'Timeout, please manually check the state using controller_app -state'
                )
            else:
                raise Exception(f'Failed to change state to {finalState}')

    def runControllerApp(self):
        self.statePersister.delete_state()
        try:
            group_id = self.check_group_id()
            if group_id is not None and not self.appMgr.is_app_alive(group_id):
                name = 'app_{}_id'.format(self.universeId)
                self.client.global_kvstore().delete(APP_STR_KEY.format(name))
                self.currentStatePersister.delete_state()
                group_id = None

            if group_id is None:
                group_id = self.appMgr.run_py_app_async(
                    APP_NAME, False,
                    json.dumps({
                        'universe_id':
                            self.universeId,
                        'python_file_path':
                            self.controllerAppProps.get('appFilePath'),
                        'git':
                            self.controllerAppProps.get('git'),
                        'callbacksPluginName':
                            self.controllerAppProps.get('callbacksPluginName'),
                        'universeAdapterPluginName':
                            self.controllerAppProps.get('universeAdapterPluginName'),
                        'username':
                            self.client.username
                    }))
                print(
                    f'Launched the app: {APP_NAME} [group_id: {group_id}] with universe_id: {self.universeId}'
                )
                self.store_group_id(group_id)
            else:
                print(
                    f'Controller app with universe id {self.universeId} is already running'
                )
        except Exception as e:
            if str(e) == 'App specified was not found':
                raise Exception(
                    'App need to be registered again, run controller -init command again'
                )
            else:
                raise Exception(str(e))

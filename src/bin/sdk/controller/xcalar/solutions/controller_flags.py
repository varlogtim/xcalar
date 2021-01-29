from xcalar.external.client import Client
from xcalar.external.session import Session
from xcalar.solutions.state_persister import StatePersister
from types import SimpleNamespace


class ControllerFlags():
    def __init__(self, universe_id: str, client: Client, session: Session):
        self.state_persistor = StatePersister(f'{universe_id}_flags',
                                              client.global_kvstore())
        flags = self.state_persistor.restore_state()
        if flags is None:
            self.flags = SimpleNamespace(paused=False)
        else:
            self.flags = flags

    def pause(self):
        self.flags.paused = True
        self.state_persistor.store_state(self.flags)

    def unpause(self):
        self.flags.paused = False
        self.state_persistor.store_state(self.flags)


def get_controller_flags(universe_id: str, client: Client, session: Session):
    return ControllerFlags(universe_id, client, session).flags

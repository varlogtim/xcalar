import pickle
import codecs
from typing import Optional
from structlog import get_logger

logger = get_logger(__name__)

PERSISTER_OBJ_KEY = '{}_71bdbef6-6008-497c-89d1-6d05364978e2'
PERSISTER_STR_KEY = '{}_038367a9-a764-4f36-b8d2-6fa9580fcf5f'
"""This class persists and retrieves a generic object to/from the KvStore

"""


class StatePersister():
    def __init__(self, name: str, kvstore):
        self.name = name
        self.kvstore = kvstore

    """Serialize the object by pickling it to a base64 string. You can optionally persist a
    human readable string as well to be viewed directly in the kvstore.

    """

    def store_state(self, obj: object, human_readable_string: str = None):
        pickled = codecs.encode(pickle.dumps(obj), 'base64').decode()
        self.kvstore.add_or_replace(
            PERSISTER_OBJ_KEY.format(self.name), pickled, True)
        if human_readable_string is not None:
            self.kvstore.add_or_replace(
                PERSISTER_STR_KEY.format(self.name), human_readable_string,
                True)

    """Deserialize and return object if stored otherwise return None.

    Returns:
        Object or None
    """

    def restore_state(self) -> Optional[object]:
        try:
            return pickle.loads(
                codecs.decode(
                    self.kvstore.lookup(PERSISTER_OBJ_KEY.format(
                        self.name)).encode(), 'base64'))
        except Exception as e:
            logger.debug(str(e))
            return None

    def delete_state(self):
        try:
            self.kvstore.delete(PERSISTER_OBJ_KEY.format(self.name))
        except Exception as e:
            logger.debug(str(e))

import pickle
import codecs
from typing import Any


def pack_to_string(obj: Any) -> str:
    return codecs.encode(pickle.dumps(obj), "base64").decode()


def unpack_from_string(value: str) -> Any:
    return pickle.loads(codecs.decode(value.encode(), "base64"))

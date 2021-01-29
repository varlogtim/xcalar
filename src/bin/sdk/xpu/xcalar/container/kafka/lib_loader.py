import importlib
import traceback
import xcalar.container.context as ctx

logger = ctx.get_logger()


def dynaload(name, path):
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None:
            raise Exception(f'No {name} plugin found in {path}')
        return spec.loader.load_module()
    except Exception as e:
        logger.warn(
            f'Failed to load {name} from {path}\n{traceback.format_exc()}')
        raise e

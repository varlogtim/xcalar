import imp
import logging

logger = logging.getLogger("xcalar")
logger.setLevel(logging.INFO)

BUILTIN_DRIVERS = [
    "do_nothing", "single_csv", "multiple_csv", "legacy_udf",
    "snapshot_export_driver", "fast_csv", "snapshot_parquet", "snapshot_csv",
    "snowflake_export"
]


class DriverParamType:
    # This has a strong contract with XD to enable special UX for some types
    name = None


class IntParamType(DriverParamType):
    name = "integer"


class StrParamType(DriverParamType):
    name = "string"


class BoolParamType(DriverParamType):
    name = "boolean"


class TargetParamType(DriverParamType):
    name = "target"


STRING = StrParamType()
INTEGER = IntParamType()
BOOLEAN = BoolParamType()
TARGET = TargetParamType()


class ParamDescriptor:
    def __init__(self, name, desc, type=STRING, secret=False, optional=False):
        if not isinstance(type, DriverParamType):
            raise ValueError(
                "parameter type '{}' is not of type DriverParamType".format(
                    type))
        if type.name is None:
            raise ValueError(
                "parameter type '{}' does not have a valid name".format(type))

        self.name = name
        self.desc = desc
        self.type = type
        self.secret = secret
        self.optional = optional

    # XXX - We may want to use the canonical thing to enable dict(TargetParam),
    # which would be to support __iter__ and yield the items as tuples
    def to_dict(self):
        return {
            "name": self.name,
            "type": self.type.name,
            "description": self.desc,
            "secret": self.secret,
            "optional": self.optional,
        }


def extract_drivers(module):
    """Extract drivers from a given module"""
    for obj in module.__dict__.values():
        if isinstance(obj, Driver):
            yield obj


def extract_drivers_from_source(path, module_source,
                                module_name="driver_module"):
    """Extract drivers from a given source code source"""
    try:
        module = imp.new_module(module_name)
        exec(module_source, module.__dict__)
    except:
        # See ENG-97; the module could be user-written and could fail to
        # compile. Not a fatal failure - report the problem and move on
        logger.exception("bad module {}".format(path))
        pass
    else:
        yield from extract_drivers(module)


class Driver:
    """A Driver is user code that runs in a distributed XPU cluster.

    Drivers publish the parameters that they accept. These are used to expose
    the driver to an end user, prompting them to fill in parameters in order
    to execute the driver."""

    def __init__(self,
                 callback,
                 params,
                 name=None,
                 desc=None,
                 is_builtin=False,
                 is_hidden=False):
        self._name = name
        self._driver_function = callback
        self._params = params
        self._is_builtin = is_builtin
        self._is_hidden = is_hidden
        self._description = desc

    @property
    def name(self):
        return self._name

    @property
    def description(self):
        raw_desc = self._description or ""
        desc = raw_desc.strip('\n').replace('\n', ' ')
        return desc

    @property
    def params(self):
        return self._params

    @property
    def is_hidden(self):
        return self._is_hidden

    @property
    def is_builtin(self):
        return self._is_builtin

    @is_builtin.setter
    def is_builtin(self, value):
        self._is_builtin = value

    def add_param(self, param):
        self._params.append(param)

    def param_dict_list(self):
        return [p.to_dict() for p in self.params.values()]

    def main(self, *args, **kwargs):
        try:
            self._driver_function(*args, **kwargs)
        except Exception as e:
            raise RuntimeError("Driver '{}' encountered error: {}".format(
                self.name, e)) from e

    def __call__(self, *args, **kwargs):
        """Alias for :meth:`main`."""
        return self.main(*args, **kwargs)


def register_export_driver(name=None,
                           desc=None,
                           is_builtin=False,
                           is_hidden=False):
    def decorator(callback):
        if isinstance(callback, Driver):
            raise TypeError(
                "Attempted to create driver twice on the same function")

        if name in BUILTIN_DRIVERS and not is_builtin:
            raise ValueError(
                "'{}' is the name of a builtin driver".format(name))

        description = desc or callback.__doc__ or ""

        # grab the params off of the function object
        if hasattr(callback, "__xc_driver_params__"):
            param_list = callback.__xc_driver_params__
            param_list.reverse()
            del callback.__xc_driver_params__
        else:
            param_list = []

        params = {p.name: p for p in param_list}

        driver = Driver(
            callback,
            params,
            name,
            description,
            is_builtin=is_builtin,
            is_hidden=is_hidden)
        return driver

    return decorator


def param(name, desc, type=STRING, secret=False, optional=False):
    def decorator(callback):
        # pass along all param arguments
        param = ParamDescriptor(name, desc, type, secret, optional)
        if isinstance(callback, Driver):
            callback.add_param(param)
        else:
            if not hasattr(callback, '__xc_driver_params__'):
                callback.__xc_driver_params__ = []
            callback.__xc_driver_params__.append(param)
        return callback

    return decorator

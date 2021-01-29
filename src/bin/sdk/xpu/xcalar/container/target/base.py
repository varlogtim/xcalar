import os


class TargetParam(object):
    def __init__(self, name, desc, secret=False, optional=False):
        self.name = name
        self.desc = desc
        self.secret = secret
        self.optional = optional

    # XXX - We may want to use the canonical thing to enable dict(TargetParam),
    # which would be to support __iter__ and yield the items as tuples
    def to_dict(self):
        return {
            "name": self.name,
            "description": self.desc,
            "secret": self.secret,
            "optional": self.optional,
        }


# This is a property on the Target TYPE itself, not on an
# instance of the target. Effectively this is acting as the initializer for the
# class itself, initializing values which will be modified by the decorators
# on the target
class TargetType(type):
    def __new__(cls, name, bases, dct):
        x = super().__new__(cls, name, bases, dct)
        x._registered = False
        x._name = None
        x._identifier = None
        x._description = None
        x._augment_instance_info_func = None

        x._parameters = []
        x._is_available = False

        return x


class BaseTarget(metaclass=TargetType):
    def __init__(self, target_name, **kwargs):
        self._target_name = target_name

    def __enter__(self):
        # default impl of context manager does nothing
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        # default impl of context manager does nothing
        pass

    @classmethod
    def register(cls, name, description=None, is_available=True):
        cls._name = name
        if description:
            cls._description = description
        elif cls.__doc__:
            cls._description = cls.__doc__
        cls._is_available = is_available

        # XXX right now, the identifier is the name of the module containing the
        # target. We may want to change this and either have the target specify
        # its identifier, or something similar to uniquely identify the target
        # type across potential changes.
        module = cls.__module__.split(".")[-1]
        cls._identifier = module
        cls._registered = True

    @classmethod
    def registered(cls):
        return cls._registered

    @classmethod
    def is_available(cls):
        return cls._is_available

    @classmethod
    def type_name(cls):
        return cls._name

    @classmethod
    def identifier(cls):
        return cls._identifier

    @classmethod
    def description(cls):
        desc = cls._description.strip('\n').replace('\n', ' ')
        return desc

    @classmethod
    def add_parameter(cls, param):
        cls._parameters.append(param)

    @classmethod
    def parameters(cls):
        return cls._parameters

    @classmethod
    def construct_target(cls, target_name, target_data, path, **runtime_args):
        # there's some extra data that gets persisted but isn't passed to the
        # constructor; let's get rid of it here
        args = target_data.copy()
        del args["type_id"]
        del args["type_name"]

        if "type_context" in args and args["type_context"] is None:
            del args["type_context"]
        args = {**args, **runtime_args}

        return cls(target_name, path, **args)

    @classmethod
    def register_augment_instance_func(cls, func):
        if not callable(func):
            raise ValueError("augment_instance function must be callable")
        cls._augment_instance_info_func = func

    @classmethod
    def augment_instance_info(cls, target_data):
        """Augment target parameters with dynamic values that might be too
        expensive to compute multiple times. This is useful since targets
        are often instantiated in multiple processes on multiple machines in
        a batch. This allows the target type to compute an expensive value once,
        and share the result across all instances of the target in the cluster.
        This is done when the target is added.
        Returns the augmented target_data or None if no function exists.
        """
        if cls._augment_instance_info_func:
            return cls._augment_instance_info_func(target_data)

    def class_name(self):
        return self.__class__.__name__

    def name(self):
        return self._target_name

    def is_global(self):
        raise NotImplementedError("is_global not implemented for {}".format(
            self.class_name()))

    def get_files(self, path, name_pattern, recursive, **user_args):
        raise NotImplementedError("get_files not implemented for {}".format(
            self.class_name()))

    def open(self, fileobj, opts):
        raise NotImplementedError("open not implemented for {}".format(
            self.class_name()))

    def delete(self, path):
        raise NotImplementedError("delete not implemented for {}".format(
            self.class_name()))


def extract_target_types(module):
    """Extract target types from a given module"""
    for obj in module.__dict__.values():
        if isinstance(obj, type) and issubclass(obj, BaseTarget):
            yield obj


def register(*args, **kwargs):
    def decorator(cls):
        if not issubclass(cls, BaseTarget):
            raise ValueError("Class '{}' is not an instance of '{}'".format(
                cls, BaseTarget))
        cls.register(*args, **kwargs)
        return cls

    return decorator


def register_augment_instance_func(*args, **kwargs):
    def decorator(cls):
        if not issubclass(cls, BaseTarget):
            raise ValueError("Class '{}' is not an instance of '{}'".format(
                cls, BaseTarget))
        cls.register_augment_instance_func(*args, **kwargs)
        return cls

    return decorator


def param(*args, **kwargs):
    def decorator(cls):
        if not issubclass(cls, BaseTarget):
            raise ValueError("Class '{}' is not an instance of '{}'".format(
                cls, BaseTarget))
        # pass along all param arguments
        param = TargetParam(*args, **kwargs)
        cls.add_parameter(param)
        return cls

    return decorator


def internal(obj):
    """Mark something as being internal only; otherwise the symbol is None"""
    if os.environ.get("_XCALAR_INTERNAL_", False) == "1":
        return obj
    else:
        return None

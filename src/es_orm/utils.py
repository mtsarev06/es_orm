class ConfigClassMeta(type):
    """
    Makes it possible to use the "in" operator with classes.
    """
    def __iter__(self):
        return (getattr(self, attr) for attr in dir(self)
                if not attr.startswith("__"))


class ConfigClass(metaclass=ConfigClassMeta):
    """
    Defines a class which can be used like a dictionary in some way.
    Useful for defining classes with some configuration data.
    """
    @classmethod
    def keys(cls):
        return (attr for attr, attr_obj in vars(cls).items()
                if not attr.startswith('__') and not callable(attr_obj))

    @classmethod
    def values(cls):
        return (getattr(cls, attr) for attr in dir(cls)
                if not attr.startswith('__'))

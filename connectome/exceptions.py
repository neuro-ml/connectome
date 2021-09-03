class GraphError(Exception):
    pass


class DependencyError(GraphError):
    pass


class FieldError(Exception):
    pass


class StorageCorruption(OSError):
    """
    Denotes various problems with disk-based storage or persistent cache
    """

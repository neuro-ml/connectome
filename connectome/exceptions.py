# TODO: legacy support
from stash.exceptions import StorageCorruption


class GraphError(Exception):
    pass


class DependencyError(GraphError):
    pass


class FieldError(Exception):
    pass

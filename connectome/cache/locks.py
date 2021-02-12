from contextlib import contextmanager


class Locker:
    def read(self, key):
        raise NotImplementedError

    def write(self, key):
        raise NotImplementedError


class NoLock(Locker):
    @contextmanager
    def read(self, key):
        yield

    @contextmanager
    def write(self, key):
        yield


class FileLock(Locker):
    @contextmanager
    def write(self, key):
        raise NotImplementedError
        # wait until the file doesn't exist
        # create file
        yield
        # remove the file

    @contextmanager
    def read(self, key):
        raise NotImplementedError
        # wait until the file doesn't exist
        yield

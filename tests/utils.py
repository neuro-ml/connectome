import inspect


class Counter:
    def __init__(self, func=lambda x: x):
        self.func = func
        self.__signature__ = inspect.signature(func)
        self.n = 0

    def __call__(self, *args, **kwargs):
        self.n += 1
        return self.func(*args, **kwargs)

    def __getstate__(self):
        return self.func

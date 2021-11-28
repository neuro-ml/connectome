import sys
from abc import ABC, abstractmethod
from multiprocessing.pool import ThreadPool

from .base import Command

if sys.version_info[:2] < (3, 7):
    from queue import Queue as SafeQueue
else:
    from queue import SimpleQueue as SafeQueue


class Thunk:
    def __init__(self, parent):
        self.parent = parent
        self.ready = False
        self.error = self.value = None


class Frame(Thunk):
    def __init__(self, stack, commands, parent):
        super().__init__(parent)
        self.stack = stack
        self.commands = commands


class Executor(ABC):
    def __init__(self, frame: Frame):
        self.frame = frame

    def push(self, x):
        self.frame.stack.append(x)

    def pop(self):
        return self.frame.stack.pop()

    def peek(self):
        return self.frame.stack[-1]

    def push_command(self, x):
        self.frame.commands.append(x)

    def pop_command(self):
        return self.frame.commands.pop()

    @abstractmethod
    def next_frame(self):
        pass

    @abstractmethod
    def enqueue_frame(self, x):
        pass

    @abstractmethod
    def clear(self):
        pass

    @abstractmethod
    def call(self, func, args, kwargs):
        pass


class Backend:
    @abstractmethod
    def build(self, frame: Frame) -> Executor:
        pass


class Synchronous(Backend):
    class _Executor(Executor):
        def __init__(self, frame: Frame):
            super().__init__(frame)
            self.frames = SafeQueue()

        def clear(self):
            q = self.frames
            while not q.empty():
                assert q.get_nowait().ready

        def next_frame(self):
            self.frame = self.frames.get_nowait()

        def enqueue_frame(self, x):
            self.frames.put_nowait(x)

        def call(self, func, args, kwargs):
            self.push(func(*args, **kwargs))

    def build(self, frame: Frame) -> Executor:
        return self._Executor(frame)


class Threads(Backend):
    class _Executor(Executor):
        def __init__(self, frame: Frame, queue: SafeQueue):
            super().__init__(frame)
            self.frames, self.requests = SafeQueue(), queue

        def clear(self):
            q = self.frames
            while not q.empty():
                assert q.get_nowait().ready

        def next_frame(self):
            self.frame = self.frames.get()

        def enqueue_frame(self, x):
            self.frames.put(x)

        def call(self, func, args, kwargs):
            thunk = Thunk(self.frame)
            self.requests.put((func, args, kwargs, thunk, self.frames))
            self.push_command((Command.AwaitThunk, thunk))
            self.next_frame()

    def build(self, frame: Frame) -> Executor:
        return self._Executor(frame, self.thunks)

    def __init__(self, n: int):
        self.n = n
        self.thunks = SafeQueue()
        self.pool = ThreadPool(n, self._loop, (self.thunks,))
        self.pool.close()

    @staticmethod
    def _loop(thunks: SafeQueue):
        while True:
            value = thunks.get()
            if value is None:
                break

            func, args, kwargs, thunk, frames = value
            try:
                thunk.value = func(*args, **kwargs)
            except BaseException as e:
                thunk.error = e

            thunk.ready = True
            frames.put(thunk.parent)

    def __del__(self):
        for _ in range(self.n):
            self.thunks.put(None)
        self.pool.join()


DefaultBackend = Synchronous()
